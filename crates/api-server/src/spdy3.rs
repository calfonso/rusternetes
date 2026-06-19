//! Minimal SPDY/3.1 server transport for the CRI streaming endpoints.
//!
//! The kubelet connects to the runtime's streaming server (the URL returned by
//! `Exec`/`Attach`/`PortForward`) using **SPDY/3.1** — and per Kubernetes KEP-4006
//! this leg deliberately stays SPDY (the WebSocket transition only covers
//! kubectl↔apiserver↔kubelet, never kubelet↔runtime). The kubelet's client is
//! `github.com/moby/spdystream`, so we implement the subset of SPDY/3 that client
//! exercises: an HTTP/1.1 `Upgrade: SPDY/3.1`, then SYN_STREAM/SYN_REPLY/DATA/
//! RST_STREAM/PING/GOAWAY/SETTINGS over the raw connection, with the Kubernetes
//! `remotecommand` stream semantics (one stream per `streamType`).
//!
//! This module is the wire codec (frames + the zlib-with-preset-dictionary NV
//! header (de)compressor) plus the connection multiplexer.
//!
//! Ported verbatim from indyjonesnl/containerd-rs crates/cri/src/spdy.rs — a real
//! SPDY/3.1 server codec (frames + zlib preset-dictionary NV compression). Not yet
//! wired into handlers (#1262).

use std::io;

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress};

// ---- control frame types (SPDY/3) ----
const TYPE_SYN_STREAM: u16 = 1;
const TYPE_SYN_REPLY: u16 = 2;
const TYPE_RST_STREAM: u16 = 3;
const TYPE_SETTINGS: u16 = 4;
const TYPE_PING: u16 = 6;
const TYPE_GOAWAY: u16 = 7;
const TYPE_HEADERS: u16 = 8;
const TYPE_WINDOW_UPDATE: u16 = 9;

const SPDY_VERSION: u16 = 3;

/// Control-frame flag: last frame on the stream in this direction.
pub const FLAG_FIN: u8 = 0x01;

const SETTINGS_INITIAL_WINDOW_SIZE: u32 = 7;
/// A generous window so we can ignore flow control entirely (never block on
/// WINDOW_UPDATE for exec/attach/port-forward volumes).
pub const BIG_WINDOW: u32 = 0x7fff_ffff;

/// RST_STREAM / GOAWAY status: CANCEL.
pub const RST_CANCEL: u32 = 5;

// ---- Kubernetes remotecommand stream headers ----
pub const HEADER_STREAM_TYPE: &str = "streamType";
pub const HEADER_PORT: &str = "port";
pub const ST_ERROR: &str = "error";
pub const ST_STDIN: &str = "stdin";
pub const ST_STDOUT: &str = "stdout";
pub const ST_STDERR: &str = "stderr";
pub const ST_RESIZE: &str = "resize";

/// A decoded SPDY frame. Only the subset the kubelet's spdystream client uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SynStream {
        stream_id: u32,
        flags: u8,
        headers: Vec<(String, String)>,
    },
    SynReply {
        stream_id: u32,
        flags: u8,
        headers: Vec<(String, String)>,
    },
    RstStream {
        stream_id: u32,
        status: u32,
    },
    Settings {
        entries: Vec<(u32, u32)>,
    },
    Ping {
        id: u32,
    },
    GoAway {
        last_good_stream_id: u32,
        status: u32,
    },
    Headers {
        stream_id: u32,
        flags: u8,
        headers: Vec<(String, String)>,
    },
    WindowUpdate {
        stream_id: u32,
        delta: u32,
    },
    Data {
        stream_id: u32,
        flags: u8,
        payload: Vec<u8>,
    },
}

fn rd_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}
fn rd_u24(b: &[u8]) -> usize {
    ((b[0] as usize) << 16) | ((b[1] as usize) << 8) | (b[2] as usize)
}
fn wr_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn wr_u24(out: &mut Vec<u8>, v: usize) {
    out.push((v >> 16) as u8);
    out.push((v >> 8) as u8);
    out.push(v as u8);
}

/// Parse one frame from the front of `buf`. Returns `Ok(None)` when `buf` does
/// not yet contain a complete frame (caller should read more). On success
/// returns the frame and the number of bytes consumed.
pub fn parse_frame(buf: &[u8], nv: &mut NvCodec) -> io::Result<Option<(Frame, usize)>> {
    if buf.len() < 8 {
        return Ok(None);
    }
    let is_control = buf[0] & 0x80 != 0;
    let length = rd_u24(&buf[5..8]);
    let total = 8 + length;
    if buf.len() < total {
        return Ok(None);
    }
    let body = &buf[8..total];

    if !is_control {
        // DATA frame: [0|stream_id:31][flags:8][len:24][payload]
        let stream_id = rd_u32(&buf[0..4]) & 0x7fff_ffff;
        let flags = buf[4];
        return Ok(Some((
            Frame::Data {
                stream_id,
                flags,
                payload: body.to_vec(),
            },
            total,
        )));
    }

    let ftype = u16::from(buf[2]) << 8 | u16::from(buf[3]);
    let flags = buf[4];

    let frame = match ftype {
        TYPE_SYN_STREAM => {
            // [stream_id:32][assoc:32][pri:3|unused:5][slot:8] then compressed NV
            let stream_id = rd_u32(&body[0..4]) & 0x7fff_ffff;
            let headers = nv.decompress_nv(&body[10..])?;
            Frame::SynStream {
                stream_id,
                flags,
                headers,
            }
        }
        TYPE_SYN_REPLY => {
            let stream_id = rd_u32(&body[0..4]) & 0x7fff_ffff;
            let headers = nv.decompress_nv(&body[4..])?;
            Frame::SynReply {
                stream_id,
                flags,
                headers,
            }
        }
        TYPE_HEADERS => {
            let stream_id = rd_u32(&body[0..4]) & 0x7fff_ffff;
            let headers = nv.decompress_nv(&body[4..])?;
            Frame::Headers {
                stream_id,
                flags,
                headers,
            }
        }
        TYPE_RST_STREAM => Frame::RstStream {
            stream_id: rd_u32(&body[0..4]) & 0x7fff_ffff,
            status: rd_u32(&body[4..8]),
        },
        TYPE_SETTINGS => {
            let n = rd_u32(&body[0..4]) as usize;
            let mut entries = Vec::with_capacity(n);
            let mut off = 4;
            for _ in 0..n {
                if off + 8 > body.len() {
                    break;
                }
                // SPDY/3 entry: [flags:8][id:24][value:32]
                let id = rd_u24(&body[off + 1..off + 4]) as u32;
                let value = rd_u32(&body[off + 4..off + 8]);
                entries.push((id, value));
                off += 8;
            }
            Frame::Settings { entries }
        }
        TYPE_PING => Frame::Ping {
            id: rd_u32(&body[0..4]),
        },
        TYPE_GOAWAY => Frame::GoAway {
            last_good_stream_id: rd_u32(&body[0..4]) & 0x7fff_ffff,
            status: if body.len() >= 8 {
                rd_u32(&body[4..8])
            } else {
                0
            },
        },
        TYPE_WINDOW_UPDATE => Frame::WindowUpdate {
            stream_id: rd_u32(&body[0..4]) & 0x7fff_ffff,
            delta: rd_u32(&body[4..8]) & 0x7fff_ffff,
        },
        _ => {
            // Unknown control frame: consume + ignore (empty SETTINGS is a no-op).
            return Ok(Some((
                Frame::Settings {
                    entries: Vec::new(),
                },
                total,
            )));
        }
    };
    Ok(Some((frame, total)))
}

/// Serialize a frame, appending to `out`. NV-bearing frames compress headers
/// through `nv` (the per-connection deflate stream — must be serialized).
pub fn write_frame(out: &mut Vec<u8>, frame: &Frame, nv: &mut NvCodec) -> io::Result<()> {
    fn control_header(out: &mut Vec<u8>, ftype: u16, flags: u8, len: usize) {
        out.push(0x80 | (SPDY_VERSION >> 8) as u8);
        out.push(SPDY_VERSION as u8);
        out.push((ftype >> 8) as u8);
        out.push(ftype as u8);
        out.push(flags);
        wr_u24(out, len);
    }
    match frame {
        Frame::SynReply {
            stream_id,
            flags,
            headers,
        } => {
            let nvb = nv.compress_nv(headers)?;
            let mut body = Vec::with_capacity(4 + nvb.len());
            wr_u32(&mut body, stream_id & 0x7fff_ffff);
            body.extend_from_slice(&nvb);
            control_header(out, TYPE_SYN_REPLY, *flags, body.len());
            out.extend_from_slice(&body);
        }
        Frame::SynStream {
            stream_id,
            flags,
            headers,
        } => {
            let nvb = nv.compress_nv(headers)?;
            let mut body = Vec::with_capacity(10 + nvb.len());
            wr_u32(&mut body, stream_id & 0x7fff_ffff);
            wr_u32(&mut body, 0); // assoc
            body.push(0); // priority|unused
            body.push(0); // slot
            body.extend_from_slice(&nvb);
            control_header(out, TYPE_SYN_STREAM, *flags, body.len());
            out.extend_from_slice(&body);
        }
        Frame::RstStream { stream_id, status } => {
            control_header(out, TYPE_RST_STREAM, 0, 8);
            wr_u32(out, stream_id & 0x7fff_ffff);
            wr_u32(out, *status);
        }
        Frame::Settings { entries } => {
            control_header(out, TYPE_SETTINGS, 0, 4 + entries.len() * 8);
            wr_u32(out, entries.len() as u32);
            for (id, value) in entries {
                out.push(0); // flags
                wr_u24(out, *id as usize);
                wr_u32(out, *value);
            }
        }
        Frame::Ping { id } => {
            control_header(out, TYPE_PING, 0, 4);
            wr_u32(out, *id);
        }
        Frame::GoAway {
            last_good_stream_id,
            status,
        } => {
            control_header(out, TYPE_GOAWAY, 0, 8);
            wr_u32(out, last_good_stream_id & 0x7fff_ffff);
            wr_u32(out, *status);
        }
        Frame::Headers {
            stream_id,
            flags,
            headers,
        } => {
            let nvb = nv.compress_nv(headers)?;
            let mut body = Vec::with_capacity(4 + nvb.len());
            wr_u32(&mut body, stream_id & 0x7fff_ffff);
            body.extend_from_slice(&nvb);
            control_header(out, TYPE_HEADERS, *flags, body.len());
            out.extend_from_slice(&body);
        }
        Frame::WindowUpdate { stream_id, delta } => {
            control_header(out, TYPE_WINDOW_UPDATE, 0, 8);
            wr_u32(out, stream_id & 0x7fff_ffff);
            wr_u32(out, delta & 0x7fff_ffff);
        }
        Frame::Data {
            stream_id,
            flags,
            payload,
        } => {
            wr_u32(out, stream_id & 0x7fff_ffff);
            out.push(*flags);
            wr_u24(out, payload.len());
            out.extend_from_slice(payload);
        }
    }
    Ok(())
}

/// The fixed SPDY/3 zlib header-compression dictionary (verbatim from
/// `github.com/moby/spdystream`, the client the kubelet uses). Must be
/// byte-exact or the peer's inflate fails.
#[rustfmt::skip]
pub const SPDY_DICTIONARY: &[u8] = &[
    0x00, 0x00, 0x00, 0x07, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x00, 0x00, 0x00, 0x04, 0x68,
    0x65, 0x61, 0x64, 0x00, 0x00, 0x00, 0x04, 0x70, 0x6f, 0x73, 0x74, 0x00, 0x00, 0x00, 0x03, 0x70,
    0x75, 0x74, 0x00, 0x00, 0x00, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x00, 0x00, 0x00, 0x05,
    0x74, 0x72, 0x61, 0x63, 0x65, 0x00, 0x00, 0x00, 0x06, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x00,
    0x00, 0x00, 0x0e, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x2d, 0x63, 0x68, 0x61, 0x72, 0x73, 0x65,
    0x74, 0x00, 0x00, 0x00, 0x0f, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x2d, 0x65, 0x6e, 0x63, 0x6f,
    0x64, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x0f, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x2d, 0x6c,
    0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x00, 0x00, 0x00, 0x0d, 0x61, 0x63, 0x63, 0x65, 0x70,
    0x74, 0x2d, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x00, 0x00, 0x00, 0x03, 0x61, 0x67, 0x65, 0x00,
    0x00, 0x00, 0x05, 0x61, 0x6c, 0x6c, 0x6f, 0x77, 0x00, 0x00, 0x00, 0x0d, 0x61, 0x75, 0x74, 0x68,
    0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x0d, 0x63, 0x61, 0x63,
    0x68, 0x65, 0x2d, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x0a, 0x63, 0x6f,
    0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x0c, 0x63, 0x6f, 0x6e, 0x74,
    0x65, 0x6e, 0x74, 0x2d, 0x62, 0x61, 0x73, 0x65, 0x00, 0x00, 0x00, 0x10, 0x63, 0x6f, 0x6e, 0x74,
    0x65, 0x6e, 0x74, 0x2d, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x10,
    0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65,
    0x00, 0x00, 0x00, 0x0e, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x6c, 0x65, 0x6e, 0x67,
    0x74, 0x68, 0x00, 0x00, 0x00, 0x10, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x6c, 0x6f,
    0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x0b, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e,
    0x74, 0x2d, 0x6d, 0x64, 0x35, 0x00, 0x00, 0x00, 0x0d, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74,
    0x2d, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x00, 0x00, 0x00, 0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e,
    0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x00, 0x00, 0x00, 0x04, 0x64, 0x61, 0x74, 0x65, 0x00, 0x00,
    0x00, 0x04, 0x65, 0x74, 0x61, 0x67, 0x00, 0x00, 0x00, 0x06, 0x65, 0x78, 0x70, 0x65, 0x63, 0x74,
    0x00, 0x00, 0x00, 0x07, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x73, 0x00, 0x00, 0x00, 0x04, 0x66,
    0x72, 0x6f, 0x6d, 0x00, 0x00, 0x00, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x00, 0x00, 0x00, 0x08, 0x69,
    0x66, 0x2d, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x00, 0x00, 0x00, 0x11, 0x69, 0x66, 0x2d, 0x6d, 0x6f,
    0x64, 0x69, 0x66, 0x69, 0x65, 0x64, 0x2d, 0x73, 0x69, 0x6e, 0x63, 0x65, 0x00, 0x00, 0x00, 0x0d,
    0x69, 0x66, 0x2d, 0x6e, 0x6f, 0x6e, 0x65, 0x2d, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x00, 0x00, 0x00,
    0x08, 0x69, 0x66, 0x2d, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x00, 0x00, 0x00, 0x13, 0x69, 0x66, 0x2d,
    0x75, 0x6e, 0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x65, 0x64, 0x2d, 0x73, 0x69, 0x6e, 0x63, 0x65,
    0x00, 0x00, 0x00, 0x0d, 0x6c, 0x61, 0x73, 0x74, 0x2d, 0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x65,
    0x64, 0x00, 0x00, 0x00, 0x08, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00,
    0x0c, 0x6d, 0x61, 0x78, 0x2d, 0x66, 0x6f, 0x72, 0x77, 0x61, 0x72, 0x64, 0x73, 0x00, 0x00, 0x00,
    0x06, 0x70, 0x72, 0x61, 0x67, 0x6d, 0x61, 0x00, 0x00, 0x00, 0x12, 0x70, 0x72, 0x6f, 0x78, 0x79,
    0x2d, 0x61, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74, 0x65, 0x00, 0x00, 0x00,
    0x13, 0x70, 0x72, 0x6f, 0x78, 0x79, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61,
    0x74, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x00, 0x00, 0x00,
    0x07, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x72, 0x00, 0x00, 0x00, 0x0b, 0x72, 0x65, 0x74, 0x72,
    0x79, 0x2d, 0x61, 0x66, 0x74, 0x65, 0x72, 0x00, 0x00, 0x00, 0x06, 0x73, 0x65, 0x72, 0x76, 0x65,
    0x72, 0x00, 0x00, 0x00, 0x02, 0x74, 0x65, 0x00, 0x00, 0x00, 0x07, 0x74, 0x72, 0x61, 0x69, 0x6c,
    0x65, 0x72, 0x00, 0x00, 0x00, 0x11, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x66, 0x65, 0x72, 0x2d, 0x65,
    0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x07, 0x75, 0x70, 0x67, 0x72, 0x61,
    0x64, 0x65, 0x00, 0x00, 0x00, 0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67, 0x65, 0x6e, 0x74,
    0x00, 0x00, 0x00, 0x04, 0x76, 0x61, 0x72, 0x79, 0x00, 0x00, 0x00, 0x03, 0x76, 0x69, 0x61, 0x00,
    0x00, 0x00, 0x07, 0x77, 0x61, 0x72, 0x6e, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x10, 0x77, 0x77,
    0x77, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74, 0x65, 0x00, 0x00,
    0x00, 0x06, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x00, 0x00, 0x00, 0x03, 0x67, 0x65, 0x74, 0x00,
    0x00, 0x00, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x00, 0x00, 0x00, 0x06, 0x32, 0x30, 0x30,
    0x20, 0x4f, 0x4b, 0x00, 0x00, 0x00, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00,
    0x00, 0x08, 0x48, 0x54, 0x54, 0x50, 0x2f, 0x31, 0x2e, 0x31, 0x00, 0x00, 0x00, 0x03, 0x75, 0x72,
    0x6c, 0x00, 0x00, 0x00, 0x06, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x00, 0x00, 0x00, 0x0a, 0x73,
    0x65, 0x74, 0x2d, 0x63, 0x6f, 0x6f, 0x6b, 0x69, 0x65, 0x00, 0x00, 0x00, 0x0a, 0x6b, 0x65, 0x65,
    0x70, 0x2d, 0x61, 0x6c, 0x69, 0x76, 0x65, 0x00, 0x00, 0x00, 0x06, 0x6f, 0x72, 0x69, 0x67, 0x69,
    0x6e, 0x31, 0x30, 0x30, 0x31, 0x30, 0x31, 0x32, 0x30, 0x31, 0x32, 0x30, 0x32, 0x32, 0x30, 0x35,
    0x32, 0x30, 0x36, 0x33, 0x30, 0x30, 0x33, 0x30, 0x32, 0x33, 0x30, 0x33, 0x33, 0x30, 0x34, 0x33,
    0x30, 0x35, 0x33, 0x30, 0x36, 0x33, 0x30, 0x37, 0x34, 0x30, 0x32, 0x34, 0x30, 0x35, 0x34, 0x30,
    0x36, 0x34, 0x30, 0x37, 0x34, 0x30, 0x38, 0x34, 0x30, 0x39, 0x34, 0x31, 0x30, 0x34, 0x31, 0x31,
    0x34, 0x31, 0x32, 0x34, 0x31, 0x33, 0x34, 0x31, 0x34, 0x34, 0x31, 0x35, 0x34, 0x31, 0x36, 0x34,
    0x31, 0x37, 0x35, 0x30, 0x32, 0x35, 0x30, 0x34, 0x35, 0x30, 0x35, 0x32, 0x30, 0x33, 0x20, 0x4e,
    0x6f, 0x6e, 0x2d, 0x41, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x74, 0x61, 0x74, 0x69, 0x76, 0x65,
    0x20, 0x49, 0x6e, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x32, 0x30, 0x34, 0x20,
    0x4e, 0x6f, 0x20, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x33, 0x30, 0x31, 0x20, 0x4d, 0x6f,
    0x76, 0x65, 0x64, 0x20, 0x50, 0x65, 0x72, 0x6d, 0x61, 0x6e, 0x65, 0x6e, 0x74, 0x6c, 0x79, 0x34,
    0x30, 0x30, 0x20, 0x42, 0x61, 0x64, 0x20, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x34, 0x30,
    0x31, 0x20, 0x55, 0x6e, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x65, 0x64, 0x34, 0x30,
    0x33, 0x20, 0x46, 0x6f, 0x72, 0x62, 0x69, 0x64, 0x64, 0x65, 0x6e, 0x34, 0x30, 0x34, 0x20, 0x4e,
    0x6f, 0x74, 0x20, 0x46, 0x6f, 0x75, 0x6e, 0x64, 0x35, 0x30, 0x30, 0x20, 0x49, 0x6e, 0x74, 0x65,
    0x72, 0x6e, 0x61, 0x6c, 0x20, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x45, 0x72, 0x72, 0x6f,
    0x72, 0x35, 0x30, 0x31, 0x20, 0x4e, 0x6f, 0x74, 0x20, 0x49, 0x6d, 0x70, 0x6c, 0x65, 0x6d, 0x65,
    0x6e, 0x74, 0x65, 0x64, 0x35, 0x30, 0x33, 0x20, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x20,
    0x55, 0x6e, 0x61, 0x76, 0x61, 0x69, 0x6c, 0x61, 0x62, 0x6c, 0x65, 0x4a, 0x61, 0x6e, 0x20, 0x46,
    0x65, 0x62, 0x20, 0x4d, 0x61, 0x72, 0x20, 0x41, 0x70, 0x72, 0x20, 0x4d, 0x61, 0x79, 0x20, 0x4a,
    0x75, 0x6e, 0x20, 0x4a, 0x75, 0x6c, 0x20, 0x41, 0x75, 0x67, 0x20, 0x53, 0x65, 0x70, 0x74, 0x20,
    0x4f, 0x63, 0x74, 0x20, 0x4e, 0x6f, 0x76, 0x20, 0x44, 0x65, 0x63, 0x20, 0x30, 0x30, 0x3a, 0x30,
    0x30, 0x3a, 0x30, 0x30, 0x20, 0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x54, 0x75, 0x65, 0x2c, 0x20, 0x57,
    0x65, 0x64, 0x2c, 0x20, 0x54, 0x68, 0x75, 0x2c, 0x20, 0x46, 0x72, 0x69, 0x2c, 0x20, 0x53, 0x61,
    0x74, 0x2c, 0x20, 0x53, 0x75, 0x6e, 0x2c, 0x20, 0x47, 0x4d, 0x54, 0x63, 0x68, 0x75, 0x6e, 0x6b,
    0x65, 0x64, 0x2c, 0x74, 0x65, 0x78, 0x74, 0x2f, 0x68, 0x74, 0x6d, 0x6c, 0x2c, 0x69, 0x6d, 0x61,
    0x67, 0x65, 0x2f, 0x70, 0x6e, 0x67, 0x2c, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2f, 0x6a, 0x70, 0x67,
    0x2c, 0x69, 0x6d, 0x61, 0x67, 0x65, 0x2f, 0x67, 0x69, 0x66, 0x2c, 0x61, 0x70, 0x70, 0x6c, 0x69,
    0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x78, 0x6d, 0x6c, 0x2c, 0x61, 0x70, 0x70, 0x6c, 0x69,
    0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x78, 0x68, 0x74, 0x6d, 0x6c, 0x2b, 0x78, 0x6d, 0x6c,
    0x2c, 0x74, 0x65, 0x78, 0x74, 0x2f, 0x70, 0x6c, 0x61, 0x69, 0x6e, 0x2c, 0x74, 0x65, 0x78, 0x74,
    0x2f, 0x6a, 0x61, 0x76, 0x61, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x2c, 0x70, 0x75, 0x62, 0x6c,
    0x69, 0x63, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74, 0x65, 0x6d, 0x61, 0x78, 0x2d, 0x61, 0x67, 0x65,
    0x3d, 0x67, 0x7a, 0x69, 0x70, 0x2c, 0x64, 0x65, 0x66, 0x6c, 0x61, 0x74, 0x65, 0x2c, 0x73, 0x64,
    0x63, 0x68, 0x63, 0x68, 0x61, 0x72, 0x73, 0x65, 0x74, 0x3d, 0x75, 0x74, 0x66, 0x2d, 0x38, 0x63,
    0x68, 0x61, 0x72, 0x73, 0x65, 0x74, 0x3d, 0x69, 0x73, 0x6f, 0x2d, 0x38, 0x38, 0x35, 0x39, 0x2d,
    0x31, 0x2c, 0x75, 0x74, 0x66, 0x2d, 0x2c, 0x2a, 0x2c, 0x65, 0x6e, 0x71, 0x3d, 0x30, 0x2e,
];

/// Persistent zlib (de)compressor for the SPDY NV header blocks. SPDY keeps one
/// zlib stream per direction for the life of the connection (compression state
/// carries across frames), seeded with the fixed dictionary.
pub struct NvCodec {
    deflate: Compress,
    inflate: Decompress,
    inflate_dict_set: bool,
}

impl Default for NvCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl NvCodec {
    pub fn new() -> Self {
        let mut deflate = Compress::new(Compression::default(), true);
        // Seed the deflate dictionary up front (zlib: deflateSetDictionary right
        // after init, before any deflate).
        deflate
            .set_dictionary(SPDY_DICTIONARY)
            .expect("set deflate dictionary");
        NvCodec {
            deflate,
            inflate: Decompress::new(true),
            inflate_dict_set: false,
        }
    }

    /// Compress a plaintext NV block (per-frame, flushed with Z_SYNC_FLUSH).
    pub fn compress_nv(&mut self, headers: &[(String, String)]) -> io::Result<Vec<u8>> {
        let plain = write_nv_block(headers);
        let mut out = Vec::with_capacity(plain.len());
        let mut in_off = 0;
        loop {
            let before_in = self.deflate.total_in();
            let before_out = self.deflate.total_out();
            self.deflate
                .compress_vec(&plain[in_off..], &mut out, FlushCompress::Sync)
                .map_err(|e| io::Error::other(format!("spdy deflate: {e:?}")))?;
            in_off += (self.deflate.total_in() - before_in) as usize;
            let produced = self.deflate.total_out() - before_out;
            if in_off >= plain.len() && produced == 0 {
                break;
            }
            if out.len() == out.capacity() {
                out.reserve(256);
            }
            if in_off >= plain.len() && produced > 0 {
                // Drain any remaining flushed bytes.
                let b2 = self.deflate.total_out();
                self.deflate
                    .compress_vec(&[], &mut out, FlushCompress::Sync)
                    .map_err(|e| io::Error::other(format!("spdy deflate: {e:?}")))?;
                if self.deflate.total_out() == b2 {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Decompress a SPDY NV header block into name/value pairs.
    pub fn decompress_nv(&mut self, raw: &[u8]) -> io::Result<Vec<(String, String)>> {
        let mut out = Vec::with_capacity(raw.len() * 4 + 64);
        let mut in_off = 0;
        loop {
            let before_in = self.inflate.total_in();
            let before_out = self.inflate.total_out();
            match self
                .inflate
                .decompress_vec(&raw[in_off..], &mut out, FlushDecompress::Sync)
            {
                Ok(_) => {}
                Err(e) if e.needs_dictionary().is_some() && !self.inflate_dict_set => {
                    self.inflate
                        .set_dictionary(SPDY_DICTIONARY)
                        .map_err(|e| io::Error::other(format!("spdy inflate dict: {e:?}")))?;
                    self.inflate_dict_set = true;
                    in_off += (self.inflate.total_in() - before_in) as usize;
                    continue;
                }
                Err(e) => return Err(io::Error::other(format!("spdy inflate: {e:?}"))),
            }
            in_off += (self.inflate.total_in() - before_in) as usize;
            let produced = self.inflate.total_out() - before_out;
            if in_off >= raw.len() && produced == 0 {
                break;
            }
            if out.len() == out.capacity() {
                out.reserve(out.capacity().max(256));
            }
        }
        read_nv_block(&out)
    }
}

/// Serialize name/value pairs into the SPDY/3 plaintext NV block layout:
/// `u32 count`, then per pair `u32 name_len, name, u32 value_len, value`.
pub fn write_nv_block(headers: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    wr_u32(&mut out, headers.len() as u32);
    for (k, v) in headers {
        wr_u32(&mut out, k.len() as u32);
        out.extend_from_slice(k.as_bytes());
        wr_u32(&mut out, v.len() as u32);
        out.extend_from_slice(v.as_bytes());
    }
    out
}

/// Parse a SPDY/3 plaintext NV block.
pub fn read_nv_block(buf: &[u8]) -> io::Result<Vec<(String, String)>> {
    if buf.len() < 4 {
        return Ok(Vec::new());
    }
    let count = rd_u32(&buf[0..4]) as usize;
    let mut off = 4;
    let mut pairs = Vec::with_capacity(count);
    let take = |buf: &[u8], off: &mut usize| -> io::Result<String> {
        if *off + 4 > buf.len() {
            return Err(io::Error::other("spdy nv: truncated length"));
        }
        let n = rd_u32(&buf[*off..*off + 4]) as usize;
        *off += 4;
        if *off + n > buf.len() {
            return Err(io::Error::other("spdy nv: truncated value"));
        }
        let s = String::from_utf8_lossy(&buf[*off..*off + n]).into_owned();
        *off += n;
        Ok(s)
    };
    for _ in 0..count {
        let name = take(buf, &mut off)?;
        let value = take(buf, &mut off)?;
        pairs.push((name, value));
    }
    Ok(pairs)
}

/// Look up a header (case-insensitive) in an NV list.
pub fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Parse a remotecommand resize message `{"Width":W,"Height":H}`.
pub fn parse_resize(json: &[u8]) -> Option<(u16, u16)> {
    let v: serde_json::Value = serde_json::from_slice(json).ok()?;
    let w = v.get("Width")?.as_u64()? as u16;
    let h = v.get("Height")?.as_u64()? as u16;
    Some((w, h))
}

// ---- metav1.Status payloads (must match the WebSocket path semantics) ----
pub fn status_success() -> Vec<u8> {
    br#"{"status":"Success"}"#.to_vec()
}
pub fn status_failure(msg: &str) -> Vec<u8> {
    serde_json::json!({"status":"Failure","message":msg})
        .to_string()
        .into_bytes()
}
pub fn status_exit(code: i32) -> Vec<u8> {
    if code == 0 {
        return status_success();
    }
    serde_json::json!({
        "status":"Failure","reason":"NonZeroExitCode",
        "details":{"causes":[{"reason":"ExitCode","message":code.to_string()}]}
    })
    .to_string()
    .into_bytes()
}

// ===================== connection multiplexer =====================

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};

/// An accepted inbound SPDY stream (the server has already SYN_REPLY'd it).
/// `data` yields inbound DATA payloads and closes on FLAG_FIN / RST_STREAM.
pub struct SpdyStream {
    pub id: u32,
    pub headers: Vec<(String, String)>,
    pub data: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl SpdyStream {
    /// The Kubernetes `streamType` of this stream, if present.
    pub fn stream_type(&self) -> Option<&str> {
        header(&self.headers, HEADER_STREAM_TYPE)
    }
}

struct WriterInner<W: AsyncWrite + Unpin> {
    w: W,
    nv: NvCodec, // server->client deflate state (serialized behind the mutex)
}

/// Cloneable write handle to a SPDY connection. All frames (including the
/// header-bearing SYN_REPLY) serialize behind one mutex so the deflate stream
/// stays ordered.
pub struct SpdyWriter<W: AsyncWrite + Unpin> {
    inner: Arc<Mutex<WriterInner<W>>>,
}

impl<W: AsyncWrite + Unpin> Clone for SpdyWriter<W> {
    fn clone(&self) -> Self {
        SpdyWriter {
            inner: self.inner.clone(),
        }
    }
}

impl<W: AsyncWrite + Unpin> SpdyWriter<W> {
    async fn write(&self, frame: &Frame) -> io::Result<()> {
        let mut g = self.inner.lock().await;
        let mut buf = Vec::new();
        write_frame(&mut buf, frame, &mut g.nv)?;
        g.w.write_all(&buf).await?;
        g.w.flush().await
    }
    async fn syn_reply(&self, stream_id: u32) -> io::Result<()> {
        self.write(&Frame::SynReply {
            stream_id,
            flags: 0,
            headers: Vec::new(),
        })
        .await
    }
    pub async fn send_data(&self, stream_id: u32, fin: bool, data: &[u8]) -> io::Result<()> {
        self.write(&Frame::Data {
            stream_id,
            flags: if fin { FLAG_FIN } else { 0 },
            payload: data.to_vec(),
        })
        .await
    }
    pub async fn rst(&self, stream_id: u32, status: u32) -> io::Result<()> {
        self.write(&Frame::RstStream { stream_id, status }).await
    }
    pub async fn goaway(&self, last_good_stream_id: u32) -> io::Result<()> {
        self.write(&Frame::GoAway {
            last_good_stream_id,
            status: 0,
        })
        .await
    }
}

/// A served SPDY connection: a writer handle plus a queue of inbound streams.
pub struct SpdyServer<W: AsyncWrite + Unpin> {
    pub writer: SpdyWriter<W>,
    incoming: mpsc::UnboundedReceiver<SpdyStream>,
}

impl<W: AsyncWrite + Unpin> SpdyServer<W> {
    /// Next inbound stream, or `None` once the peer closed the connection.
    pub async fn accept(&mut self) -> Option<SpdyStream> {
        self.incoming.recv().await
    }
}

/// Drive SPDY over an upgraded byte stream: spawn the read loop and return a
/// server handle. Sends an initial SETTINGS advertising a large window so we can
/// ignore flow control.
pub async fn serve<S>(io: S) -> io::Result<SpdyServer<tokio::io::WriteHalf<S>>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (rd, wr) = tokio::io::split(io);
    let writer = SpdyWriter {
        inner: Arc::new(Mutex::new(WriterInner {
            w: wr,
            nv: NvCodec::new(),
        })),
    };
    writer
        .write(&Frame::Settings {
            entries: vec![(SETTINGS_INITIAL_WINDOW_SIZE, BIG_WINDOW)],
        })
        .await?;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(read_loop(rd, writer.clone(), tx));
    Ok(SpdyServer {
        writer,
        incoming: rx,
    })
}

async fn read_loop<R, W>(
    mut rd: R,
    writer: SpdyWriter<W>,
    incoming: mpsc::UnboundedSender<SpdyStream>,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nv = NvCodec::new(); // client->server inflate state
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut streams: HashMap<u32, mpsc::UnboundedSender<Vec<u8>>> = HashMap::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        // Drain all complete frames currently buffered.
        loop {
            let parsed = match parse_frame(&buf, &mut nv) {
                Ok(Some((frame, consumed))) => {
                    buf.drain(..consumed);
                    frame
                }
                Ok(None) => break,
                Err(_) => return, // protocol error: close the connection
            };
            match parsed {
                Frame::SynStream {
                    stream_id, headers, ..
                } => {
                    if writer.syn_reply(stream_id).await.is_err() {
                        return;
                    }
                    let (dtx, drx) = mpsc::unbounded_channel();
                    streams.insert(stream_id, dtx);
                    if incoming
                        .send(SpdyStream {
                            id: stream_id,
                            headers,
                            data: drx,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Frame::Data {
                    stream_id,
                    flags,
                    payload,
                } => {
                    if !payload.is_empty() {
                        if let Some(tx) = streams.get(&stream_id) {
                            let _ = tx.send(payload);
                        }
                    }
                    if flags & FLAG_FIN != 0 {
                        streams.remove(&stream_id); // closes the receiver
                    }
                }
                Frame::Ping { id } => {
                    let _ = writer.write(&Frame::Ping { id }).await;
                }
                Frame::RstStream { stream_id, .. } => {
                    streams.remove(&stream_id);
                }
                Frame::GoAway { .. } => return,
                // Settings / WindowUpdate / Headers / SynReply: ignore.
                _ => {}
            }
        }
        match rd.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nv_block_plaintext_roundtrip() {
        let h = vec![
            ("streamType".to_string(), "stdout".to_string()),
            ("port".to_string(), "8080".to_string()),
        ];
        let bytes = write_nv_block(&h);
        let back = read_nv_block(&bytes).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn nv_codec_roundtrip_across_frames() {
        // The deflate/inflate streams are stateful; a second block must still
        // decode against the same codecs (cross-frame state preserved).
        let mut enc = NvCodec::new();
        let mut dec = NvCodec::new();
        for h in [
            vec![("streamType".to_string(), "error".to_string())],
            vec![
                ("streamType".to_string(), "stdin".to_string()),
                ("x".to_string(), "y".to_string()),
            ],
        ] {
            let c = enc.compress_nv(&h).unwrap();
            let back = dec.decompress_nv(&c).unwrap();
            assert_eq!(h, back);
        }
    }

    #[test]
    fn dictionary_length_is_exact() {
        // moby/spdystream headerDictionary length (guards accidental edits).
        assert_eq!(SPDY_DICTIONARY.len(), 1423);
    }

    #[test]
    fn frame_roundtrip_data_and_control() {
        let mut nv = NvCodec::new();
        let mut dec_nv = NvCodec::new();
        let frames = vec![
            Frame::Data {
                stream_id: 1,
                flags: FLAG_FIN,
                payload: b"hello".to_vec(),
            },
            Frame::RstStream {
                stream_id: 3,
                status: RST_CANCEL,
            },
            Frame::Ping { id: 42 },
            Frame::GoAway {
                last_good_stream_id: 5,
                status: 0,
            },
            Frame::Settings {
                entries: vec![(SETTINGS_INITIAL_WINDOW_SIZE, BIG_WINDOW)],
            },
        ];
        for f in &frames {
            let mut buf = Vec::new();
            write_frame(&mut buf, f, &mut nv).unwrap();
            let (parsed, n) = parse_frame(&buf, &mut dec_nv).unwrap().unwrap();
            assert_eq!(n, buf.len());
            assert_eq!(&parsed, f);
        }
    }

    #[test]
    fn syn_reply_roundtrip_with_headers() {
        let mut enc = NvCodec::new();
        let mut dec = NvCodec::new();
        let f = Frame::SynReply {
            stream_id: 1,
            flags: 0,
            headers: vec![("streamType".to_string(), "stdout".to_string())],
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &f, &mut enc).unwrap();
        // A SYN_REPLY parses via the same NV decode path SYN_STREAM uses.
        let (parsed, n) = parse_frame(&buf, &mut dec).unwrap().unwrap();
        assert_eq!(n, buf.len());
        assert_eq!(parsed, f);
    }

    #[test]
    fn parse_incomplete_returns_none() {
        let mut nv = NvCodec::new();
        let mut buf = Vec::new();
        write_frame(&mut buf, &Frame::Ping { id: 7 }, &mut NvCodec::new()).unwrap();
        // Truncated: not enough bytes yet.
        assert!(parse_frame(&buf[..4], &mut nv).unwrap().is_none());
    }

    #[test]
    fn status_payloads() {
        assert_eq!(status_exit(0), status_success());
        let s = String::from_utf8(status_exit(2)).unwrap();
        assert!(s.contains("NonZeroExitCode") && s.contains("\"2\""));
    }

    #[test]
    fn resize_parse() {
        assert_eq!(
            parse_resize(br#"{"Width":120,"Height":40}"#),
            Some((120, 40))
        );
        assert_eq!(parse_resize(b"garbage"), None);
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Golden vector: the exact bytes a real kubelet (`moby/spdystream`, the
    /// reference client) sent to our streaming server on `kubectl exec` — the
    /// three SYN_STREAM frames opening the error/stdout/stderr streams, captured
    /// from the kubeadm harness. Proves our inflate + the SPDY/3 dictionary
    /// decode the production client's compressed NV headers byte-for-byte.
    #[test]
    fn decodes_real_kubelet_syn_streams() {
        let bytes = unhex(concat!(
            "80030001000000290000000100000000000078f9e3c6a7c26260606004a7b392",
            "a2d4c45c6851c29a0a0a6800000000ffff800300010000001900000003000000",
            "000000c22ac5565c92925f5a02000000ffff8003000100000015000000050000",
            "00000000c229975a5404000000ffff",
        ));
        let mut nv = NvCodec::new();
        let mut off = 0;
        let mut syn = Vec::new();
        while let Some((frame, n)) = parse_frame(&bytes[off..], &mut nv).unwrap() {
            if n == 0 {
                break;
            }
            off += n;
            if let Frame::SynStream {
                stream_id, headers, ..
            } = frame
            {
                let st = header(&headers, HEADER_STREAM_TYPE)
                    .unwrap_or("")
                    .to_ascii_lowercase();
                syn.push((stream_id, st));
            }
        }
        let ids: Vec<u32> = syn.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![1, 3, 5], "stream ids");
        let mut types: Vec<String> = syn.iter().map(|(_, t)| t.clone()).collect();
        types.sort();
        assert_eq!(
            types,
            vec!["error", "stderr", "stdout"],
            "stream types: {syn:?}"
        );
    }
}
