//! Per-container parent process for the youki backend. See
//! `rusternetes_kubelet::backend::youki::shim`.

fn main() {
    std::process::exit(rusternetes_kubelet::backend::youki::shim::main(
        std::env::args().collect(),
    ));
}
