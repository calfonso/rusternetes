#!/usr/bin/env bash
#
# Recover host DNS after rusternetes-kube-proxy poisoned the host's
# iptables. Symptoms it fixes:
#   - `getent hosts github.com` works but `curl` / browser / docker pull fail
#   - Containers can't resolve anything (DNS timeouts to 127.0.0.11)
#   - systemd-resolved stub at 127.0.0.53:53 unreachable
#
# These happen because a previous kube-proxy run left RUSTERNETES-SERVICES,
# RUSTERNETES-NODEPORTS, and/or KUBE-FORWARD chains (with their jump rules)
# on the host's iptables. After this script:
#   - All RUSTERNETES-* / KUBE-FORWARD chains and jumps are gone.
#   - Docker's own iptables are rebuilt by restarting docker.service.
#   - systemd-resolved is restarted so the stub listener is fresh.
#
# Idempotent — safe to re-run. Requires sudo.
#
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
NC='\033[0m'

step() { echo -e "\n${BLUE}==>${NC} $1"; }
ok()   { echo -e "${GREEN}✓${NC} $1"; }
warn() { echo -e "${YELLOW}!${NC} $1"; }
fail() { echo -e "${RED}✗${NC} $1" >&2; }
die()  { fail "$1"; exit 1; }

if [ "$(id -u)" -ne 0 ]; then
  warn "this script needs root for iptables + systemctl — re-running under sudo"
  exec sudo -E bash "$0" "$@"
fi

IPT="${IPT:-iptables}"
if ! command -v "$IPT" >/dev/null 2>&1; then
  die "iptables not found"
fi

step "1/4 — remove jump rules from host's host-level chains"
# Each `-D` is best-effort: rule may or may not exist on any given system.
# Loop until -D fails (no more matching rules) so duplicate jumps are
# all cleared.
remove_all_jumps_to() {
  local table="$1" from_chain="$2" target="$3"
  while "$IPT" -t "$table" -D "$from_chain" -j "$target" 2>/dev/null; do
    :
  done
}
for tgt in RUSTERNETES-SERVICES RUSTERNETES-NODEPORTS; do
  remove_all_jumps_to nat PREROUTING "$tgt"
  remove_all_jumps_to nat OUTPUT     "$tgt"
done
remove_all_jumps_to filter FORWARD KUBE-FORWARD
remove_all_jumps_to filter OUTPUT  KUBE-FORWARD

# Also remove the over-broad POSTROUTING MASQUERADE rules that older
# kube-proxy versions installed directly on the host. These are NOT
# inside our managed chains, so flush_rules / delete_chain wouldn't
# touch them — they persist forever otherwise.
remove_all_matching() {
  local table="$1"; shift
  while "$IPT" -t "$table" -D "$@" 2>/dev/null; do
    :
  done
}
remove_all_matching nat POSTROUTING \
  -m comment --comment "rusternetes DNAT traffic masquerade" \
  -m conntrack --ctstate DNAT -j MASQUERADE
remove_all_matching nat POSTROUTING \
  -m comment --comment "rusternetes ClusterIP DNAT masquerade" \
  -m conntrack --ctstate DNAT --ctorigdst 10.96.0.0/12 -j MASQUERADE
remove_all_matching nat POSTROUTING \
  -m comment --comment "rusternetes nodeport masquerade" \
  -m addrtype --src-type LOCAL -j MASQUERADE
for proto in tcp udp; do
  remove_all_matching nat POSTROUTING \
    -m comment --comment "rusternetes NodePort masquerade" \
    -p "$proto" \
    -m conntrack --ctstate DNAT --ctorigdstport 30000:32767 -j MASQUERADE
done
# Hairpin MASQUERADE is scoped to a bridge CIDR. Discover and remove any
# rule whose comment matches, regardless of CIDR.
#
# The previous shape of this loop was `... | head -1 | read -r line`,
# which is buggy: `read` in a pipeline runs in a subshell so `$line` is
# empty in the loop body and the hairpin rule never gets deleted.
#
# Using `read -ra cmd <<< "$line"` is also wrong here because plain bash
# word-splitting doesn't honor the double-quotes that `iptables -S`
# emits around comment values — the comment `"rusternetes service
# hairpin masquerade"` would be torn into 4 separate args.
#
# `xargs` reads stdin with shell-style quoting, so a quoted comment
# stays a single argument. Loop until no hairpin rule is left.
while :; do
  line=$("$IPT" -t nat -S POSTROUTING 2>/dev/null \
         | grep -F 'rusternetes service hairpin masquerade' | head -1)
  [ -n "$line" ] || break
  if ! echo "$line" | sed 's/^-A /-D /' | xargs "$IPT" -t nat 2>/dev/null; then
    break
  fi
done
ok "host jump rules + legacy MASQUERADE rules removed"

step "2/4 — flush and delete RUSTERNETES-* / KUBE-FORWARD chains"
# Discover any RUSTERNETES-* or KUBE-SEP-* / KUBE-NP-SEP-* chains and
# flush+delete them. The SEP chains are per-endpoint and named with
# random suffixes — `iptables -L` is the only way to enumerate.
flush_and_drop_chain() {
  local table="$1" chain="$2"
  "$IPT" -t "$table" -F "$chain" 2>/dev/null || true
  "$IPT" -t "$table" -X "$chain" 2>/dev/null || true
}
discovered_nat=$("$IPT" -t nat    -S 2>/dev/null | awk '/^-N (RUSTERNETES-|KUBE-SEP-|KUBE-NP-SEP-)/ {print $2}' || true)
discovered_flt=$("$IPT" -t filter -S 2>/dev/null | awk '/^-N (KUBE-FORWARD)/                              {print $2}' || true)
for c in $discovered_nat; do flush_and_drop_chain nat "$c"; done
for c in $discovered_flt; do flush_and_drop_chain filter "$c"; done
ok "chains flushed and removed"

step "3/4 — restart docker.service to rebuild its own iptables"
if systemctl is-active --quiet docker.service; then
  systemctl restart docker.service
  ok "docker.service restarted"
else
  warn "docker.service not active; skipping restart"
fi

step "4/4 — restart systemd-resolved"
if systemctl is-active --quiet systemd-resolved.service; then
  systemctl restart systemd-resolved.service
  ok "systemd-resolved restarted"
else
  warn "systemd-resolved not active; skipping restart"
fi

step "smoke-check DNS"
if getent hosts github.com >/dev/null; then
  ok "host can resolve github.com"
else
  fail "host still can't resolve github.com — check /etc/resolv.conf and reboot if needed"
  exit 2
fi
if docker info >/dev/null 2>&1; then
  if docker run --rm --network bridge alpine:3 getent hosts github.com >/dev/null 2>&1; then
    ok "container can resolve github.com"
  else
    warn "container DNS still failing — try one more `docker network prune` + restart"
  fi
fi

echo
echo -e "${GREEN}Host DNS recovered.${NC} If kube-proxy is still running, stop the cluster"
echo "first (\`docker compose down\`) or this script will need to be re-run."
