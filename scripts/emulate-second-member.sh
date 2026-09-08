#!/usr/bin/env bash
# Emulate a second v0.4.0 fleet member for local testing when no real peer is
# available (e.g. JD is out). Stands up a second gateway identity, enrolled by
# THIS machine's root into the same fleet, pointed at an ISOLATED registry
# (a Neon branch — never production), and proves the L4 signed-row + enforcement
# path end to end against live infra.
#
# Usage:
#   REGISTRY_URL='postgresql://…?sslmode=verify-full' scripts/emulate-second-member.sh
#
# REGISTRY_URL MUST use sslmode=verify-full (a2a's TLS is SPKI-pinned and rejects
# sslmode=require). Point it at a disposable Neon BRANCH of the mesh registry,
# forked at HEAD so it already carries the migrated schema and the current rows.
# NEVER point this at the production registry.env — it publishes throwaway rows.
#
# Leaves: an emul home under $EMUL, an emul member row in the branch registry.
# Delete the Neon branch when done to clean up.
set -euo pipefail

: "${REGISTRY_URL:?set REGISTRY_URL to an isolated branch URL (sslmode=verify-full)}"
A2A="${A2A:-a2a}"
MIKE_HOME="${MIKE_HOME:-$HOME/.a2a}"       # holds root.key (the fleet owner)
EMUL="${EMUL:-$(mktemp -d)/l4-emul}"

mkdir -p "$EMUL/home/mesh/fleet"
printf '%s\n' "$REGISTRY_URL" > "$EMUL/home/mesh/fleet/registry.env"
chmod 600 "$EMUL/home/mesh/fleet/registry.env"
cp "$MIKE_HOME/mesh/fleet/root.pub" "$EMUL/home/mesh/fleet/root.pub"
printf '[mesh.gateway]\nexpose = "none"\n' > "$EMUL/home/config.toml"

echo "== 1. emul member mints its identity =="
"$A2A" --home "$EMUL/home" mesh gateway identity --json \
  | python3 -c "import json,sys;d=json.load(sys.stdin)['data'];open('$EMUL/id','w').write(d['gateway_id']+'\n'+d['pubkey_b64']);print(' ',d['gateway_id'])"
GID=$(sed -n 1p "$EMUL/id"); PUB=$(sed -n 2p "$EMUL/id")

echo "== 2. this machine's root mints a token (root.key never leaves $MIKE_HOME) =="
"$A2A" --home "$MIKE_HOME" mesh fleet enroll --gateway-id "$GID" --pubkey "$PUB" | tail -1 > "$EMUL/token"

echo "== 3. emul installs the token (forces an immediate re-sign) =="
"$A2A" --home "$EMUL/home" mesh gateway install-token --file "$EMUL/token" | grep -v warning: || true

echo "== 4. emul publishes its signed row to the branch registry =="
"$A2A" --home "$EMUL/home" mesh gateway resign >/dev/null

echo "== 5. read the branch as the emul member: both v0.4.0 members should be valid-signed =="
"$A2A" --home "$EMUL/home" --json mesh fleet ls 2>/dev/null | tail -1 | python3 -c "
import json,sys
for r in json.load(sys.stdin)['data'].get('rows',[]):
    print('   %-24s %-14s v%s' % (r['gateway_id'], r.get('verdict','?'), r.get('version','?')))"

echo "== 6. enforcement: require_signed_rows=true refuses an unsigned peer at work-send =="
printf '[mesh.gateway]\nrequire_signed_rows = true\nexpose = "none"\n' > "$EMUL/home/config.toml"
UNSIGNED=$("$A2A" --home "$EMUL/home" --json mesh fleet ls 2>/dev/null | tail -1 \
  | python3 -c "import json,sys;print(next((r['gateway_id'] for r in json.load(sys.stdin)['data']['rows'] if r.get('verdict')=='UNSIGNED'),''))")
printf 'x\n' > "$EMUL/brief"
if [ -n "$UNSIGNED" ]; then
  "$A2A" --home "$EMUL/home" mesh work send --to "$UNSIGNED" --repo https://example.test/r.git \
    --base-ref main --title t --brief "$EMUL/brief" --acceptance x 2>&1 \
    | grep -oE 'WORK_PEER_ROW_INVALID' | head -1 || echo '   (no unsigned peer to refuse)'
fi

echo "== emul home: $EMUL/home  (delete the Neon branch to fully clean up) =="
