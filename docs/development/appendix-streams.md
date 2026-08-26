# Appendix: Streams

## Mesh Anti-Entropy

Mesh feed replication is a grow-only per-origin stream. The join for entries is
the set union over `(origin, origin_seq)`, so the merge is commutative,
associative, and idempotent: peers may relay entries in either direction,
repeat frames, or receive the same origin through different gateways without
changing the final result.

Each link periodically exchanges a digest containing `{origin, max_seq, count}`.
A peer requests a catch-up range when its contiguous prefix is behind the
remote `max_seq`. It also requests repair when both peers report the same range
but the entry count differs; this detects a silently deleted mid-log entry that
a watermark alone would miss.

Catch-up replies are contiguous, oldest-first, and size-capped. Receivers only
advance a cursor across a contiguous prefix. Entries above a gap remain in a
bounded per-origin reorder buffer; when the cap evicts an entry the gateway logs
the eviction so the next anti-entropy cycle can make the loss visible and
repairable.

## Presence LWW

Presence is an observed-value cache, not a grow-only log, so it uses a
last-writer-wins register with a gateway-local counter ahead of wall time. A
presence update wins only when `(presence_seq, last_seen, gateway_id)` is
lexicographically greater than the cached tuple. The `presence_seq` is persisted
in gateway state and is monotonic for that gateway, so a peer whose clock moves
backward still supersedes its own older presence. `last_seen` remains useful
inside a single counter value, and `gateway_id` is the deterministic final
tiebreaker that makes replicas converge.

Remote-homed presence carries `home_gateway`; local staleness does not reclaim
that cache because the home gateway owns the peer's lifecycle. The local cache
is dropped only after the home gateway has disappeared from the registry, or is
revoked there, and its last heartbeat is older than the mesh GC horizon.
Retiring such an origin also removes its cursor and archives its outbound spool
with an `origin_retired` gateway log line, so a dead destination cannot remain
in silent retry forever.

## Aging Detectors

Anti-entropy updates replication health with `last_anti_entropy` and
`per_origin_lag`. Sustained non-zero lag emits `kind: "drift"`, giving operators
a detector for links that remain connected but are no longer converging. The
same cycle re-checks active links against discovery; a peer missing from
discovery for two consecutive cycles is treated as revoked or retired and its
link is closed.
