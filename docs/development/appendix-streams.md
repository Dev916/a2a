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

## Aging Detectors

Anti-entropy updates replication health with `last_anti_entropy` and
`per_origin_lag`. Sustained non-zero lag emits `kind: "drift"`, giving operators
a detector for links that remain connected but are no longer converging. The
same cycle re-checks active links against discovery; a peer missing from
discovery for two consecutive cycles is treated as revoked or retired and its
link is closed.
