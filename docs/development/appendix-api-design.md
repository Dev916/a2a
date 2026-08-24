# API Design Appendix

## Mesh Injection Digest

`a2a mesh inject` is deliberately a bounded digest, not a paginated reader. Hooks
run at context-sensitive turn boundaries, so the common path needs either no
output or a compact block that preserves the next prompt's budget. When backlog
content exceeds the injection budget, the digest advances cursors fully and
points at the full-fidelity readers (`a2a mesh inbox` and `a2a mesh feed
--since <seq>`) instead of turning the ambient hook into a browsing session.
