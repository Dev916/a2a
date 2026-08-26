CREATE TABLE IF NOT EXISTS mesh_gateways (
  fleet_id       text NOT NULL,
  gateway_id     text NOT NULL,
  owner          text NOT NULL,
  endpoint_url   text,                      -- wss://...; NULL = enrolled but unreachable (outbound-only)
  pubkey_fpr     text NOT NULL,
  cert_fpr       text,                      -- SPKI sha256 of the gateway's TLS cert; NULL until first exposure
  version        text NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  last_heartbeat timestamptz NOT NULL DEFAULT now(),
  revoked_at     timestamptz,
  schema_version int  NOT NULL DEFAULT 1,
  PRIMARY KEY (fleet_id, gateway_id)
);
