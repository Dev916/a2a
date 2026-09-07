CREATE TABLE IF NOT EXISTS mesh_work_items (
  fleet_id        text NOT NULL,
  work_id         text NOT NULL,
  sender_gw       text NOT NULL,
  addressing      text NOT NULL,          -- 'directed:<gw>' | 'pool'
  status          text NOT NULL,          -- §3
  claimed_by      text,                   -- gateway_id set by the claim CAS
  result_ref      text,                   -- branch name once done
  result_delivery text,                   -- 'delivered' | 'rejected:<code>' | 'release_stalled' | NULL
  swept_by        text,                   -- gateway that expired/abandoned it
  created_at      timestamptz NOT NULL,
  updated_at      timestamptz NOT NULL,
  expires_at      timestamptz NOT NULL,
  schema_version  integer NOT NULL,
  PRIMARY KEY (fleet_id, work_id)
);
