ALTER TABLE mesh_gateways
  ADD COLUMN IF NOT EXISTS row_msg    text,
  ADD COLUMN IF NOT EXISTS row_token  text,
  ADD COLUMN IF NOT EXISTS row_sig    text,
  ADD COLUMN IF NOT EXISTS signed_at  timestamptz,
  ADD COLUMN IF NOT EXISTS revocation text;
