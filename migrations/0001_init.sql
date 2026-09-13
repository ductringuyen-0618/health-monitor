CREATE TABLE IF NOT EXISTS targets (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  url                  text NOT NULL UNIQUE,
  webhook_url          text,
  status               text NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN ('PENDING','UP','DOWN')),
  consecutive_failures int  NOT NULL DEFAULT 0,
  last_checked_at      timestamptz,
  last_status_code     int,
  last_error           text,
  next_check_at        timestamptz NOT NULL DEFAULT now(),
  created_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS targets_due ON targets (next_check_at);
