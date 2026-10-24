-- overflow: mixed-version review
-- Stop old workers before enabling sponsor-scoped admission: they charge the
-- retained singleton instead. Keeping that table permits an operator rollback.
create table reconciliation_sponsor_sweep_rounds (
  sponsor_id uuid primary key references users(id) on delete cascade,
  window_start timestamptz not null check (isfinite(window_start)),
  spent integer not null default 0 check (spent >= 0),
  rotation_cursor text,
  next_enqueue_at timestamptz not null check (isfinite(next_enqueue_at))
);

create table repository_reconciliation_sweep_policy (
  repository_id uuid primary key references registered_repositories(id) on delete cascade,
  history_size bigint not null default 0 check (history_size between 0 and 9007199254740991),
  measured_at timestamptz check (measured_at is null or isfinite(measured_at)),
  next_measure_at timestamptz not null default now() check (isfinite(next_measure_at)),
  catch_up_state text check (catch_up_state in ('PENDING', 'IN_FLIGHT', 'COMPLETED', 'FAILED')),
  catch_up_revision integer,
  catch_up_format integer
);
