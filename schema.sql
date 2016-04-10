create table events (
  id text primary key,
  type text not null,
  created_at timestamp not null,
  received_at timestamp not null,
  payload json not null
);

create index events_received_at_type
on events (received_at, type);
