CREATE sink bhv_http_sink FROM bhv_mv WITH (
  connector = 'http',
  url = 'http://localhost:8080/endpoint',
  format = 'json',
  type = 'append-only',
  force_append_only='true',
  primary_key = 'user_id',
  gid.connector.http.sink.header.Origin = '*',
  "gid.connector.http.sink.header.X-Content-Type-Options" = 'nosniff',
  "gid.connector.http.sink.header.Content-Type" = 'application/json'
);