#!/usr/bin/env ruby
require 'pg'

def get_rw_conn(host: 'localhost', port: 4566, options: '', dbname: 'dev', user: 'root', password: '')
  # The `tty` option is deprecated in modern libpq/pg releases and can fail the connection.
  conn = PG.connect(host: host, port: port, options: options, dbname: dbname, user: user, password: password)
  # https://github.com/risingwavelabs/risingwave/issues/14682
  # conn.type_map_for_results = PG::BasicTypeMapForResults.new(conn)
  conn
end
