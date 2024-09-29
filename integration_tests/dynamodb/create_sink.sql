set sink_decouple = false;

CREATE SINK dyn_sink
FROM
  movies
WITH
(
  connector = 'dynamodb',
  table = 'Movies',
  primary_key = 'year,title',
  endpoint = 'http://dynamodb:8000',
  region = 'us',
  access_key = 'ac',
  secret_key = 'sk'
);
