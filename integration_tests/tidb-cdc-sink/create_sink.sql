CREATE SINK hot_hashtags_sink FROM hot_hashtags
WITH (
   connector='jdbc',
   jdbc.url='jdbc:mysql://tidb:4000/test',
   user='root',
   password='',
   table.name='hot_hashtags',
   type='upsert',
   primary_key='window_start,hashtag'
);

CREATE SINK tidb_sink_datatypes_sink FROM tidb_sink_datatypes
WITH (
   connector='jdbc',
   jdbc.url='jdbc:mysql://tidb:4000/test',
   user='root',
   password='',
   table.name='tidb_sink_datatypes',
   type='upsert',
   primary_key='id'
);
