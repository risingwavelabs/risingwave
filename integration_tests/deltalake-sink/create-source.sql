CREATE SOURCE source (id int, name varchar)
WITH (
     connector = 'datagen',
     fields.id.kind = 'sequence',
     fields.id.start = '1',
     fields.id.end = '10000',
     fields.name.kind = 'random',
     fields.name.length = '10',
     datagen.rows.per.second = '200'
 ) ROW FORMAT JSON;