CREATE SOURCE source (id int, name varchar)
WITH (
     connector = 'datagen',
     fields.id.kind = 'sequence',
     fields.id.start = '1',
     fields.id.end = '100',
     datagen.rows.per.second = '100'
 ) ROW FORMAT JSON;