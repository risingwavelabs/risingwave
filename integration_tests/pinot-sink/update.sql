update orders
set status = 'PROCESSING',
    updated_at = updated_at + 1000
where id = 1;
FLUSH;
