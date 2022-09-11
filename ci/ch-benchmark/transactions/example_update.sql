-- Example of an update transaction
BEGIN TRANSACTION;

UPDATE customer SET c_first = 'RisingWave' WHERE c_id = 1;
UPDATE stock SET st_quantity = 42 WHERE st_i_id = 1;

END TRANSACTION;
