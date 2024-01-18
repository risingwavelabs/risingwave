DELETE FROM upsert_user_behaviors WHERE user_id = 2;

UPDATE upsert_user_behaviors SET target_id = 30 WHERE user_id = 3;

FLUSH;
