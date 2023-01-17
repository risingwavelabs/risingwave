
-- PG
CREATE TABLE IF NOT EXISTS new_order (
         no_o_id INT NOT NULL,
         no_d_id INT NOT NULL,
         no_w_id INT NOT NULL,
         PRIMARY KEY(no_w_id, no_d_id, no_o_id)
);
