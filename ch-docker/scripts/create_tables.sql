CREATE TABLE IF NOT EXISTS customer (
        c_id INTEGER,
        c_d_id INTEGER,
        c_w_id INTEGER,
        c_first VARCHAR(16),
        c_middle VARCHAR(2),
        c_last VARCHAR(16),
        c_street_1 VARCHAR(20),
        c_street_2 VARCHAR(20),
        c_city VARCHAR(20),
        c_state VARCHAR(2),
        c_zip VARCHAR(9),
        c_phone VARCHAR(16),
        c_since TEXT,
        c_credit VARCHAR(2),
        c_credit_lim REAL,
        c_discount REAL,
        c_balance REAL,
        c_ytd_payment REAL,
        c_payment_cnt INTEGER,
        c_delivery_cnt INTEGER,
        c_data1 VARCHAR(250),
        c_data2 VARCHAR(250),
        PRIMARY KEY (c_id, c_d_id, c_w_id)
);

CREATE TABLE IF NOT EXISTS district (
        d_id INTEGER,
        d_w_id INTEGER,
        d_name VARCHAR(10),
        d_street_1 VARCHAR(20),
        d_street_2 VARCHAR(20),
        d_city VARCHAR(20),
        d_state VARCHAR(2),
        d_zip VARCHAR(9),
        d_tax REAL,
        d_ytd REAL,
        d_next_o_id INTEGER,
        PRIMARY KEY (d_id, d_w_id)
);

CREATE TABLE IF NOT EXISTS history (
        h_c_id INTEGER,
        h_c_d_id INTEGER,
        h_c_w_id INTEGER,
        h_d_id INTEGER,
        h_w_id INTEGER,
        h_date TEXT,
        h_amount REAL,
        h_data VARCHAR(24)
);

CREATE TABLE IF NOT EXISTS item (
        i_id INTEGER,
        i_im_id INTEGER,
        i_name VARCHAR(24),
        i_price REAL,
        i_data VARCHAR(50),
        PRIMARY KEY (i_id)
);

CREATE TABLE IF NOT EXISTS neworder (
        no_o_id INTEGER,
        no_d_id INTEGER,
        no_w_id INTEGER,
        PRIMARY KEY (no_o_id, no_d_id, no_w_id)
);

CREATE TABLE IF NOT EXISTS orders (
        o_id INTEGER,
        o_d_id INTEGER,
        o_w_id INTEGER,
        o_c_id INTEGER,
        o_entry_d TEXT,
        o_carrier_id INTEGER,
        o_ol_cnt INTEGER,
        o_all_local INTEGER,
        PRIMARY KEY (o_id, o_d_id, o_w_id)
);

CREATE TABLE IF NOT EXISTS orderline (
        ol_o_id INTEGER,
        ol_d_id INTEGER,
        ol_w_id INTEGER,
        ol_number INTEGER,
        ol_i_id INTEGER,
        ol_supply_w_id INTEGER,
        ol_delivery_d TEXT,
        ol_quantity INTEGER,
        ol_amount REAL,
        ol_dist_info VARCHAR(24),
        PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number)
);

CREATE TABLE IF NOT EXISTS stock (
        st_i_id INTEGER,
        st_w_id INTEGER,
        st_quantity INTEGER,
        st_dist_01 VARCHAR(24),
        st_dist_02 VARCHAR(24),
        st_dist_03 VARCHAR(24),
        st_dist_04 VARCHAR(24),
        st_dist_05 VARCHAR(24),
        st_dist_06 VARCHAR(24),
        st_dist_07 VARCHAR(24),
        st_dist_08 VARCHAR(24),
        st_dist_09 VARCHAR(24),
        st_dist_10 VARCHAR(24),
        st_ytd INTEGER,
        st_order_cnt INTEGER,
        st_remote_cnt INTEGER,
        st_data VARCHAR(50),
        PRIMARY KEY (st_i_id, st_w_id)
);

CREATE TABLE IF NOT EXISTS warehouse (
        w_id INTEGER,
        w_name VARCHAR(10),
        w_street_1 VARCHAR(20),
        w_street_2 VARCHAR(20),
        w_city VARCHAR(20),
        w_state VARCHAR(2),
        w_zip VARCHAR(9),
        w_tax REAL,
        w_YTD REAL,
        PRIMARY KEY (w_id)
);
