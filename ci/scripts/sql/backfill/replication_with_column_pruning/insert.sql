insert into t1 select generate_series + 1, generate_series + 2, generate_series + 3 from generate_series(1, 1000000);
flush;
