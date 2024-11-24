  SELECT
    count(*)
      FROM supplier
    JOIN orders ON s_suppkey = o_custkey
    JOIN lineitem ON o_orderkey = l_orderkey;

