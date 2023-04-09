--
-- INT4
--

CREATE TABLE INT4_TBL(f1 int4);

INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
INSERT INTO INT4_TBL(f1) VALUES ('     ');
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
INSERT INTO INT4_TBL(f1) VALUES ('');


SELECT * FROM INT4_TBL;

SELECT i.* FROM INT4_TBL i WHERE i.f1 <> smallint '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 = smallint '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 = int '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 < smallint '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 < int '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 <= smallint '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 <= int '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 > smallint '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 > int '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 >= smallint '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 >= int '0';

-- positive odds
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % smallint '2') = smallint '1';

-- any evens
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % int '2') = smallint '0';

SELECT i.f1, i.f1 * smallint '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 * smallint '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT i.f1, i.f1 * int '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 * int '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT i.f1, i.f1 + smallint '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 + smallint '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT i.f1, i.f1 + int '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 + int '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT i.f1, i.f1 - smallint '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 - smallint '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT i.f1, i.f1 - int '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 - int '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT i.f1, i.f1 / smallint '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 / int '2' AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
SELECT -2+3 AS one;

SELECT 4-2 AS two;

SELECT 2- -1 AS three;

SELECT 2 - -2 AS four;

SELECT smallint '2' * smallint '2' = smallint '16' / smallint '4' AS true;

SELECT int '2' * smallint '2' = smallint '16' / int '4' AS true;

SELECT smallint '2' * int '2' = int '16' / smallint '4' AS true;

SELECT int '1000' < int '999' AS false;

SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;

SELECT 2 + 2 / 2 AS three;

SELECT (2 + 2) / 2 AS two;

-- corner case
SELECT (-1::int4<<31)::text AS text;
SELECT ((-1::int4<<31)+1)::text AS text;

-- check sane handling of INT_MIN overflow cases
SELECT (-2147483648)::int4 * (-1)::int4;
SELECT (-2147483648)::int4 / (-1)::int4;
--@ SELECT (-2147483648)::int4 % (-1)::int4;
SELECT (-2147483648)::int4 * (-1)::int2;
SELECT (-2147483648)::int4 / (-1)::int2;
--@ SELECT (-2147483648)::int4 % (-1)::int2;

-- check rounding when casting from float
--@ SELECT x, x::int4 AS int4_value
--@ FROM (VALUES (-2.5::float8),
--@              (-1.5::float8),
--@              (-0.5::float8),
--@              (0.0::float8),
--@              (0.5::float8),
--@              (1.5::float8),
--@              (2.5::float8)) t(x);

-- check rounding when casting from numeric
SELECT x, x::int4 AS int4_value
FROM (VALUES (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric)) t(x);

-- test gcd()
--@ SELECT a, b, gcd(a, b), gcd(a, -b), gcd(b, a), gcd(-b, a)
--@ FROM (VALUES (0::int4, 0::int4),
--@              (0::int4, 6410818::int4),
--@              (61866666::int4, 6410818::int4),
--@              (-61866666::int4, 6410818::int4),
--@              ((-2147483648)::int4, 1::int4),
--@              ((-2147483648)::int4, 2147483647::int4),
--@              ((-2147483648)::int4, 1073741824::int4)) AS v(a, b);

SELECT gcd((-2147483648)::int4, 0::int4); -- overflow
SELECT gcd((-2147483648)::int4, (-2147483648)::int4); -- overflow

-- test lcm()
--@ SELECT a, b, lcm(a, b), lcm(a, -b), lcm(b, a), lcm(-b, a)
--@ FROM (VALUES (0::int4, 0::int4),
--@              (0::int4, 42::int4),
--@              (42::int4, 42::int4),
--@              (330::int4, 462::int4),
--@              (-330::int4, 462::int4),
--@              ((-2147483648)::int4, 0::int4)) AS v(a, b);

SELECT lcm((-2147483648)::int4, 1::int4); -- overflow
SELECT lcm(2147483647::int4, 2147483646::int4); -- overflow

DROP TABLE INT4_TBL;
