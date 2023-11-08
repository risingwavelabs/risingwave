--
-- VARCHAR
--

CREATE TABLE VARCHAR_TBL_TMP(f1 varchar);

INSERT INTO VARCHAR_TBL_TMP (f1) VALUES ('a');

INSERT INTO VARCHAR_TBL_TMP (f1) VALUES ('A');

-- any of the following three input formats are acceptable
INSERT INTO VARCHAR_TBL_TMP (f1) VALUES ('1');

INSERT INTO VARCHAR_TBL_TMP (f1) VALUES (2);

INSERT INTO VARCHAR_TBL_TMP (f1) VALUES ('3');

-- zero-length char
INSERT INTO VARCHAR_TBL_TMP (f1) VALUES ('');

-- try varchar's of greater than 1 length
INSERT INTO VARCHAR_TBL_TMP (f1) VALUES ('cd');
INSERT INTO VARCHAR_TBL_TMP (f1) VALUES ('c     ');


SELECT * FROM VARCHAR_TBL_TMP;

SELECT c.*
   FROM VARCHAR_TBL_TMP c
   WHERE c.f1 <> 'a';

SELECT c.*
   FROM VARCHAR_TBL_TMP c
   WHERE c.f1 = 'a';

SELECT c.*
   FROM VARCHAR_TBL_TMP c
   WHERE c.f1 < 'a';

SELECT c.*
   FROM VARCHAR_TBL_TMP c
   WHERE c.f1 <= 'a';

SELECT c.*
   FROM VARCHAR_TBL_TMP c
   WHERE c.f1 > 'a';

SELECT c.*
   FROM VARCHAR_TBL_TMP c
   WHERE c.f1 >= 'a';

DROP TABLE VARCHAR_TBL_TMP;

--
-- Now test longer arrays of char
--
-- This varchar_tbl was already created and filled in test_setup.sql.
--

SELECT * FROM VARCHAR_TBL;
