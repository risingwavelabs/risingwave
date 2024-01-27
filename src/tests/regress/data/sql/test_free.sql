--
-- TEST_FREE --- drops the tables created during setup that other tests may use
--

--@ DROP TABLE CHAR_TBL

DROP TABLE FLOAT8_TBL;

DROP TABLE INT2_TBL;
DROP TABLE INT4_TBL;
DROP TABLE INT8_TBL;

--@ DROP TABLE POINT_TBL;

DROP TABLE TEXT_TBL;

DROP TABLE VARCHAR_TBL;

DROP TABLE onek;
DROP TABLE onek2;

DROP TABLE tenk1;
DROP TABLE tenk2;

--@ DROP TABLE person;
--@ DROP TABLE emp;
--@ DROP TABLE student;
--@ DROP TABLE stud_emp;
--@
--@ DROP TABLE road;
--@ DROP TABLE ihighway;
--@ DROP TABLE shighway;
