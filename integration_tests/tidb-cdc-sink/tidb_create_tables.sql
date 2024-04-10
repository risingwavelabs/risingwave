CREATE TABLE `tweet` (
  `id` varchar(20) NOT NULL,
  `text` varchar(1024) DEFAULT NULL,
  `lang` varchar(20) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `author_id` varchar(20) NOT NULL,
  PRIMARY KEY (`id`)
);

create table user (
    `id` varchar(20) NOT NULL,
    `name` varchar(100) DEFAULT NULL,
    `username` varchar(100) DEFAULT NULL,
    `followers` bigint not null,
    `created_at` timestamp NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
);

create table hot_hashtags (
    `window_start` timestamp not null,
    `hashtag` varchar(100) not null,
    `hashtag_occurrences` bigint not null,
    PRIMARY KEY (window_start, hashtag)
);

create table datatype (
    `id`            int(11) NOT NULL AUTO_INCREMENT,
    `c0_boolean`    boolean NOT NULL,
    `c1_tinyint`    tinyint(4) DEFAULT NULL,
    `c2_smallint`   smallint(6) DEFAULT NULL,
    `c3_mediumint`  mediumint(9) DEFAULT NULL,
    `c4_int`        int(11) DEFAULT NULL,
    `c5_bigint`     bigint(20) DEFAULT NULL,
    `c6_float`      float(5,3) DEFAULT NULL,
    `c7_double`     double(10,5) DEFAULT NULL,
    `c8_decimal`    decimal(16,8) DEFAULT NULL,
    `c9_date`       date DEFAULT NULL,
    `c10_datetime`  datetime DEFAULT NULL,
    `c11_time`      time DEFAULT NULL,
    `c12_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `c13_char`      char(10) DEFAULT NULL,
    `c14_varchar`   varchar(50) DEFAULT NULL,
    `c15_binary`    binary(10) DEFAULT NULL,
    `c16_varbinary` varbinary(10) DEFAULT NULL,
    `c17_blob`      blob,
    `c18_text`      text,
    `c19_json`      json,
    PRIMARY KEY (`id`)
);

CREATE TABLE tidb_sink_datatypes (
  id integer PRIMARY KEY,
  c_boolean boolean,
  c_tinyint tinyint,
  c_smallint smallint,
  c_mediumint mediumint,
  c_integer integer,
  c_bigint bigint,
  c_decimal decimal,
  c_float float,
  c_double double,
  c_char_255 char(255),
  c_varchar_10000 varchar(10000),
  c_text text,
  c_blob BLOB,
  c_binary_255 binary(255),
  c_varbinary_10000 varbinary(10000),
  c_date date,
  c_time time(6),
  c_datetime datetime(6),
  c_timestamp timestamp(6),
  c_json JSON,
  c_smallint_array LONGTEXT,
  c_integer_array LONGTEXT,
  c_bigint_array LONGTEXT,
  c_real_array LONGTEXT,
  c_double_precision_array LONGTEXT,
  c_varchar_array LONGTEXT
);

CREATE TABLE tidb_cdc_types (
  id integer PRIMARY KEY,
  c_boolean boolean,
  c_bit bit,
  c_tinyint tinyint,
  c_smallint smallint,
  c_mediumint mediumint,
  c_integer integer,
  c_bigint bigint,
  c_decimal decimal,
  c_float float,
  c_double double,
  c_char_255 char(255),
  c_varchar_10000 varchar(10000),
  c_binary_255 binary(255),
  c_varbinary_10000 varbinary(10000),
  c_date date,
  c_time time(6),
  c_datetime datetime(6),
  c_timestamp timestamp(6),
  c_json JSON
);
