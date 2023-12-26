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