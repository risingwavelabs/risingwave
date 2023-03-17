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


create table (
    `window_start` timestamp not null,
    `hashtag` varchar(100) not null,
    `hashtag_occurrences` bigint not null,
    PRIMARY KEY (window_start, hashtag)
);
