DROP DATABASE IF EXISTS `my@db`;
CREATE DATABASE `my@db`;

USE `my@db`;

CREATE TABLE products (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(512)
);

ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products VALUES (default,"101","101"),
(default,"102","102"),
(default,"103","103"),
(default,"104","104"),
(default,"105","105"),
(default,"106","106"),
(default,"107","107"),
(default,"108","108")
