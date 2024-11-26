CREATE TABLE IF NOT EXISTS movies (
    year integer,
    title varchar,
    description varchar,
    primary key (year, title)
);
