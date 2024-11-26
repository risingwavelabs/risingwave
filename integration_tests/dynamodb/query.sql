INSERT INTO
    movies (year, title, description)
VALUES
    (2020, 'The Emoji Movie', 'a'),
    (2019, 'Avengers: Endgame', 'b'),
    (2018, 'Black Panther', 'c'),
    (2017, 'Wonder Woman', 'd');

INSERT INTO
    movies (year, title, description)
VALUES
    (2023, 'Beautiful beauty', 'ABC DUMMY');

FLUSH;

DELETE FROM
    movies
WHERE
    title = 'Beautiful beauty';

DELETE FROM
    movies
WHERE
    year = 2017;

FLUSH;

INSERT INTO
    movies (year, title, description)
VALUES
    (2017, 'Beautiful beauty', 'ABC DUMMY');

FLUSH;

UPDATE
    movies
SET
    description = 'ABC'
WHERE
    year = 2017;

FLUSH;
