CREATE TABLE IF NOT EXISTS adoption_center_1
(date DATE, type VARCHAR, name VARCHAR, age INTEGER);

CREATE TABLE IF NOT EXISTS adoption_center_2
(date DATE, type VARCHAR, name VARCHAR, age INTEGER);

INSERT INTO
    adoption_center_1 (date, type, name, age)
VALUES
    ('2022-01-01', 'Dog', 'Bingo', 4),
    ('2022-02-02', 'Cat', 'Bob', 7),
    ('2022-03-04', 'Fish', 'Bubbles', 2);

INSERT INTO
    adoption_center_2 (date, type, name, age)
VALUES
    ('2022-06-10', 'Horse', 'Seabiscuit', 4),
    ('2022-07-15', 'Snake', 'Stripes', 8),
    ('2022-08-07', 'Rabbit', 'Hops', 3);