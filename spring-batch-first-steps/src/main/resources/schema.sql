CREATE TABLE persons
(
    id IDENTITY PRIMARY KEY,
    first_name VARCHAR(255),
    last_name  VARCHAR(255)
);

CREATE TABLE persons_backup (
     id IDENTITY PRIMARY KEY,
     first_name VARCHAR(255),
     last_name VARCHAR(255)
);