CREATE ROLE admin LOGIN PASSWORD 'admin';

GRANT pg_read_all_data TO admin;
ALTER ROLE admin BYPASSRLS;

CREATE SCHEMA users;

GRANT ALL ON SCHEMA users TO admin;

CREATE TABLE users.department (
    department_id serial PRIMARY KEY,
    code text NOT NULL UNIQUE,
    name text NOT NULL
);

CREATE TABLE users.end_user (
    user_id serial PRIMARY KEY,
    username text NOT NULL UNIQUE,
    full_name text,
    department_id integer REFERENCES users.department(department_id),
    is_active boolean NOT NULL DEFAULT true
);

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA users TO admin;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA users TO admin;

INSERT INTO users.department (code, name)
VALUES
    ('sales', 'Sales'),
    ('eng', 'Engineering');

INSERT INTO users.end_user (
    username,
    full_name,
    department_id
)
SELECT
    'alice',
    'Alice Example',
    department_id
FROM users.department
WHERE code = 'sales';

INSERT INTO users.end_user (
    username,
    full_name,
    department_id
)
SELECT
    'bob',
    'Bob Example',
    department_id
FROM users.department
WHERE code = 'eng';
