-- Users / organization master data. Demo database used to prove RLS works
-- end-to-end (users -> core -> mart).

CREATE SCHEMA IF NOT EXISTS users AUTHORIZATION CURRENT_USER;

CREATE TABLE IF NOT EXISTS users.department (
    department_id   SERIAL PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users.end_user (
    user_id         SERIAL PRIMARY KEY,
    username        TEXT NOT NULL UNIQUE,
    full_name       TEXT NOT NULL,
    department_id   INTEGER REFERENCES users.department(department_id),
    is_active       BOOLEAN NOT NULL DEFAULT true
);

-- ---------------------------------------------------------------------
-- Seed data: 5 departments, 10 fictional end users
-- ---------------------------------------------------------------------
INSERT INTO users.department (code, name) VALUES
    ('SALES', 'Sales'),
    ('ENG',   'Engineering'),
    ('FIN',   'Finance'),
    ('HR',    'Human Resources'),
    ('OPS',   'Operations')
ON CONFLICT (code) DO NOTHING;

INSERT INTO users.end_user (username, full_name, department_id, is_active) VALUES
    ('asalo',    'Aino Salo',      (SELECT department_id FROM users.department WHERE code = 'SALES'), true),
    ('mvirta',   'Mikko Virtanen', (SELECT department_id FROM users.department WHERE code = 'ENG'),   true),
    ('lkoski',   'Liisa Koskinen', (SELECT department_id FROM users.department WHERE code = 'ENG'),   true),
    ('jheikki',  'Juha Heikkinen', (SELECT department_id FROM users.department WHERE code = 'FIN'),   true),
    ('sniemi',   'Sanna Niemi',    (SELECT department_id FROM users.department WHERE code = 'HR'),    true),
    ('tlahti',   'Timo Lahtinen',  (SELECT department_id FROM users.department WHERE code = 'OPS'),   true),
    ('kmaki',    'Kaisa Mäkinen',  (SELECT department_id FROM users.department WHERE code = 'SALES'), false),
    ('pkorho',   'Pekka Korhonen', (SELECT department_id FROM users.department WHERE code = 'ENG'),   true),
    ('hlaine',   'Hanna Laine',    (SELECT department_id FROM users.department WHERE code = 'FIN'),   true),
    ('avirt',    'Antti Virtala',  (SELECT department_id FROM users.department WHERE code = 'OPS'),   false)
ON CONFLICT (username) DO NOTHING;

-- ---------------------------------------------------------------------
-- Grants: read-only across the schema
-- ---------------------------------------------------------------------
GRANT USAGE ON SCHEMA users TO users_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA users TO users_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA users GRANT SELECT ON TABLES TO users_reader;

GRANT ALL PRIVILEGES ON SCHEMA users TO CURRENT_USER;
