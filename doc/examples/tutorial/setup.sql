CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    department_id INTEGER REFERENCES departments(id),
    name TEXT NOT NULL
);
INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Support');
INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 2, 'Boris'), (3, NULL, 'Clara');
