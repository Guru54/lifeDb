CREATE TABLE emp (id INT PRIMARY KEY, name TEXT);
INSERT INTO emp (id, name) VALUES (1, 'Alice');
INSERT INTO emp (id, name) VALUES (2, 'Bob');
INSERT INTO emp (id, name) VALUES (3, 'Carol');
SELECT * FROM emp;
DELETE FROM emp WHERE id = 2;
SELECT * FROM emp;
UPDATE emp SET name = 'Alicia' WHERE id = 1;
SELECT * FROM emp;
.exit
