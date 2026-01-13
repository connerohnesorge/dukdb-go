CREATE TABLE departments (
	id INTEGER PRIMARY KEY,
	dept_name VARCHAR(50) UNIQUE NOT NULL,
	manager_id INTEGER,
	budget DECIMAL(12,2)
);
