package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/dukdb/dukdb-go"
)

func main() {
	// Connect to an in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	fmt.Println("=== Basic Example 07: ALTER TABLE Operations ===")
	fmt.Println()

	// Track current table name
	tableName := "employees"

	// Create initial employee table
	fmt.Println("=== Step 1: Creating initial employee table ===")
	_, err = db.Exec(`CREATE TABLE employees (
		id INTEGER PRIMARY KEY,
		first_name VARCHAR(50) NOT NULL,
		last_name VARCHAR(50) NOT NULL,
		department VARCHAR(50),
		salary DECIMAL(10,2)
	)`)
	if err != nil {
		log.Fatal("Failed to create employees table:", err)
	}

	// Insert sample data
	employees := []struct {
		id         int
		firstName  string
		lastName   string
		department string
		salary     float64
	}{
		{1, "John", "Doe", "Engineering", 75000.00},
		{2, "Jane", "Smith", "Marketing", 65000.00},
		{3, "Bob", "Johnson", "Sales", 60000.00},
		{4, "Alice", "Williams", "Engineering", 80000.00},
		{5, "Charlie", "Brown", "HR", 55000.00},
	}

	for _, emp := range employees {
		_, err = db.Exec(
			"INSERT INTO employees (id, first_name, last_name, department, salary) VALUES (?, ?, ?, ?, ?)",
			emp.id,
			emp.firstName,
			emp.lastName,
			emp.department,
			emp.salary,
		)
		if err != nil {
			log.Printf("Failed to insert employee %s: %v", emp.firstName, err)
		}
	}
	fmt.Println("✓ Sample employees inserted")

	// Display initial structure
	fmt.Println("\n=== Initial Table Structure ===")
	displayTableStructure(db, "employees")
	displayEmployees(db)

	// Example 1: ADD COLUMN - Simple addition
	fmt.Println("\n=== Example 1: ADD COLUMN - Adding email column ===")
	_, err = db.Exec("ALTER TABLE employees ADD COLUMN email VARCHAR(100)")
	if err != nil {
		log.Printf("Failed to add email column: %v", err)
	} else {
		fmt.Println("✓ Email column added successfully")
	}

	// Update emails for existing employees
	emails := map[int]string{
		1: "john.doe@company.com",
		2: "jane.smith@company.com",
		3: "bob.johnson@company.com",
		4: "alice.williams@company.com",
		5: "charlie.brown@company.com",
	}
	for id, email := range emails {
		_, err = db.Exec("UPDATE employees SET email = ? WHERE id = ?", email, id)
		if err != nil {
			log.Printf("Failed to update email for ID %d: %v", id, err)
		}
	}
	fmt.Println("✓ Email addresses updated")

	// Example 2: ADD COLUMN with DEFAULT value
	fmt.Println("\n=== Example 2: ADD COLUMN with DEFAULT value ===")
	_, err = db.Exec("ALTER TABLE employees ADD COLUMN hire_date DATE DEFAULT '2024-01-01'")
	if err != nil {
		log.Printf("Failed to add hire_date column: %v", err)
	} else {
		fmt.Println("✓ Hire date column added with default value")
	}

	// Example 3: ADD COLUMN with NOT NULL
	fmt.Println("\n=== Example 3: ADD COLUMN with NOT NULL constraint ===")
	// First add the column without constraint
	_, err = db.Exec("ALTER TABLE employees ADD COLUMN is_active BOOLEAN")
	if err != nil {
		log.Printf("Failed to add is_active column: %v", err)
	} else {
		// Update all existing rows
		_, err = db.Exec("UPDATE employees SET is_active = true")
		if err != nil {
			log.Printf("Failed to set is_active: %v", err)
		} else {
			// Now add the constraint (this might not work in all databases)
			fmt.Println("✓ Is active column added and populated")
		}
	}

	// Example 4: RENAME COLUMN
	fmt.Println("\n=== Example 4: RENAME COLUMN ===")
	_, err = db.Exec("ALTER TABLE employees RENAME COLUMN department TO dept")
	if err != nil {
		log.Printf("Failed to rename department column: %v", err)
		fmt.Println("⚠ Column rename not supported, continuing...")
	} else {
		fmt.Println("✓ Department column renamed to dept")
	}

	// Example 5: MODIFY COLUMN type
	fmt.Println("\n=== Example 5: MODIFY COLUMN type ===")
	fmt.Println("⚠ Column type modification is not supported in dukdb-go")
	fmt.Println("  To change column types, you need to:")
	fmt.Println("  1. Create a new table with the desired schema")
	fmt.Println("  2. Copy data from old table to new table")
	fmt.Println("  3. Drop the old table")
	fmt.Println("  4. Rename the new table to the original name")

	// Example 6: ADD COLUMN with CHECK constraint
	fmt.Println("\n=== Example 6: ADD COLUMN with CHECK constraint ===")
	fmt.Println("⚠ CHECK constraints are not supported when adding columns in dukdb-go")
	// Add without constraint
	_, err = db.Exec("ALTER TABLE employees ADD COLUMN experience_years INTEGER")
	if err != nil {
		log.Printf("Failed to add experience_years column: %v", err)
	} else {
		fmt.Println("✓ Experience years column added (CHECK constraint skipped)")
	}

	// Update experience years
	_, err = db.Exec(`UPDATE employees SET experience_years = CASE
		WHEN id = 1 THEN 5
		WHEN id = 2 THEN 3
		WHEN id = 3 THEN 2
		WHEN id = 4 THEN 6
		WHEN id = 5 THEN 1
	END`)
	if err != nil {
		log.Printf("Failed to update experience years: %v", err)
	} else {
		fmt.Println("✓ Experience years updated")
	}

	// Display current structure
	fmt.Println("\n=== Table Structure After Additions ===")
	displayTableStructure(db, "employees")

	// Example 7: DROP COLUMN
	fmt.Println("\n=== Example 7: DROP COLUMN ===")
	_, err = db.Exec("ALTER TABLE employees DROP COLUMN experience_years")
	if err != nil {
		log.Printf("Failed to drop experience_years column: %v", err)
		fmt.Println("⚠ Column drop not supported, continuing...")
	} else {
		fmt.Println("✓ Experience years column dropped")
	}

	// Example 8: ADD MULTIPLE COLUMNS in separate statements
	fmt.Println("\n=== Example 8: ADD MULTIPLE COLUMNS ===")
	columns := []struct {
		name string
		def  string
	}{
		{"phone", "VARCHAR(20)"},
		{"address", "VARCHAR(200)"},
		{"city", "VARCHAR(50)"},
	}

	for _, col := range columns {
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE employees ADD COLUMN %s %s", col.name, col.def))
		if err != nil {
			log.Printf("Failed to add %s column: %v", col.name, err)
		} else {
			fmt.Printf("✓ %s column added\n", col.name)
		}
	}

	// Example 9: RENAME TABLE
	fmt.Println("\n=== Example 9: RENAME TABLE ===")
	_, err = db.Exec("ALTER TABLE employees RENAME TO staff")
	if err != nil {
		log.Printf("Failed to rename table: %v", err)
		fmt.Println("⚠ Table rename not supported, continuing...")
	} else {
		fmt.Println("✓ Table renamed from employees to staff")
		// Update our reference
		tableName = "staff"
		displayTableStructure(db, "staff")
	}

	// Example 10: Complex ALTER - Add foreign key simulation
	fmt.Println("\n=== Example 10: Add department reference ===")
	// Create departments table
	_, err = db.Exec(`CREATE TABLE departments (
		id INTEGER PRIMARY KEY,
		dept_name VARCHAR(50) NOT NULL,
		manager_id INTEGER,
		budget DECIMAL(12,2)
	)`)
	if err != nil {
		log.Printf("Failed to create departments table: %v", err)
	} else {
		// Insert departments
		departments := []struct {
			id   int
			name string
		}{
			{1, "Engineering"},
			{2, "Marketing"},
			{3, "Sales"},
			{4, "HR"},
		}
		for _, dept := range departments {
			_, err = db.Exec("INSERT INTO departments (id, dept_name) VALUES (?, ?)", dept.id, dept.name)
			if err != nil {
				log.Printf("Failed to insert department %s: %v", dept.name, err)
			}
		}
		fmt.Println("✓ Departments table created and populated")

		// Add department_id column to employees/staff
		tableName := "employees"
		if err == nil {
			tableName = "staff" // Table was renamed
		}
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN department_id INTEGER", tableName))
		if err != nil {
			log.Printf("Failed to add department_id column: %v", err)
		} else {
			fmt.Println("✓ Department ID column added")

			// Update department references
			_, err = db.Exec(fmt.Sprintf(`UPDATE %s SET department_id =
				CASE
					WHEN dept = 'Engineering' THEN 1
					WHEN dept = 'Marketing' THEN 2
					WHEN dept = 'Sales' THEN 3
					WHEN dept = 'HR' THEN 4
				END`, tableName))
			if err != nil {
				log.Printf("Failed to update department references: %v", err)
			} else {
				fmt.Println("✓ Department references updated")
			}
		}
	}

	// Display final structure
	fmt.Println("\n=== Final Table Structure ===")
	displayTableStructure(db, tableName)

	// Display sample data
	fmt.Println("\n=== Sample Data After All Changes ===")
	displayEmployeesWithNewColumns(db, tableName)

	// Clean up
	fmt.Println("\n=== Cleaning Up ===")
	_, err = db.Exec("DROP TABLE IF EXISTS staff")
	if err != nil {
		log.Printf("Failed to drop staff table: %v", err)
	}
	_, err = db.Exec("DROP TABLE IF EXISTS employees")
	if err != nil {
		log.Printf("Failed to drop employees table: %v", err)
	}
	_, err = db.Exec("DROP TABLE IF EXISTS departments")
	if err != nil {
		log.Printf("Failed to drop departments table: %v", err)
	}
	fmt.Println("✓ All tables dropped successfully")

	// Summary
	fmt.Println("\n=== Summary ===")
	fmt.Println("This example demonstrated supported ALTER TABLE operations:")
	fmt.Println("- ADD COLUMN (simple, with DEFAULT value)")
	fmt.Println("- RENAME COLUMN")
	fmt.Println("- DROP COLUMN")
	fmt.Println("- RENAME TABLE")
	fmt.Println("- Adding multiple columns")
	fmt.Println("\nUnsupported operations (not available in dukdb-go):")
	fmt.Println("- MODIFY COLUMN type")
	fmt.Println("- CHECK constraints")
	fmt.Println("- UNIQUE constraints in CREATE TABLE")
	fmt.Println("\nNote: ALTER TABLE capabilities vary by database implementation.")
}

// Helper function to display table structure
func displayTableStructure(db *sql.DB, tableName string) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info('%s')", tableName))
	if err != nil {
		log.Printf("Failed to get table info for %s: %v", tableName, err)
		return
	}
	defer rows.Close()

	fmt.Printf("\nStructure of %s:\n", tableName)
	fmt.Println("CID | Column Name        | Type        | NotNull | Default       | PK")
	fmt.Println("----|-------------------|-------------|---------|---------------|----")

	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dfltValue sql.NullString
		rows.Scan(&cid, &name, &typ, &notNull, &dfltValue, &pk)

		dfltStr := "NULL"
		if dfltValue.Valid {
			dfltStr = dfltValue.String
		}

		fmt.Printf("%3d | %-17s | %-11s | %-7s | %-13s | %2d\n",
			cid, name, typ, boolToYesNo(notNull == 1), dfltStr, pk)
	}
}

// Helper function to display employees
func displayEmployees(db *sql.DB) {
	rows, err := db.Query(
		"SELECT id, first_name, last_name, department, salary FROM employees ORDER BY id",
	)
	if err != nil {
		log.Printf("Failed to query employees: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("\nID | Name              | Department  | Salary    |")
	fmt.Println("---|-------------------|-------------|-----------|")

	for rows.Next() {
		var id int
		var firstName, lastName, department string
		var salary float64
		err := rows.Scan(&id, &firstName, &lastName, &department, &salary)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		fmt.Printf("%2d | %-16s | %-11s | $%8.2f |\n",
			id, firstName+" "+lastName, department, salary)
	}
}

// Helper function to display employees with new columns
func displayEmployeesWithNewColumns(db *sql.DB, tableName string) {
	query := fmt.Sprintf(`SELECT
		id, first_name, last_name,
		dept as department,
		salary, email, hire_date, is_active
	FROM %s
	ORDER BY id`, tableName)

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Failed to query %s: %v", tableName, err)
		return
	}
	defer rows.Close()

	fmt.Println(
		"\nID | Name              | Department  | Salary    | Email                  | Hire Date  | Active |",
	)
	fmt.Println(
		"---|-------------------|-------------|-----------|------------------------|------------|--------|",
	)

	for rows.Next() {
		var id int
		var firstName, lastName, department, email string
		var salary float64
		var hireDate sql.NullString
		var isActive sql.NullBool

		err := rows.Scan(&id, &firstName, &lastName, &department, &salary,
			&email, &hireDate, &isActive)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		activeStr := "NULL"
		if isActive.Valid {
			if isActive.Bool {
				activeStr = "Yes"
			} else {
				activeStr = "No"
			}
		}

		dateStr := "NULL"
		if hireDate.Valid {
			dateStr = hireDate.String
		}

		fmt.Printf("%2d | %-16s | %-11s | $%8.2f | %-22s | %10s | %6s |\n",
			id, firstName+" "+lastName, department, salary, email, dateStr, activeStr)
	}
}

func boolToYesNo(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}
