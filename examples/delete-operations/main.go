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

	// Create a tasks table
	_, err = db.Exec(`CREATE TABLE tasks (
		id INTEGER PRIMARY KEY,
		task_name VARCHAR(100),
		priority VARCHAR(20),
		status VARCHAR(20),
		assigned_to VARCHAR(50),
		created_date DATE,
		due_date DATE,
		completion_percentage INTEGER DEFAULT 0,
		estimated_hours DECIMAL(5,2)
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert sample tasks
	tasks := []struct {
		id         int
		name       string
		priority   string
		status     string
		assigned   string
		created    string
		due        string
		completion int
		hours      float64
	}{
		{
			1,
			"Design Database Schema",
			"High",
			"Completed",
			"Alice",
			"2024-01-01",
			"2024-01-05",
			100,
			8.0,
		},
		{
			2,
			"Implement API Endpoints",
			"High",
			"In Progress",
			"Bob",
			"2024-01-02",
			"2024-01-10",
			60,
			16.0,
		},
		{
			3,
			"Write Unit Tests",
			"Medium",
			"Not Started",
			"Charlie",
			"2024-01-03",
			"2024-01-08",
			0,
			12.0,
		},
		{
			4,
			"Create Documentation",
			"Low",
			"Not Started",
			"David",
			"2024-01-04",
			"2024-01-15",
			0,
			6.0,
		},
		{
			5,
			"Setup CI/CD Pipeline",
			"High",
			"Completed",
			"Alice",
			"2024-01-05",
			"2024-01-07",
			100,
			4.0,
		},
		{6, "Code Review", "Medium", "In Progress", "Bob", "2024-01-06", "2024-01-09", 30, 3.0},
		{
			7,
			"Deploy to Staging",
			"High",
			"Not Started",
			"Charlie",
			"2024-01-10",
			"2024-01-12",
			0,
			2.0,
		},
		{
			8,
			"Performance Testing",
			"Medium",
			"Not Started",
			"David",
			"2024-01-08",
			"2024-01-14",
			0,
			8.0,
		},
		{9, "Security Audit", "High", "In Progress", "Alice", "2024-01-07", "2024-01-11", 45, 10.0},
		{
			10,
			"User Acceptance Testing",
			"Low",
			"Not Started",
			"Bob",
			"2024-01-12",
			"2024-01-18",
			0,
			16.0,
		},
		{11, "Bug Fixes", "High", "Not Started", "Charlie", "2024-01-13", "2024-01-16", 0, 20.0},
		{
			12,
			"Release Preparation",
			"High",
			"Not Started",
			"David",
			"2024-01-15",
			"2024-01-17",
			0,
			4.0,
		},
	}

	for _, task := range tasks {
		_, err = db.Exec(
			`INSERT INTO tasks (id, task_name, priority, status, assigned_to, created_date, due_date, completion_percentage, estimated_hours)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			task.id,
			task.name,
			task.priority,
			task.status,
			task.assigned,
			task.created,
			task.due,
			task.completion,
			task.hours,
		)
		if err != nil {
			log.Printf("Failed to insert task %s: %v", task.name, err)
		}
	}
	fmt.Println("Initial tasks inserted")

	// Display initial tasks
	fmt.Println("\n=== Initial Tasks ===")
	displayTasks(db)

	// Example 1: Simple DELETE with WHERE clause
	fmt.Println("\n=== Example 1: Simple DELETE with WHERE clause ===")
	result, err := db.Exec("DELETE FROM tasks WHERE id = ?", 12)
	if err != nil {
		log.Printf("Failed to delete task: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Release Preparation removed\n", rowsAffected)
	}

	// Example 2: DELETE with multiple conditions (AND)
	fmt.Println("\n=== Example 2: DELETE with multiple conditions (AND) ===")
	result, err = db.Exec(
		"DELETE FROM tasks WHERE priority = ? AND status = ?",
		"Low",
		"Not Started",
	)
	if err != nil {
		log.Printf("Failed to delete low priority tasks: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Low priority, not started tasks removed\n", rowsAffected)
	}

	// Example 3: DELETE with OR conditions
	fmt.Println("\n=== Example 3: DELETE with OR conditions ===")
	result, err = db.Exec(
		"DELETE FROM tasks WHERE estimated_hours < ? OR completion_percentage = ?",
		3.0,
		100,
	)
	if err != nil {
		log.Printf("Failed to delete quick/completed tasks: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Quick tasks (<3 hours) or completed tasks removed\n", rowsAffected)
	}

	// Example 4: DELETE with IN clause
	fmt.Println("\n=== Example 4: DELETE with IN clause ===")
	result, err = db.Exec("DELETE FROM tasks WHERE assigned_to IN (?, ?)", "David", "Charlie")
	if err != nil {
		log.Printf("Failed to delete tasks by assignee: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Tasks assigned to David and Charlie removed\n", rowsAffected)
	}

	// Example 5: DELETE with date conditions
	fmt.Println("\n=== Example 5: DELETE with date conditions ===")
	result, err = db.Exec(
		"DELETE FROM tasks WHERE due_date < ? AND status != ?",
		"2024-01-10",
		"Completed",
	)
	if err != nil {
		log.Printf("Failed to delete overdue tasks: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Overdue tasks (due before Jan 10) removed\n", rowsAffected)
	}

	// Example 6: DELETE with LIKE pattern matching
	fmt.Println("\n=== Example 6: DELETE with LIKE pattern matching ===")
	result, err = db.Exec("DELETE FROM tasks WHERE task_name LIKE ?", "%Testing%")
	if err != nil {
		log.Printf("Failed to delete testing tasks: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Tasks with 'Testing' in name removed\n", rowsAffected)
	}

	// Example 7: DELETE with NOT conditions
	fmt.Println("\n=== Example 7: DELETE with NOT conditions ===")
	result, err = db.Exec(
		"DELETE FROM tasks WHERE priority != ? AND status != ?",
		"High",
		"In Progress",
	)
	if err != nil {
		log.Printf("Failed to delete non-high/non-progress tasks: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Non-high priority, non-in-progress tasks removed\n", rowsAffected)
	}

	// Example 8: DELETE with BETWEEN range
	fmt.Println("\n=== Example 8: DELETE with BETWEEN range ===")
	result, err = db.Exec("DELETE FROM tasks WHERE completion_percentage BETWEEN ? AND ?", 20, 50)
	if err != nil {
		log.Printf("Failed to delete partially complete tasks: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("Deleted %d task(s): Tasks 20-50% complete removed\n", rowsAffected)
	}

	// Example 9: DELETE with subquery (if supported)
	fmt.Println("\n=== Example 9: DELETE with subquery ===")
	// First, get the average estimated hours
	var avgHours float64
	err = db.QueryRow("SELECT AVG(estimated_hours) FROM tasks").Scan(&avgHours)
	if err != nil {
		log.Printf("Failed to calculate average hours: %v", err)
	} else {
		// Delete tasks that take much longer than average
		result, err = db.Exec("DELETE FROM tasks WHERE estimated_hours > ? * 1.5", avgHours)
		if err != nil {
			log.Printf("Failed to delete long tasks: %v", err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			fmt.Printf("Deleted %d task(s): Tasks taking 1.5x average time (%.1f hours) removed\n", rowsAffected, avgHours)
		}
	}

	// Example 10: Conditional DELETE with Go logic
	fmt.Println("\n=== Example 10: Conditional DELETE with Go logic ===")
	// Query tasks to evaluate for deletion
	rows, err := db.Query(
		"SELECT id, task_name, priority, status, completion_percentage FROM tasks",
	)
	if err != nil {
		log.Printf("Failed to query tasks: %v", err)
	} else {
		defer rows.Close()
		deletionCount := 0
		for rows.Next() {
			var id, completion int
			var name, priority, status string
			err := rows.Scan(&id, &name, &priority, &status, &completion)
			if err != nil {
				log.Printf("Failed to scan row: %v", err)
				continue
			}

			// Complex business logic for deletion
			shouldDelete := false
			if priority == "Low" && status == "Not Started" && completion == 0 {
				shouldDelete = true
			} else if status == "In Progress" && completion < 25 {
				shouldDelete = true
			}

			if shouldDelete {
				_, err = db.Exec("DELETE FROM tasks WHERE id = ?", id)
				if err != nil {
					log.Printf("Failed to delete task %d: %v", id, err)
				} else {
					deletionCount++
					fmt.Printf("Deleted task: %s (Priority: %s, Status: %s, Completion: %d%%)\n",
						name, priority, status, completion)
				}
			}
		}
		fmt.Printf("Total tasks deleted with conditional logic: %d\n", deletionCount)
	}

	// Display remaining tasks
	fmt.Println("\n=== Remaining Tasks After Deletions ===")
	displayTasks(db)

	// Show deletion summary
	fmt.Println("\n=== Deletion Summary ===")
	var remainingTasks int
	err = db.QueryRow("SELECT COUNT(*) FROM tasks").Scan(&remainingTasks)
	if err == nil {
		fmt.Printf("Tasks remaining: %d\n", remainingTasks)
		fmt.Printf("Tasks deleted: %d\n", len(tasks)-remainingTasks)
	}

	// Example: Safe DELETE - always use WHERE clause
	fmt.Println("\n=== Safe DELETE Practices ===")
	fmt.Println("Always use WHERE clause to avoid deleting all records!")
	fmt.Println("Before DELETE, you can check what will be deleted with SELECT:")

	// Show how to preview what will be deleted
	previewRows, err := db.Query("SELECT id, task_name FROM tasks WHERE priority = ?", "High")
	if err != nil {
		log.Printf("Failed to preview: %v", err)
	} else {
		defer previewRows.Close()
		fmt.Println("High priority tasks that would be deleted:")
		for previewRows.Next() {
			var id int
			var name string
			previewRows.Scan(&id, &name)
			fmt.Printf("  ID %d: %s\n", id, name)
		}
	}

	// Clean up
	_, err = db.Exec("DROP TABLE tasks")
	if err != nil {
		log.Printf("Failed to drop table: %v", err)
	}
	fmt.Println("\nTable dropped successfully")

	// Summary
	fmt.Println("\n=== DELETE Operations Summary ===")
	fmt.Println("This example demonstrated:")
	fmt.Println("- Simple DELETE with WHERE clause")
	fmt.Println("- DELETE with multiple AND conditions")
	fmt.Println("- DELETE with OR conditions")
	fmt.Println("- DELETE with IN clause")
	fmt.Println("- DELETE with date comparisons")
	fmt.Println("- DELETE with LIKE pattern matching")
	fmt.Println("- DELETE with NOT conditions")
	fmt.Println("- DELETE with BETWEEN range")
	fmt.Println("- DELETE with subqueries")
	fmt.Println("- Conditional DELETE using Go logic")
	fmt.Println("- Safe DELETE practices (preview before delete)")
	fmt.Println("\nAll operations completed successfully!")
}

// Helper function to display tasks
func displayTasks(db *sql.DB) {
	rows, err := db.Query(`SELECT
		id, task_name, priority, status, assigned_to,
		due_date, completion_percentage, estimated_hours
	FROM tasks
	ORDER BY
		CASE priority
			WHEN 'High' THEN 1
			WHEN 'Medium' THEN 2
			WHEN 'Low' THEN 3
		END,
		id`)
	if err != nil {
		log.Printf("Failed to query tasks: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println(
		"ID | Task Name                    | Priority | Status      | Assigned | Due Date   | Comp% | Hours",
	)
	fmt.Println(
		"---|------------------------------|----------|-------------|----------|------------|-------|------",
	)

	for rows.Next() {
		var id, completion int
		var taskName, priority, status, assigned, dueDate string
		var hours float64

		err := rows.Scan(&id, &taskName, &priority, &status, &assigned,
			&dueDate, &completion, &hours)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		// Truncate long task names
		if len(taskName) > 28 {
			taskName = taskName[:25] + "..."
		}

		fmt.Printf("%2d | %-28s | %-8s | %-11s | %-8s | %10s | %5d | %5.1f\n",
			id, taskName, priority, status, assigned, dueDate, completion, hours)
	}
	fmt.Println()
}
