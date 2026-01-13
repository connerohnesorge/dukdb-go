package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/dukdb/dukdb-go"
)

func main() {
	// Connect to an in-memory database
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Create a comprehensive table with various data types
	_, err = db.Exec(`CREATE TABLE data_types_demo (
		-- Integer types
		id INTEGER PRIMARY KEY,
		small_int SMALLINT,
		big_int BIGINT,

		-- Decimal/Numeric types
		price DECIMAL(10,2),
		rating NUMERIC(3,2),

		-- String types
		name VARCHAR(100),
		description TEXT,
		code CHAR(10),

		-- Date and Time types
		birth_date DATE,
		created_at TIMESTAMP,
		meeting_time TIME,

		-- Boolean type
		is_active BOOLEAN,

		-- Floating point types
		float_val FLOAT,
		double_val DOUBLE,

		-- Binary data
		binary_data BLOB,

		-- JSON type (stored as string)
		metadata VARCHAR(500)
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	fmt.Println("Table 'data_types_demo' created successfully")

	// Example 1: Insert with basic data types
	fmt.Println("\n=== Example 1: Basic data types ===")
	_, err = db.Exec(`INSERT INTO data_types_demo (
		id, small_int, big_int, price, rating, name, description, code,
		birth_date, created_at, meeting_time, is_active, float_val, double_val
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		1,
		32767,           // SMALLINT max value
		9223372036854775807, // BIGINT max value
		1234.56,         // DECIMAL
		4.75,            // NUMERIC
		"Sample Product", // VARCHAR
		"This is a detailed description that can be very long", // TEXT
		"PRD001",        // CHAR(10)
		"1990-05-15",    // DATE
		"2024-01-15 14:30:00", // TIMESTAMP
		"09:00:00",      // TIME
		true,            // BOOLEAN
		3.14159,         // FLOAT
		2.71828182846,   // DOUBLE
	)
	if err != nil {
		log.Printf("Failed to insert basic data: %v", err)
	} else {
		fmt.Println("Basic data inserted successfully")
	}

	// Example 2: Insert with NULL values
	fmt.Println("\n=== Example 2: Insert with NULL values ===")
	_, err = db.Exec(`INSERT INTO data_types_demo (
		id, name, price, is_active, birth_date, metadata
	) VALUES (?, ?, ?, ?, ?, ?)`,
		2,
		"Product with NULLs",
		nil,  // NULL price
		false,
		nil,  // NULL date
		nil,  // NULL metadata
	)
	if err != nil {
		log.Printf("Failed to insert NULL data: %v", err)
	} else {
		fmt.Println("Data with NULLs inserted successfully")
	}

	// Example 3: Insert with binary data (BLOB)
	fmt.Println("\n=== Example 3: Binary data (BLOB) ===")
	binaryData := []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x42, 0x4C, 0x4F, 0x42} // "Hello BLOB"
	_, err = db.Exec(`INSERT INTO data_types_demo (
		id, name, binary_data
	) VALUES (?, ?, ?)`,
		3,
		"Binary Data Product",
		binaryData,
	)
	if err != nil {
		log.Printf("Failed to insert binary data: %v", err)
	} else {
		fmt.Println("Binary data inserted successfully")
	}

	// Example 4: Insert with JSON data
	fmt.Println("\n=== Example 4: JSON data ===")
	jsonData := `{"color": "blue", "size": "large", "tags": ["new", "featured"]}`
	_, err = db.Exec(`INSERT INTO data_types_demo (
		id, name, metadata
	) VALUES (?, ?, ?)`,
		4,
		"JSON Product",
		jsonData,
	)
	if err != nil {
		log.Printf("Failed to insert JSON data: %v", err)
	} else {
		fmt.Println("JSON data inserted successfully")
	}

	// Example 5: Insert with current timestamp
	fmt.Println("\n=== Example 5: Current timestamp ===")
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(`INSERT INTO data_types_demo (
		id, name, created_at
	) VALUES (?, ?, ?)`,
		5,
		"Current Time Product",
		currentTime,
	)
	if err != nil {
		log.Printf("Failed to insert with current time: %v", err)
	} else {
		fmt.Println("Data with current timestamp inserted successfully")
	}

	// Example 6: Bulk insert with prepared statement
	fmt.Println("\n=== Example 6: Bulk insert with prepared statement ===")
	stmt, err := db.Prepare(`INSERT INTO data_types_demo (
		id, name, price, is_active, birth_date
	) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatal("Failed to prepare statement:", err)
	}
	defer stmt.Close()

	bulkData := []struct {
		id       int
		name     string
		price    float64
		active   bool
		birthDate string
	}{
		{6, "Bulk Product 1", 99.99, true, "2020-01-01"},
		{7, "Bulk Product 2", 149.99, false, "2021-06-15"},
		{8, "Bulk Product 3", 199.99, true, "2022-12-31"},
		{9, "Bulk Product 4", 79.99, true, "2023-03-20"},
		{10, "Bulk Product 5", 299.99, false, "2024-01-01"},
	}

	for _, data := range bulkData {
		_, err = stmt.Exec(data.id, data.name, data.price, data.active, data.birthDate)
		if err != nil {
			log.Printf("Failed to insert bulk data %d: %v", data.id, err)
		}
	}
	fmt.Println("Bulk data inserted successfully")

	// Example 7: Insert with type conversions
	fmt.Println("\n=== Example 7: Type conversions ===")

	// Insert string that will be converted to number
	_, err = db.Exec(`INSERT INTO data_types_demo (id, name, price) VALUES (?, ?, ?)`,
		11, "String to Number", "123.45")
	if err != nil {
		log.Printf("Failed to insert with string number: %v", err)
	} else {
		fmt.Println("String to number conversion successful")
	}

	// Example 8: Query and display all inserted data
	fmt.Println("\n=== Displaying all inserted data ===")
	rows, err := db.Query(`SELECT
		id, name, price, is_active, birth_date,
		LENGTH(binary_data) as blob_size,
		LENGTH(metadata) as json_size
	FROM data_types_demo ORDER BY id`)
	if err != nil {
		log.Fatal("Failed to query data:", err)
	}
	defer rows.Close()

	type productSummary struct {
		id        int
		name      string
		price     sql.NullFloat64
		isActive  sql.NullBool
		birthDate sql.NullString
		blobSize  sql.NullInt64
		jsonSize  sql.NullInt64
	}

	fmt.Println("ID | Name                 | Price    | Active | Birth Date | Blob Size | JSON Size")
	fmt.Println("---|----------------------|----------|--------|------------|-----------|----------")

	for rows.Next() {
		var p productSummary
		err := rows.Scan(&p.id, &p.name, &p.price, &p.isActive, &p.birthDate,
			&p.blobSize, &p.jsonSize)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		priceStr := "NULL"
		if p.price.Valid {
			priceStr = fmt.Sprintf("$%8.2f", p.price.Float64)
		}

		activeStr := "NULL"
		if p.isActive.Valid {
			activeStr = fmt.Sprintf("%t", p.isActive.Bool)
		}

		dateStr := "NULL"
		if p.birthDate.Valid {
			dateStr = p.birthDate.String
		}

		blobStr := "NULL"
		if p.blobSize.Valid {
			blobStr = fmt.Sprintf("%d", p.blobSize.Int64)
		}

		jsonStr := "NULL"
		if p.jsonSize.Valid {
			jsonStr = fmt.Sprintf("%d", p.jsonSize.Int64)
		}

		fmt.Printf("%2d | %-20s | %s | %6s | %10s | %9s | %9s\n",
			p.id, p.name, priceStr, activeStr, dateStr, blobStr, jsonStr)
	}

	// Example 9: Verify binary data
	fmt.Println("\n=== Verifying binary data ===")
	var binaryDataRetrieved []byte
	err = db.QueryRow("SELECT binary_data FROM data_types_demo WHERE id = ?", 3).Scan(&binaryDataRetrieved)
	if err != nil {
		log.Printf("Failed to retrieve binary data: %v", err)
	} else {
		fmt.Printf("Retrieved binary data: %v\n", binaryDataRetrieved)
		fmt.Printf("As string: %s\n", string(binaryDataRetrieved))
	}

	// Example 10: Verify JSON data
	fmt.Println("\n=== Verifying JSON data ===")
	var jsonDataRetrieved string
	err = db.QueryRow("SELECT metadata FROM data_types_demo WHERE id = ?", 4).Scan(&jsonDataRetrieved)
	if err != nil {
		log.Printf("Failed to retrieve JSON data: %v", err)
	} else {
		fmt.Printf("Retrieved JSON data: %s\n", jsonDataRetrieved)
	}

	// Clean up
	_, err = db.Exec("DROP TABLE data_types_demo")
	if err != nil {
		log.Printf("Failed to drop table: %v", err)
	}
	fmt.Println("\nTable dropped successfully")

	// Summary
	fmt.Println("\n=== Summary ===")
	fmt.Println("This example demonstrated:")
	fmt.Println("- Various numeric types (INTEGER, SMALLINT, BIGINT, DECIMAL, NUMERIC)")
	fmt.Println("- String types (VARCHAR, TEXT, CHAR)")
	fmt.Println("- Date/Time types (DATE, TIMESTAMP, TIME)")
	fmt.Println("- Boolean type")
	fmt.Println("- Floating point types (FLOAT, DOUBLE)")
	fmt.Println("- Binary data (BLOB)")
	fmt.Println("- JSON data (stored as VARCHAR)")
	fmt.Println("- NULL value handling")
	fmt.Println("- Bulk inserts with prepared statements")
	fmt.Println("- Type conversions")
	fmt.Println("- Retrieving and displaying various data types")
	fmt.Println("\nAll operations completed successfully!")
}