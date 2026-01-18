package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/dukdb/dukdb-go"
	// Import engine to register backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create a new database connection
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	fmt.Println("=== CSV Data Cleaning Example ===")

	// Example 1: Create messy CSV data
	fmt.Println("\n1. Creating messy CSV data for cleaning...")

	messyCSV := `id,name,email,phone,age,salary,join_date,department
1,John Doe,john.doe@email.com,555-1234,28,50000,2020-01-15,Engineering
2,jane smith,jane.smith@email.com,(555) 234-5678,32,60000,2019-06-20,marketing
3,Bob Johnson,bob.johnson@email,555.345.6789,45,75000,2021-03-10,Engineering
4,alice  brown  ,ALICE.BROWN@EMAIL.COM,5554567890,29,55000,2022-11-25,  sales
5,Charlie Wilson,charlie@email,123-456-7890,-5,45000,2023-13-45,Engineering
6,diana prince,diana.prince@email.com,555-5678,25,48000,2023-01-15,HR
7,  Ed Davis  ,ed.davis@email.com,555-6789,30,52000,2020-12-01,Engineering
8,Frank Miller,frank.miller@email.com,555-7890,35,70000,2018-04-30,Marketing
9,Grace Lee,grace.lee@email,555-8901,twenty-six,58000,2021-08-15,Sales
10,Henry Clark,henry.clark@email.com,555-9012,40,65000,2019-10-20,Engineering
11,,invalid@,555-0000,25,30000,2023-01-01,Engineering
12,Test User,test@email.com,,30,,2022-06-15,`

	messyFile := "messy_data.csv"
	err = os.WriteFile(messyFile, []byte(messyCSV), 0644)
	if err != nil {
		log.Fatal("Failed to create messy CSV:", err)
	}
	defer os.Remove(messyFile)

	// Example 2: Basic data quality assessment
	fmt.Println("\n2. Data Quality Assessment:")

	// Count total rows
	var totalRows int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_csv_auto('%s')", messyFile)).
		Scan(&totalRows)
	if err != nil {
		log.Fatal("Failed to count rows:", err)
	}
	fmt.Printf("Total rows: %d\n", totalRows)

	// Check for NULL values in each column
	columns := []string{"id", "name", "email", "phone", "age", "salary", "join_date", "department"}
	for _, col := range columns {
		var nullCount int
		query := fmt.Sprintf(
			"SELECT COUNT(*) FROM read_csv_auto('%s') WHERE %s IS NULL OR TRIM(%s) = ''",
			messyFile,
			col,
			col,
		)
		err = db.QueryRow(query).Scan(&nullCount)
		if err != nil {
			log.Printf("Warning: Failed to check nulls in %s: %v", col, err)
			continue
		}
		if nullCount > 0 {
			fmt.Printf("  %s: %d NULL/empty values\n", col, nullCount)
		}
	}

	// Example 3: Clean whitespace
	fmt.Println("\n3. Cleaning Whitespace:")

	query := fmt.Sprintf(`
		SELECT
			TRIM(name) as cleaned_name,
			TRIM(email) as cleaned_email,
			TRIM(department) as cleaned_department
		FROM read_csv_auto('%s')
		WHERE name IS NOT NULL
		LIMIT 5
	`, messyFile)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("Failed to query data:", err)
	}
	defer rows.Close()

	fmt.Println("Before and after whitespace cleaning:")
	for rows.Next() {
		var cleanedName, cleanedEmail, cleanedDept string
		err := rows.Scan(&cleanedName, &cleanedEmail, &cleanedDept)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}
		fmt.Printf(
			"  Name: '%s' | Email: '%s' | Dept: '%s'\n",
			cleanedName,
			cleanedEmail,
			cleanedDept,
		)
	}

	// Example 4: Standardize case
	fmt.Println("\n4. Standardizing Case:")

	query = fmt.Sprintf(`
		SELECT
// 			name,
// 			UPPER(name) as upper_case,
// 			LOWER(name) as lower_case,
// 			INITCAP(name) as proper_case
// 		FROM read_csv_auto('%s')
// 		WHERE name IS NOT NULL
// 		LIMIT 5
// 	`, messyFile)
	//
	// 	rows, err = db.Query(query)
	// 	if err != nil {
	// 		log.Fatal("Failed to query data:", err)
	// 	}
	// 	defer rows.Close()
	//
	// 	fmt.Println("Case standardization examples:")
	// 	for rows.Next() {
	// 		var name, upper, lower, proper string
	// 		err := rows.Scan(&name, &upper, &lower, &proper)
	// 		if err != nil {
	// 			log.Fatal("Failed to scan row:", err)
	// 		}
	// 		fmt.Printf("  Original: '%s'\n", name)
	// 		fmt.Printf("    UPPER: '%s'\n", upper)
	// 		fmt.Printf("    LOWER: '%s'\n", lower)
	// 		fmt.Printf("    INITCAP: '%s'\n", proper)
	// 	}
	//
	// 	// Example 5: Validate and clean email addresses
	// 	fmt.Println("\n5. Email Validation and Cleaning:")
	//
	query = fmt.Sprintf(`
		SELECT
			email,
			CASE
				WHEN email IS NULL THEN 'MISSING_EMAIL'
				WHEN TRIM(email) = '' THEN 'EMPTY_EMAIL'
				WHEN email NOT LIKE '%%@%%.%%' THEN 'INVALID_FORMAT'
				WHEN POSITION(' ' IN email) > 0 THEN 'CONTAINS_SPACE'
				ELSE 'VALID'
			END as email_status,
			LOWER(TRIM(email)) as cleaned_email
		FROM read_csv_auto('%s')
		ORDER BY email_status
	`, messyFile)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query emails:", err)
	}
	defer rows.Close()

	fmt.Println("Email validation results:")
	for rows.Next() {
		var email, status, cleaned string
		err := rows.Scan(&email, &status, &cleaned)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}
		fmt.Printf("  '%s' -> %s (cleaned: '%s')\n", email, status, cleaned)
	}

	// Example 6: Phone number standardization
	fmt.Println("\n6. Phone Number Standardization:")

	query = fmt.Sprintf(`
		SELECT
			phone,
			REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as digits_only,
			CASE
				WHEN REGEXP_REPLACE(phone, '[^0-9]', '', 'g') LIKE '555%%%%' THEN
					'(' || SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 1, 3) || ') ' ||
					SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 4, 3) || '-' ||
					SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 7, 4)
				ELSE phone
			END as formatted_phone
		FROM read_csv_auto('%s')
		WHERE phone IS NOT NULL AND TRIM(phone) != ''
		LIMIT 8
	`, messyFile)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query phones:", err)
	}
	defer rows.Close()

	fmt.Println("Phone number standardization:")
	for rows.Next() {
		var original, digits, formatted string
		err := rows.Scan(&original, &digits, &formatted)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}
		fmt.Printf("  '%s' -> '%s' (digits: %s)\n", original, formatted, digits)
	}

	// Example 7: Data type validation and conversion
	fmt.Println("\n7. Data Type Validation and Conversion:")

	query = fmt.Sprintf(`
		SELECT
			id,
			age,
			CASE
				WHEN age IS NULL THEN 'NULL_VALUE'
				WHEN TRIM(age) = '' THEN 'EMPTY_STRING'
				WHEN age ~ '^[0-9]+$' THEN 'VALID_NUMBER'
				ELSE 'INVALID_FORMAT'
			END as age_validation,
			TRY_CAST(age AS INTEGER) as age_as_number,
			CASE
				WHEN TRY_CAST(age AS INTEGER) BETWEEN 18 AND 65 THEN 'VALID_AGE'
				WHEN TRY_CAST(age AS INTEGER) < 18 THEN 'TOO_YOUNG'
				WHEN TRY_CAST(age AS INTEGER) > 65 THEN 'TOO_OLD'
				ELSE 'INVALID_AGE'
			END as age_range_check
		FROM read_csv_auto('%s')
	`, messyFile)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query age validation:", err)
	}
	defer rows.Close()

	fmt.Println("Age validation and conversion:")
	for rows.Next() {
		var id, age, validation string
		var ageNumber sql.NullInt64
		var rangeCheck string

		err := rows.Scan(&id, &age, &validation, &ageNumber, &rangeCheck)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		ageNumStr := "NULL"
		if ageNumber.Valid {
			ageNumStr = strconv.FormatInt(ageNumber.Int64, 10)
		}

		fmt.Printf("  ID %s: age='%s' -> validation=%s, number=%s, range=%s\n",
			id, age, validation, ageNumStr, rangeCheck)
	}

	// Example 8: Date cleaning and validation
	fmt.Println("\n8. Date Cleaning and Validation:")

	query = fmt.Sprintf(`
		SELECT
			join_date,
			CASE
				WHEN join_date IS NULL THEN 'MISSING_DATE'
				WHEN TRY_CAST(join_date AS DATE) IS NOT NULL THEN 'VALID_DATE'
				ELSE 'INVALID_DATE'
			END as date_validation,
			TRY_CAST(join_date AS DATE) as cleaned_date,
			CASE
				WHEN TRY_CAST(join_date AS DATE) IS NOT NULL AND
				     TRY_CAST(join_date AS DATE) > CURRENT_DATE THEN 'FUTURE_DATE'
				WHEN TRY_CAST(join_date AS DATE) IS NOT NULL AND
				     TRY_CAST(join_date AS DATE) < '2000-01-01' THEN 'TOO_OLD'
				ELSE 'VALID_RANGE'
			END as date_range_check
		FROM read_csv_auto('%s')
	`, messyFile)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal("Failed to query date validation:", err)
	}
	defer rows.Close()

	fmt.Println("Date validation and cleaning:")
	for rows.Next() {
		var originalDate, validation, cleanedDate, rangeCheck string
		err := rows.Scan(&originalDate, &validation, &cleanedDate, &rangeCheck)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}
		fmt.Printf("  '%s' -> %s (cleaned: %s, range: %s)\n",
			originalDate, validation, cleanedDate, rangeCheck)
	}

	// Example 9: Create cleaned dataset
	fmt.Println("\n9. Creating Cleaned Dataset:")

	cleanedFile := "cleaned_data.csv"
	createQuery := fmt.Sprintf(`
// 		COPY (
// 			SELECT
// 				id,
// 				INITCAP(TRIM(name)) as clean_name,
// 				LOWER(TRIM(email)) as clean_email,
// 				CASE
// 					WHEN REGEXP_REPLACE(phone, '[^0-9]', '', 'g') LIKE '555%%%%' THEN
// 						'(' || SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 1, 3) || ') ' ||
// 						SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 4, 3) || '-' ||
// 						SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 7, 4)
// 					ELSE NULL
// 				END as clean_phone,
// 				TRY_CAST(TRIM(age) AS INTEGER) as clean_age,
// 				TRY_CAST(salary AS DOUBLE) as clean_salary,
// 				TRY_CAST(TRIM(join_date) AS DATE) as clean_join_date,
// 				INITCAP(TRIM(department)) as clean_department
// 			FROM read_csv_auto('%s')
// 			WHERE id IS NOT NULL
// 		) TO '%s' WITH (HEADER true)
// 	`, messyFile, cleanedFile)
	//
	_, err = db.Exec(createQuery)
	if err != nil {
		log.Fatal("Failed to create cleaned dataset:", err)
	}
	defer os.Remove(cleanedFile)

	fmt.Printf("Cleaned dataset exported to %s\n", cleanedFile)

	// Verify cleaned data
	var cleanedCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_csv_auto('%s')", cleanedFile)).
		Scan(&cleanedCount)
	if err != nil {
		log.Fatal("Failed to count cleaned rows:", err)
	}
	fmt.Printf("Cleaned dataset has %d rows\n", cleanedCount)

	// Example 10: Data quality report
	fmt.Println("\n10. Final Data Quality Report:")

	reportQuery := fmt.Sprintf(`
		SELECT
			'Original Data' as dataset,
			COUNT(*) as total_rows,
			COUNT(CASE WHEN name IS NULL OR TRIM(name) = '' THEN 1 END) as missing_names,
			COUNT(CASE WHEN email IS NULL OR email NOT LIKE '%%@%%.%%' THEN 1 END) as invalid_emails,
			COUNT(CASE WHEN age IS NULL OR TRY_CAST(age AS INTEGER) IS NULL OR TRY_CAST(age AS INTEGER) < 0 THEN 1 END) as invalid_ages,
			COUNT(CASE WHEN join_date IS NULL OR TRY_CAST(join_date AS DATE) IS NULL THEN 1 END) as invalid_dates
		FROM read_csv_auto('%s')
		UNION ALL
		SELECT
			'Cleaned Data' as dataset,
			COUNT(*) as total_rows,
			COUNT(CASE WHEN clean_name IS NULL THEN 1 END) as missing_names,
			COUNT(CASE WHEN clean_email IS NULL OR clean_email NOT LIKE '%%@%%.%%' THEN 1 END) as invalid_emails,
			COUNT(CASE WHEN clean_age IS NULL OR clean_age < 0 THEN 1 END) as invalid_ages,
			COUNT(CASE WHEN clean_join_date IS NULL THEN 1 END) as invalid_dates
		FROM read_csv_auto('%s')
	`, messyFile, cleanedFile)

	rows, err = db.Query(reportQuery)
	if err != nil {
		log.Fatal("Failed to generate quality report:", err)
	}
	defer rows.Close()

	fmt.Println("Dataset\t\tTotal\tMissing Names\tInvalid Emails\tInvalid Ages\tInvalid Dates")
	fmt.Println(strings.Repeat("-", 80))
	for rows.Next() {
		var dataset string
		var total, missingNames, invalidEmails, invalidAges, invalidDates int
		err := rows.Scan(
			&dataset,
			&total,
			&missingNames,
			&invalidEmails,
			&invalidAges,
			&invalidDates,
		)
		if err != nil {
			log.Fatal("Failed to scan report row:", err)
		}
		fmt.Printf("%-12s\t%d\t%d\t\t%d\t\t%d\t\t%d\n",
			dataset, total, missingNames, invalidEmails, invalidAges, invalidDates)
	}

	fmt.Println("\n✓ CSV data cleaning example completed successfully!")
	fmt.Println("\nKey cleaning operations performed:")
	fmt.Println("- Trimmed whitespace from text fields")
	fmt.Println("- Standardized case formatting")
	fmt.Println("- Validated email formats")
	fmt.Println("- Standardized phone numbers")
	fmt.Println("- Validated and converted data types")
	fmt.Println("- Validated date formats")
	fmt.Println("- Removed records with NULL IDs")
	fmt.Println("- Generated clean output dataset")
}

// Helper function to demonstrate regex patterns
func demonstrateRegexPatterns() {
	fmt.Println("\nCommon Regex Patterns for Data Cleaning:")
	fmt.Println("1. Email validation: '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
	fmt.Println("2. Phone digits: '[^0-9]' (remove non-digits)")
	fmt.Println("3. Whitespace: '^\\s+|\\s+$' (trim leading/trailing)")
	fmt.Println("4. Multiple spaces: '\\s+' (replace with single space)")
	fmt.Println("5. Numbers only: '^[0-9]+$'")
}

// Helper function to create a data cleaning checklist
func createCleaningChecklist() {
	fmt.Println("\nData Cleaning Checklist:")
	fmt.Println("□ Remove leading/trailing whitespace")
	fmt.Println("□ Standardize case (UPPER/LOWER/INITCAP)")
	fmt.Println("□ Validate email formats")
	fmt.Println("□ Standardize phone numbers")
	fmt.Println("□ Convert data types")
	fmt.Println("□ Validate date formats")
	fmt.Println("□ Check for NULL/empty values")
	fmt.Println("□ Remove duplicate records")
	fmt.Println("□ Validate ranges (age, dates, etc.)")
	fmt.Println("□ Generate quality report")
}
