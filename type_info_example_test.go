package dukdb_test

import (
	"fmt"

	"github.com/dukdb/dukdb-go"
)

// ExampleNewTypeInfo demonstrates creating TypeInfo for primitive types.
func ExampleNewTypeInfo() {
	// Create TypeInfo for an integer column
	intInfo, err := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	if err != nil {
		panic(err)
	}
	fmt.Println("Type:", intInfo.InternalType())
	fmt.Println("SQL:", intInfo.SQLType())

	// Create TypeInfo for a varchar column
	varcharInfo, err := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
	if err != nil {
		panic(err)
	}
	fmt.Println("Type:", varcharInfo.InternalType())
	fmt.Println("SQL:", varcharInfo.SQLType())

	// Output:
	// Type: INTEGER
	// SQL: INTEGER
	// Type: VARCHAR
	// SQL: VARCHAR
}

// ExampleNewDecimalInfo demonstrates creating DECIMAL type information.
func ExampleNewDecimalInfo() {
	// Create DECIMAL(10,2) for currency values
	decInfo, err := dukdb.NewDecimalInfo(10, 2)
	if err != nil {
		panic(err)
	}
	fmt.Println("SQL:", decInfo.SQLType())

	// Access details
	details := decInfo.Details().(*dukdb.DecimalDetails)
	fmt.Println("Width:", details.Width)
	fmt.Println("Scale:", details.Scale)

	// Output:
	// SQL: DECIMAL(10,2)
	// Width: 10
	// Scale: 2
}

// ExampleNewEnumInfo demonstrates creating ENUM type information.
func ExampleNewEnumInfo() {
	// Create an enum type for t-shirt sizes
	enumInfo, err := dukdb.NewEnumInfo("small", "medium", "large", "x-large")
	if err != nil {
		panic(err)
	}
	fmt.Println("SQL:", enumInfo.SQLType())

	// Access the enum values
	details := enumInfo.Details().(*dukdb.EnumDetails)
	fmt.Println("Values:", details.Values)

	// Output:
	// SQL: ENUM('small', 'medium', 'large', 'x-large')
	// Values: [small medium large x-large]
}

// ExampleNewListInfo demonstrates creating LIST type information.
func ExampleNewListInfo() {
	// Create a list of integers
	intInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	listInfo, err := dukdb.NewListInfo(intInfo)
	if err != nil {
		panic(err)
	}
	fmt.Println("SQL:", listInfo.SQLType())

	// Access the child type
	details := listInfo.Details().(*dukdb.ListDetails)
	fmt.Println("Child type:", details.Child.SQLType())

	// Output:
	// SQL: INTEGER[]
	// Child type: INTEGER
}

// ExampleNewArrayInfo demonstrates creating fixed-size ARRAY type information.
func ExampleNewArrayInfo() {
	// Create a fixed-size array of 3 integers
	intInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	arrayInfo, err := dukdb.NewArrayInfo(intInfo, 3)
	if err != nil {
		panic(err)
	}
	fmt.Println("SQL:", arrayInfo.SQLType())

	// Access the array details
	details := arrayInfo.Details().(*dukdb.ArrayDetails)
	fmt.Println("Size:", details.Size)
	fmt.Println("Child type:", details.Child.SQLType())

	// Output:
	// SQL: INTEGER[3]
	// Size: 3
	// Child type: INTEGER
}

// ExampleNewMapInfo demonstrates creating MAP type information.
func ExampleNewMapInfo() {
	// Create a map from string to integer
	keyInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
	valueInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)

	mapInfo, err := dukdb.NewMapInfo(keyInfo, valueInfo)
	if err != nil {
		panic(err)
	}
	fmt.Println("SQL:", mapInfo.SQLType())

	// Access the key and value types
	details := mapInfo.Details().(*dukdb.MapDetails)
	fmt.Println("Key type:", details.Key.SQLType())
	fmt.Println("Value type:", details.Value.SQLType())

	// Output:
	// SQL: MAP(VARCHAR, INTEGER)
	// Key type: VARCHAR
	// Value type: INTEGER
}

// ExampleNewStructInfo demonstrates creating STRUCT type information.
func ExampleNewStructInfo() {
	// Create struct entries
	intInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	strInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

	idEntry, _ := dukdb.NewStructEntry(intInfo, "id")
	nameEntry, _ := dukdb.NewStructEntry(strInfo, "name")

	// Create the struct type
	structInfo, err := dukdb.NewStructInfo(idEntry, nameEntry)
	if err != nil {
		panic(err)
	}
	fmt.Println("SQL:", structInfo.SQLType())

	// Access struct fields
	details := structInfo.Details().(*dukdb.StructDetails)
	for _, entry := range details.Entries {
		fmt.Printf("Field %q: %s\n", entry.Name(), entry.Info().SQLType())
	}

	// Output:
	// SQL: STRUCT("id" INTEGER, "name" VARCHAR)
	// Field "id": INTEGER
	// Field "name": VARCHAR
}

// ExampleNewUnionInfo demonstrates creating UNION type information.
func ExampleNewUnionInfo() {
	// Create member types
	intInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	strInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

	// Create the union type
	unionInfo, err := dukdb.NewUnionInfo(
		[]dukdb.TypeInfo{intInfo, strInfo},
		[]string{"num", "str"},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("SQL:", unionInfo.SQLType())

	// Access union members
	details := unionInfo.Details().(*dukdb.UnionDetails)
	for _, member := range details.Members {
		fmt.Printf("Member %q: %s\n", member.Name, member.Type.SQLType())
	}

	// Output:
	// SQL: UNION("num" INTEGER, "str" VARCHAR)
	// Member "num": INTEGER
	// Member "str": VARCHAR
}

// Example_nestedTypes demonstrates creating nested type structures.
func Example_nestedTypes() {
	// Create a complex nested type: MAP[VARCHAR, LIST[STRUCT(id INTEGER, name VARCHAR)]]

	// Build from innermost to outermost:
	intInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	strInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

	// 1. Create the inner struct
	idEntry, _ := dukdb.NewStructEntry(intInfo, "id")
	nameEntry, _ := dukdb.NewStructEntry(strInfo, "name")
	structInfo, _ := dukdb.NewStructInfo(idEntry, nameEntry)

	// 2. Wrap in a list
	listInfo, _ := dukdb.NewListInfo(structInfo)

	// 3. Create the map
	mapInfo, _ := dukdb.NewMapInfo(strInfo, listInfo)

	fmt.Println("SQL:", mapInfo.SQLType())

	// Output:
	// SQL: MAP(VARCHAR, STRUCT("id" INTEGER, "name" VARCHAR)[])
}
