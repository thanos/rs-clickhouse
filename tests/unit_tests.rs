use clickhouse_rs::types::{Block, Value, ColumnData, Column, Row};
use clickhouse_rs::error::Error;
use std::collections::HashMap;

#[test]
fn test_block_creation_and_manipulation() {
    let mut block = Block::new();
    
    // Test empty block
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.column_count(), 0);
    
    // Add columns
    block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2, 3, 4, 5])));
    block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
        "Diana".to_string(),
        "Eve".to_string(),
    ])));
    block.add_column("age", Column::new("age", "UInt8", ColumnData::UInt8(vec![25, 30, 35, 28, 32])));
    block.add_column("active", Column::new("active", "UInt8", ColumnData::UInt8(vec![1, 1, 0, 1, 0])));
    
    // Test block properties
    assert_eq!(block.row_count(), 5);
    assert_eq!(block.column_count(), 4);
    
    // Test column access
    let id_column = block.get_column("id").expect("Should have id column");
    let name_column = block.get_column("name").expect("Should have name column");
    let age_column = block.get_column("age").expect("Should have age column");
    let active_column = block.get_column("active").expect("Should have active column");
    
    assert_eq!(id_column.name, "id");
    assert_eq!(name_column.name, "name");
    assert_eq!(age_column.name, "age");
    assert_eq!(active_column.name, "active");
    
    // Test column data
    assert_eq!(id_column.data.len(), 5);
    assert_eq!(name_column.data.len(), 5);
    assert_eq!(age_column.data.len(), 5);
    assert_eq!(active_column.data.len(), 5);
    
    // Test non-existent column
    assert!(block.get_column("non_existent").is_none());
}

#[test]
fn test_row_access() {
    let mut block = Block::new();
    
    // Create a simple block
    block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2, 3])));
    block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
    ])));
    
    // Test row access
    let row0 = block.get_row(0).expect("Should have first row");
    let row1 = block.get_row(1).expect("Should have second row");
    let row2 = block.get_row(2).expect("Should have third row");
    
    // Test row values
    let id0 = row0.get(0).and_then(|v| v.as_ref()).expect("Should have id value");
    let name0 = row0.get(1).and_then(|v| v.as_ref()).expect("Should have name value");
    
    let id1 = row1.get(0).and_then(|v| v.as_ref()).expect("Should have id value");
    let name1 = row1.get(1).and_then(|v| v.as_ref()).expect("Should have name value");
    
    let id2 = row2.get(0).and_then(|v| v.as_ref()).expect("Should have id value");
    let name2 = row2.get(1).and_then(|v| v.as_ref()).expect("Should have name value");
    
    // Verify values
    assert!(matches!(id0, Value::UInt32(1)));
    assert!(matches!(name0, Value::String(s) if s == "Alice"));
    
    assert!(matches!(id1, Value::UInt32(2)));
    assert!(matches!(name1, Value::String(s) if s == "Bob"));
    
    assert!(matches!(id2, Value::UInt32(3)));
    assert!(matches!(name2, Value::String(s) if s == "Charlie"));
    
    // Test out of bounds access
    assert!(block.get_row(3).is_none());
    assert!(row0.get(2).is_none());
}

#[test]
fn test_value_types_and_conversions() {
    // Test numeric types
    let uint8_val = Value::UInt8(42);
    let uint16_val = Value::UInt16(1000);
    let uint32_val = Value::UInt32(1000000);
    let uint64_val = Value::UInt64(1000000000);
    let uint128_val = Value::UInt128(1000000000000000000);
    
    let int8_val = Value::Int8(-42);
    let int16_val = Value::Int16(-1000);
    let int32_val = Value::Int32(-1000000);
    let int64_val = Value::Int64(-1000000000);
    let int128_val = Value::Int128(-1000000000000000000);
    
    let float32_val = Value::Float32(3.14);
    let float64_val = Value::Float64(2.718);
    
    // Test string types
    let string_val = Value::String("hello world".to_string());
    let fixed_string_val = Value::FixedString(b"fixed".to_vec());
    let low_cardinality_val = Value::LowCardinality("low_card".to_string());
    
    // Test date/time types
    let date_val = Value::Date(chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap());
    let datetime_val = Value::DateTime(
        chrono::NaiveDateTime::parse_from_str("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    let datetime64_val = Value::DateTime64(
        chrono::NaiveDateTime::parse_from_str("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    );
    
    // Test UUID
    let uuid_val = Value::UUID(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());
    
    // Test type names
    assert_eq!(uint8_val.type_name(), "UInt8");
    assert_eq!(uint16_val.type_name(), "UInt16");
    assert_eq!(uint32_val.type_name(), "UInt32");
    assert_eq!(uint64_val.type_name(), "UInt64");
    assert_eq!(uint128_val.type_name(), "UInt128");
    
    assert_eq!(int8_val.type_name(), "Int8");
    assert_eq!(int16_val.type_name(), "Int16");
    assert_eq!(int32_val.type_name(), "Int32");
    assert_eq!(int64_val.type_name(), "Int64");
    assert_eq!(int128_val.type_name(), "Int128");
    
    assert_eq!(float32_val.type_name(), "Float32");
    assert_eq!(float64_val.type_name(), "Float64");
    
    assert_eq!(string_val.type_name(), "String");
    assert_eq!(fixed_string_val.type_name(), "FixedString");
    assert_eq!(low_cardinality_val.type_name(), "LowCardinality");
    
    assert_eq!(date_val.type_name(), "Date");
    assert_eq!(datetime_val.type_name(), "DateTime");
    assert_eq!(datetime64_val.type_name(), "DateTime64");
    
    assert_eq!(uuid_val.type_name(), "UUID");
    
    // Test formatting
    assert_eq!(format!("{}", uint8_val), "42");
    assert_eq!(format!("{}", uint16_val), "1000");
    assert_eq!(format!("{}", string_val), "hello world");
    assert_eq!(format!("{}", date_val), "2023-01-01");
    assert_eq!(format!("{}", datetime_val), "2023-01-01 12:00:00");
}

#[test]
fn test_complex_value_types() {
    // Test array type
    let array_values = vec![
        Value::UInt32(1),
        Value::UInt32(2),
        Value::UInt32(3),
    ];
    let array_val = Value::Array(array_values);
    assert_eq!(array_val.type_name(), "Array");
    
    // Test nullable type
    let nullable_val = Value::Nullable(Some(Box::new(Value::String("not null".to_string()))));
    assert_eq!(nullable_val.type_name(), "Nullable");
    
    let null_val = Value::Null;
    assert_eq!(null_val.type_name(), "Null");
    
    // Test tuple type
    let tuple_values = vec![
        Value::UInt32(1),
        Value::String("hello".to_string()),
        Value::Float64(3.14),
    ];
    let tuple_val = Value::Tuple(tuple_values);
    assert_eq!(tuple_val.type_name(), "Tuple");
    
    // Test map type
    let mut map = HashMap::new();
    map.insert("key1".to_string(), Value::String("value1".to_string()));
    map.insert("key2".to_string(), Value::UInt32(42));
    let map_val = Value::Map(map);
    assert_eq!(map_val.type_name(), "Map");
}

#[test]
fn test_column_data_operations() {
    // Test UInt8 column data
    let uint8_data = ColumnData::UInt8(vec![1, 2, 3, 4, 5]);
    assert_eq!(uint8_data.len(), 5);
    
    // Test String column data
    let string_data = ColumnData::String(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
    ]);
    assert_eq!(string_data.len(), 3);
    
    // Test Date column data
    let date_data = ColumnData::Date(vec![
        chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
        chrono::NaiveDate::from_ymd_opt(2023, 1, 3).unwrap(),
    ]);
    assert_eq!(date_data.len(), 3);
    
    // Test DateTime column data
    let datetime_data = ColumnData::DateTime(vec![
        chrono::NaiveDateTime::parse_from_str("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        chrono::NaiveDateTime::parse_from_str("2023-01-01 13:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
    ]);
    assert_eq!(datetime_data.len(), 2);
    
    // Test UUID column data
    let uuid_data = ColumnData::UUID(vec![
        uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap(),
    ]);
    assert_eq!(uuid_data.len(), 2);
}

#[test]
fn test_column_operations() {
    let column = Column::new("test_column", "UInt32", ColumnData::UInt32(vec![1, 2, 3, 4, 5]));
    
    assert_eq!(column.name, "test_column");
    assert_eq!(column.data.len(), 5);
    assert_eq!(column.type_name, "UInt32");
    
    // Test getting values
    let value0 = column.get_value(0).expect("Should have value at index 0");
    let value1 = column.get_value(1).expect("Should have value at index 1");
    let value2 = column.get_value(2).expect("Should have value at index 2");
    
    assert!(matches!(value0, Value::UInt32(1)));
    assert!(matches!(value1, Value::UInt32(2)));
    assert!(matches!(value2, Value::UInt32(3)));
    
    // Test out of bounds access
    assert!(column.get_value(5).is_none());
}

#[test]
fn test_row_operations() {
    let values = vec![
        Some(Value::UInt32(1)),
        Some(Value::String("Alice".to_string())),
        Some(Value::UInt8(25)),
        None, // Null value
    ];
    
    let row = Row::new(values);
    
    assert_eq!(row.len(), 4);
    assert!(!row.is_empty());
    
    // Test getting values
    let id = row.get(0).and_then(|v| v.as_ref()).expect("Should have id value");
    let name = row.get(1).and_then(|v| v.as_ref()).expect("Should have name value");
    let age = row.get(2).and_then(|v| v.as_ref()).expect("Should have age value");
    let null_val = row.get(3).and_then(|v| v.as_ref());
    
    assert!(matches!(id, Value::UInt32(1)));
    assert!(matches!(name, Value::String(s) if s == "Alice"));
    assert!(matches!(age, Value::UInt8(25)));
    assert!(null_val.is_none()); // Null value
    
    // Test out of bounds access
    assert!(row.get(4).is_none());
}

#[test]
fn test_block_with_different_data_types() {
    let mut block = Block::new();
    
    // Add columns with different data types
    block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2, 3])));
    block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
    ])));
    block.add_column("age", Column::new("age", "UInt8", ColumnData::UInt8(vec![25, 30, 35])));
    block.add_column("height", Column::new("height", "Float32", ColumnData::Float32(vec![1.65, 1.75, 1.80])));
    block.add_column("active", Column::new("active", "UInt8", ColumnData::UInt8(vec![1, 1, 0])));
    block.add_column("birth_date", Column::new("birth_date", "Date", ColumnData::Date(vec![
        chrono::NaiveDate::from_ymd_opt(1998, 1, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(1993, 1, 1).unwrap(),
        chrono::NaiveDate::from_ymd_opt(1988, 1, 1).unwrap(),
    ])));
    
    assert_eq!(block.row_count(), 3);
    assert_eq!(block.column_count(), 6);
    
    // Test row iteration
    for i in 0..block.row_count() {
        let row = block.get_row(i).expect("Should have row");
        println!("Row {}: {:?}", i, row);
        
        // Each row should have 6 values
        assert_eq!(row.len(), 6);
    }
    
    // Test column iteration
    for column in block.columns() {
        println!("Column: {} (type: {})", column.name, column.type_name);
        assert_eq!(column.data.len(), 3);
    }
}

#[test]
fn test_value_equality() {
    // Test numeric equality
    let val1 = Value::UInt32(42);
    let val2 = Value::UInt32(42);
    let val3 = Value::UInt32(43);
    
    assert_eq!(val1, val2);
    assert_ne!(val1, val3);
    
    // Test string equality
    let str1 = Value::String("hello".to_string());
    let str2 = Value::String("hello".to_string());
    let str3 = Value::String("world".to_string());
    
    assert_eq!(str1, str2);
    assert_ne!(str1, str3);
    
    // Test different types
    assert_ne!(val1, str1);
    
    // Test null equality
    let null1 = Value::Null;
    let null2 = Value::Null;
    assert_eq!(null1, null2);
}

#[test]
fn test_block_clone() {
    let mut block = Block::new();
    block.add_column("id", Column::new("id", "UInt32", ColumnData::UInt32(vec![1, 2, 3])));
    block.add_column("name", Column::new("name", "String", ColumnData::String(vec![
        "Alice".to_string(),
        "Bob".to_string(),
        "Charlie".to_string(),
    ])));
    
    let cloned_block = block.clone();
    
    assert_eq!(block.row_count(), cloned_block.row_count());
    assert_eq!(block.column_count(), cloned_block.column_count());
    
    // Test that data is actually cloned
    let original_row = block.get_row(0).expect("Should have first row");
    let cloned_row = cloned_block.get_row(0).expect("Should have first row");
    
    // Compare row contents since Row doesn't implement PartialEq
    assert_eq!(original_row.len(), cloned_row.len());
    for i in 0..original_row.len() {
        let orig_val = original_row.get(i).and_then(|v| v.as_ref());
        let cloned_val = cloned_row.get(i).and_then(|v| v.as_ref());
        assert_eq!(orig_val.is_some(), cloned_val.is_some());
    }
}

#[test]
fn test_error_types() {
    // Test various error types
    let errors = vec![
        Error::Protocol("Protocol error".to_string()),
        Error::Timeout(std::time::Duration::from_secs(5)),
        Error::Unsupported("Feature not implemented".to_string()),
        Error::Authentication("Auth failed".to_string()),
        Error::Compression("Compression error".to_string()),
        Error::Serialization("Serialization error".to_string()),
    ];
    
    for error in errors {
        // Test that we can get the error message
        let message = error.to_string();
        assert!(!message.is_empty(), "Error message should not be empty");
        
        // Test that we can get the error message
        let message = error.to_string();
        assert!(!message.is_empty(), "Error message should not be empty");
        
        // Test debug formatting
        let debug_str = format!("{:?}", error);
        assert!(!debug_str.is_empty(), "Debug string should not be empty");
    }
}

#[test]
fn test_block_with_nullable_values() {
    let mut block = Block::new();
    
    // Add a column with nullable values
    let nullable_values = vec![
        Some(Value::String("Alice".to_string())),
        None,
        Some(Value::String("Bob".to_string())),
        None,
        Some(Value::String("Charlie".to_string())),
    ];
    
    block.add_column("name", Column::new("name", "Nullable(String)", ColumnData::Nullable(nullable_values)));
    
    assert_eq!(block.row_count(), 5);
    assert_eq!(block.column_count(), 1);
    
    // Test accessing nullable values
    let row0 = block.get_row(0).expect("Should have first row");
    let row1 = block.get_row(1).expect("Should have second row");
    let row2 = block.get_row(2).expect("Should have third row");
    
    let name0 = row0.get(0).and_then(|v| v.as_ref());
    let name1 = row1.get(0).and_then(|v| v.as_ref());
    let name2 = row2.get(0).and_then(|v| v.as_ref());
    
    assert!(name0.is_some());
    assert!(name1.is_some()); // Value exists but is nullable
    assert!(name2.is_some());
    
    if let Some(name) = name0 {
        // This should be a nullable value that contains Some(String)
        if let Value::Nullable(Some(boxed)) = name {
            if let Value::String(s) = &**boxed {
                assert_eq!(s, "Alice");
            } else {
                panic!("Expected String value in nullable");
            }
        } else {
            panic!("Expected nullable value");
        }
    }
    
    if let Some(name) = name1 {
        // This should be a nullable value that is None
        assert!(matches!(name, Value::Nullable(None)));
    }
    
    if let Some(name) = name2 {
        // This should be a nullable value that contains Some(String)
        if let Value::Nullable(Some(boxed)) = name {
            if let Value::String(s) = &**boxed {
                assert_eq!(s, "Bob");
            } else {
                panic!("Expected String value in nullable");
            }
        } else {
            panic!("Expected nullable value");
        }
    }
}

#[test]
fn test_block_with_array_values() {
    let mut block = Block::new();
    
    // Add a column with array values
    let array_values = vec![
        vec![Value::UInt32(1), Value::UInt32(2), Value::UInt32(3)],
        vec![Value::UInt32(4), Value::UInt32(5)],
        vec![Value::UInt32(6)],
    ];
    
    block.add_column("numbers", Column::new("numbers", "Array(UInt32)", ColumnData::Array(array_values)));
    
    assert_eq!(block.row_count(), 3);
    assert_eq!(block.column_count(), 1);
    
    // Test accessing array values
    let row0 = block.get_row(0).expect("Should have first row");
    let numbers0 = row0.get(0).and_then(|v| v.as_ref()).expect("Should have numbers value");
    
    // Access the array data directly from the column
    let numbers_column = block.get_column("numbers").expect("Should have numbers column");
    if let ColumnData::Array(arrays) = &numbers_column.data {
        let first_array = &arrays[0];
        assert_eq!(first_array.len(), 3);
        assert!(matches!(first_array[0], Value::UInt32(1)));
        assert!(matches!(first_array[1], Value::UInt32(2)));
        assert!(matches!(first_array[2], Value::UInt32(3)));
    } else {
        panic!("Expected array column data");
    }
}

#[test]
fn test_block_with_map_values() {
    let mut block = Block::new();
    
    // Add a column with map values
    let mut map1 = HashMap::new();
    map1.insert("key1".to_string(), Value::String("value1".to_string()));
    map1.insert("key2".to_string(), Value::UInt32(42));
    
    let mut map2 = HashMap::new();
    map2.insert("name".to_string(), Value::String("Alice".to_string()));
    map2.insert("age".to_string(), Value::UInt8(25));
    
    let map_values = vec![
        map1,
        map2,
    ];
    
    block.add_column("metadata", Column::new("metadata", "Map(String, String)", ColumnData::Map(map_values)));
    
    assert_eq!(block.row_count(), 2);
    assert_eq!(block.column_count(), 1);
    
    // Test accessing map values
    let row0 = block.get_row(0).expect("Should have first row");
    let metadata0 = row0.get(0).and_then(|v| v.as_ref()).expect("Should have metadata value");
    
    // Access the map data directly from the column
    let metadata_column = block.get_column("metadata").expect("Should have metadata column");
    if let ColumnData::Map(maps) = &metadata_column.data {
        let first_map = &maps[0];
        assert_eq!(first_map.len(), 2);
        assert!(first_map.contains_key("key1"));
        assert!(first_map.contains_key("key2"));
        
        if let Some(Value::String(val)) = first_map.get("key1") {
            assert_eq!(val, "value1");
        } else {
            panic!("Expected string value for key1");
        }
        
        if let Some(Value::UInt32(val)) = first_map.get("key2") {
            assert_eq!(*val, 42);
        } else {
            panic!("Expected UInt32 value for key2");
        }
    } else {
        panic!("Expected map column data");
    }
}
