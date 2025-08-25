//! ClickHouse data types and core structures

mod numeric;
mod string;
mod datetime;
mod complex;
mod geometric;


pub use numeric::*;
pub use string::*;
pub use datetime::*;
pub use complex::*;
pub use geometric::*;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a ClickHouse data block containing multiple columns
#[derive(Debug, Clone)]
pub struct Block {
    /// Block metadata
    pub info: BlockInfo,
    /// Columns in the block
    pub columns: Vec<Column>,
    /// Number of rows in the block
    pub row_count: usize,
}

impl Block {
    /// Create a new empty block
    pub fn new() -> Self {
        Self {
            info: BlockInfo::default(),
            columns: Vec::new(),
            row_count: 0,
        }
    }

    /// Get the number of rows in the block
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Create a new block with the specified columns
    pub fn with_columns(columns: Vec<Column>) -> Self {
        let row_count = columns.first().map(|col| col.len()).unwrap_or(0);
        Self {
            info: BlockInfo::default(),
            columns,
            row_count,
        }
    }

    /// Add a column to the block
    pub fn add_column(&mut self, _name: impl Into<String>, column: Column) {
        let column_len = column.len();
        self.columns.push(column);
        if self.row_count == 0 {
            self.row_count = column_len;
        }
    }

    /// Get a column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Get a mutable column by name
    pub fn get_column_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.columns.iter_mut().find(|col| col.name == name)
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if the block is empty
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Clear all data from the block
    pub fn clear(&mut self) {
        self.columns.clear();
        self.row_count = 0;
    }

    /// Get a row by index
    pub fn get_row(&self, index: usize) -> Option<Row> {
        if index >= self.row_count {
            return None;
        }

        let mut values = Vec::new();
        for column in &self.columns {
            if let Some(value) = column.get_value(index) {
                values.push(Some(value));
            } else {
                values.push(None);
            }
        }

        Some(Row { values })
    }

    /// Iterate over rows
    pub fn rows(&self) -> RowIterator {
        RowIterator {
            block: self,
            current: 0,
        }
    }

    /// Iterate over columns
    pub fn columns(&self) -> std::slice::Iter<Column> {
        self.columns.iter()
    }
}

impl Default for Block {
    fn default() -> Self {
        Self::new()
    }
}

/// Block metadata information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    /// Whether this is an overflow block
    pub is_overflows: bool,
    /// Block number
    pub bucket_num: i32,
    /// Number of blocks
    pub num_buckets: i32,
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self {
            is_overflows: false,
            bucket_num: -1,
            num_buckets: -1,
        }
    }
}

/// Represents a column in a ClickHouse block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column type
    pub type_name: String,
    /// Column data
    pub data: ColumnData,
}

impl Column {
    /// Create a new column
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, data: ColumnData) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            data,
        }
    }

    /// Get the length of the column
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Get a value at the specified index
    pub fn get_value(&self, index: usize) -> Option<Value> {
        self.data.get_value(index)
    }

    /// Set a value at the specified index
    pub fn set_value(&mut self, index: usize, value: Value) -> Result<(), String> {
        self.data.set_value(index, value)
    }

    /// Append a value to the column
    pub fn push(&mut self, value: Value) -> Result<(), String> {
        self.data.push(value)
    }

    /// Get the column type
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Check if the column is nullable
    pub fn is_nullable(&self) -> bool {
        self.type_name.starts_with("Nullable(")
    }

    /// Get the underlying type name (without Nullable wrapper)
    pub fn underlying_type(&self) -> &str {
        if self.is_nullable() {
            &self.type_name[9..self.type_name.len() - 1]
        } else {
            &self.type_name
        }
    }
}

/// Column data container
#[derive(Debug, Clone)]
pub enum ColumnData {
    /// UInt8 values
    UInt8(Vec<u8>),
    /// UInt16 values
    UInt16(Vec<u16>),
    /// UInt32 values
    UInt32(Vec<u32>),
    /// UInt64 values
    UInt64(Vec<u64>),
    /// UInt128 values
    UInt128(Vec<u128>),
    /// UInt256 values
    UInt256(Vec<u256::U256>),
    /// Int8 values
    Int8(Vec<i8>),
    /// Int16 values
    Int16(Vec<i16>),
    /// Int32 values
    Int32(Vec<i32>),
    /// Int64 values
    Int64(Vec<i64>),
    /// Int128 values
    Int128(Vec<i128>),
    /// Int256 values
    Int256(Vec<i256::I256>),
    /// Float32 values
    Float32(Vec<f32>),
    /// Float64 values
    Float64(Vec<f64>),
    /// String values
    String(Vec<String>),
    /// FixedString values
    FixedString(Vec<Vec<u8>>),
    /// LowCardinality values
    LowCardinality(Vec<String>),
    /// Date values
    Date(Vec<chrono::NaiveDate>),
    /// DateTime values
    DateTime(Vec<chrono::NaiveDateTime>),
    /// DateTime64 values
    DateTime64(Vec<chrono::NaiveDateTime>),
    /// UUID values
    UUID(Vec<uuid::Uuid>),
    /// Array values
    Array(Vec<Vec<Value>>),
    /// Nullable values
    Nullable(Vec<Option<Value>>),
    /// Tuple values
    Tuple(Vec<Vec<Value>>),
    /// Map values
    Map(Vec<HashMap<String, Value>>),
}

impl ColumnData {
    /// Get the length of the column data
    pub fn len(&self) -> usize {
        match self {
            ColumnData::UInt8(v) => v.len(),
            ColumnData::UInt16(v) => v.len(),
            ColumnData::UInt32(v) => v.len(),
            ColumnData::UInt64(v) => v.len(),
            ColumnData::UInt128(v) => v.len(),
            ColumnData::UInt256(v) => v.len(),
            ColumnData::Int8(v) => v.len(),
            ColumnData::Int16(v) => v.len(),
            ColumnData::Int32(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Int128(v) => v.len(),
            ColumnData::Int256(v) => v.len(),
            ColumnData::Float32(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::String(v) => v.len(),
            ColumnData::FixedString(v) => v.len(),
            ColumnData::LowCardinality(v) => v.len(),
            ColumnData::Date(v) => v.len(),
            ColumnData::DateTime(v) => v.len(),
            ColumnData::DateTime64(v) => v.len(),
            ColumnData::UUID(v) => v.len(),
            ColumnData::Array(v) => v.len(),
            ColumnData::Nullable(v) => v.len(),
            ColumnData::Tuple(v) => v.len(),
            ColumnData::Map(v) => v.len(),
        }
    }

    /// Get a value at the specified index
    pub fn get_value(&self, index: usize) -> Option<Value> {
        if index >= self.len() {
            return None;
        }

        match self {
            ColumnData::UInt8(v) => Some(Value::UInt8(v[index])),
            ColumnData::UInt16(v) => Some(Value::UInt16(v[index])),
            ColumnData::UInt32(v) => Some(Value::UInt32(v[index])),
            ColumnData::UInt64(v) => Some(Value::UInt64(v[index])),
            ColumnData::UInt128(v) => Some(Value::UInt128(v[index])),
            ColumnData::UInt256(v) => Some(Value::UInt256(v[index].clone())),
            ColumnData::Int8(v) => Some(Value::Int8(v[index])),
            ColumnData::Int16(v) => Some(Value::Int16(v[index])),
            ColumnData::Int32(v) => Some(Value::Int32(v[index])),
            ColumnData::Int64(v) => Some(Value::Int64(v[index])),
            ColumnData::Int128(v) => Some(Value::Int128(v[index])),
            ColumnData::Int256(v) => Some(Value::Int256(v[index].clone())),
            ColumnData::Float32(v) => Some(Value::Float32(v[index])),
            ColumnData::Float64(v) => Some(Value::Float64(v[index])),
            ColumnData::String(v) => Some(Value::String(v[index].clone())),
            ColumnData::FixedString(v) => Some(Value::FixedString(v[index].clone())),
            ColumnData::LowCardinality(v) => Some(Value::LowCardinality(v[index].clone())),
            ColumnData::Date(v) => Some(Value::Date(v[index])),
            ColumnData::DateTime(v) => Some(Value::DateTime(v[index])),
            ColumnData::DateTime64(v) => Some(Value::DateTime64(v[index])),
            ColumnData::UUID(v) => Some(Value::UUID(v[index])),
            ColumnData::Array(v) => Some(Value::Array(v[index].clone())),
            ColumnData::Nullable(v) => Some(Value::Nullable(v[index].as_ref().map(|val| Box::new(val.clone())))),
            ColumnData::Tuple(v) => Some(Value::Tuple(v[index].clone())),
            ColumnData::Map(v) => Some(Value::Map(v[index].clone())),
        }
    }

    /// Set a value at the specified index
    pub fn set_value(&mut self, index: usize, value: Value) -> Result<(), String> {
        if index >= self.len() {
            return Err("Index out of bounds".to_string());
        }

        match (self, value) {
            (ColumnData::UInt8(v), Value::UInt8(val)) => v[index] = val,
            (ColumnData::UInt16(v), Value::UInt16(val)) => v[index] = val,
            (ColumnData::UInt32(v), Value::UInt32(val)) => v[index] = val,
            (ColumnData::UInt64(v), Value::UInt64(val)) => v[index] = val,
            (ColumnData::UInt128(v), Value::UInt128(val)) => v[index] = val,
            (ColumnData::UInt256(v), Value::UInt256(val)) => v[index] = val,
            (ColumnData::Int8(v), Value::Int8(val)) => v[index] = val,
            (ColumnData::Int16(v), Value::Int16(val)) => v[index] = val,
            (ColumnData::Int32(v), Value::Int32(val)) => v[index] = val,
            (ColumnData::Int64(v), Value::Int64(val)) => v[index] = val,
            (ColumnData::Int128(v), Value::Int128(val)) => v[index] = val,
            (ColumnData::Int256(v), Value::Int256(val)) => v[index] = val,
            (ColumnData::Float32(v), Value::Float32(val)) => v[index] = val,
            (ColumnData::Float64(v), Value::Float64(val)) => v[index] = val,
            (ColumnData::String(v), Value::String(val)) => v[index] = val,
            (ColumnData::FixedString(v), Value::FixedString(val)) => v[index] = val,
            (ColumnData::LowCardinality(v), Value::LowCardinality(val)) => v[index] = val,
            (ColumnData::Date(v), Value::Date(val)) => v[index] = val,
            (ColumnData::DateTime(v), Value::DateTime(val)) => v[index] = val,
            (ColumnData::DateTime64(v), Value::DateTime64(val)) => v[index] = val,
            (ColumnData::UUID(v), Value::UUID(val)) => v[index] = val,
            (ColumnData::Array(v), Value::Array(val)) => v[index] = val,
            (ColumnData::Nullable(v), Value::Nullable(val)) => v[index] = val.map(|val| *val),
            (ColumnData::Tuple(v), Value::Tuple(val)) => v[index] = val,
            (ColumnData::Map(v), Value::Map(val)) => v[index] = val,
            _ => return Err("Type mismatch".to_string()),
        }

        Ok(())
    }

    /// Push a value to the column
    pub fn push(&mut self, value: Value) -> Result<(), String> {
        match (self, value) {
            (ColumnData::UInt8(v), Value::UInt8(val)) => v.push(val),
            (ColumnData::UInt16(v), Value::UInt16(val)) => v.push(val),
            (ColumnData::UInt32(v), Value::UInt32(val)) => v.push(val),
            (ColumnData::UInt64(v), Value::UInt64(val)) => v.push(val),
            (ColumnData::UInt128(v), Value::UInt128(val)) => v.push(val),
            (ColumnData::UInt256(v), Value::UInt256(val)) => v.push(val),
            (ColumnData::Int8(v), Value::Int8(val)) => v.push(val),
            (ColumnData::Int16(v), Value::Int16(val)) => v.push(val),
            (ColumnData::Int32(v), Value::Int32(val)) => v.push(val),
            (ColumnData::Int64(v), Value::Int64(val)) => v.push(val),
            (ColumnData::Int128(v), Value::Int128(val)) => v.push(val),
            (ColumnData::Int256(v), Value::Int256(val)) => v.push(val),
            (ColumnData::Float32(v), Value::Float32(val)) => v.push(val),
            (ColumnData::Float64(v), Value::Float64(val)) => v.push(val),
            (ColumnData::String(v), Value::String(val)) => v.push(val),
            (ColumnData::FixedString(v), Value::FixedString(val)) => v.push(val),
            (ColumnData::LowCardinality(v), Value::LowCardinality(val)) => v.push(val),
            (ColumnData::Date(v), Value::Date(val)) => v.push(val),
            (ColumnData::DateTime(v), Value::DateTime(val)) => v.push(val),
            (ColumnData::DateTime64(v), Value::DateTime64(val)) => v.push(val),
            (ColumnData::UUID(v), Value::UUID(val)) => v.push(val),
            (ColumnData::Array(v), Value::Array(val)) => v.push(val),
            (ColumnData::Nullable(v), Value::Nullable(val)) => v.push(val.map(|val| *val)),
            (ColumnData::Tuple(v), Value::Tuple(val)) => v.push(val),
            (ColumnData::Map(v), Value::Map(val)) => v.push(val),
            _ => return Err("Type mismatch".to_string()),
        }

        Ok(())
    }
}

/// Represents a row in a ClickHouse block
#[derive(Debug, Clone)]
pub struct Row {
    /// Values in the row
    pub values: Vec<Option<Value>>,
}

impl Row {
    /// Create a new row with the specified values
    pub fn new(values: Vec<Option<Value>>) -> Self {
        Self { values }
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&Option<Value>> {
        self.values.get(index)
    }

    /// Get a value by index with type conversion
    pub fn get_typed<T>(&self, index: usize) -> Result<T, String>
    where
        T: TryFrom<Value>,
        T::Error: std::fmt::Display,
    {
        self.values
            .get(index)
            .and_then(|v| v.as_ref())
            .ok_or_else(|| "Value not found or null".to_string())?
            .clone()
            .try_into()
            .map_err(|e: <T as TryFrom<Value>>::Error| e.to_string())
    }

    /// Get the number of values in the row
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Iterator over rows in a block
pub struct RowIterator<'a> {
    block: &'a Block,
    current: usize,
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.block.row_count {
            None
        } else {
            let row = self.block.get_row(self.current);
            self.current += 1;
            row
        }
    }
}

/// Represents a ClickHouse value
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Null value
    Null,
    /// UInt8 value
    UInt8(u8),
    /// UInt16 value
    UInt16(u16),
    /// UInt32 value
    UInt32(u32),
    /// UInt64 value
    UInt64(u64),
    /// UInt128 value
    UInt128(u128),
    /// UInt256 value
    UInt256(u256::U256),
    /// Int8 value
    Int8(i8),
    /// Int16 value
    Int16(i16),
    /// Int32 value
    Int32(i32),
    /// Int64 value
    Int64(i64),
    /// Int128 value
    Int128(i128),
    /// Int256 value
    Int256(i256::I256),
    /// Float32 value
    Float32(f32),
    /// Float64 value
    Float64(f64),
    /// String value
    String(String),
    /// FixedString value
    FixedString(Vec<u8>),
    /// Low cardinality string value
    LowCardinality(String),
    /// Date value
    Date(chrono::NaiveDate),
    /// DateTime value
    DateTime(chrono::NaiveDateTime),
    /// DateTime64 value
    DateTime64(chrono::NaiveDateTime),
    /// UUID value
    UUID(uuid::Uuid),
    /// Array value
    Array(Vec<Value>),
    /// Nullable value
    Nullable(Option<Box<Value>>),
    /// Tuple value
    Tuple(Vec<Value>),
    /// Map value
    Map(HashMap<String, Value>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::UInt8(v) => write!(f, "{}", v),
            Value::UInt16(v) => write!(f, "{}", v),
            Value::UInt32(v) => write!(f, "{}", v),
            Value::UInt64(v) => write!(f, "{}", v),
            Value::UInt128(v) => write!(f, "{}", v),
            Value::UInt256(v) => write!(f, "{:?}", v),
            Value::Int8(v) => write!(f, "{}", v),
            Value::Int16(v) => write!(f, "{}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Int128(v) => write!(f, "{}", v),
            Value::Int256(v) => write!(f, "{:?}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::FixedString(v) => write!(f, "{:?}", v),
            Value::LowCardinality(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v),
            Value::DateTime64(v) => write!(f, "{}", v),
            Value::Array(v) => {
                write!(f, "[")?;
                for (i, item) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            Value::Nullable(v) => match v {
                Some(val) => write!(f, "{}", **val),
                None => write!(f, "NULL"),
            },
            Value::Tuple(v) => {
                write!(f, "(")?;
                for (i, item) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }
            Value::Map(v) => {
                write!(f, "{{")?;
                for (i, (key, value)) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", key, value)?;
                }
                write!(f, "}}")
            }
            Value::UUID(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl Value {
    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Nullable(None))
    }

    /// Get the type name of the value
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Nullable(_) => "Nullable",
            Value::UInt8(_) => "UInt8",
            Value::UInt16(_) => "UInt16",
            Value::UInt32(_) => "UInt32",
            Value::UInt64(_) => "UInt64",
            Value::UInt128(_) => "UInt128",
            Value::UInt256(_) => "UInt256",
            Value::Int8(_) => "Int8",
            Value::Int16(_) => "Int16",
            Value::Int32(_) => "Int32",
            Value::Int64(_) => "Int64",
            Value::Int128(_) => "Int128",
            Value::Int256(_) => "Int256",
            Value::Float32(_) => "Float32",
            Value::Float64(_) => "Float64",
            Value::String(_) => "String",
            Value::FixedString(_) => "FixedString",
            Value::LowCardinality(_) => "LowCardinality",
            Value::Date(_) => "Date",
            Value::DateTime(_) => "DateTime",
            Value::DateTime64(_) => "DateTime64",
            Value::Array(_) => "Array",
            Value::Tuple(_) => "Tuple",
            Value::Map(_) => "Map",
            Value::UUID(_) => "UUID",
            Value::Null => "Null",

        }
    }
}

// Type aliases for convenience
pub type UInt8 = u8;
pub type UInt16 = u16;
pub type UInt32 = u32;
pub type UInt64 = u64;
pub type UInt128 = u128;
pub type UInt256 = u256::U256;

pub type Int8 = i8;
pub type Int16 = i16;
pub type Int32 = i32;
pub type Int64 = i64;
pub type Int128 = i128;
pub type Int256 = i256::I256;

pub type Float32 = f32;
pub type Float64 = f64;

pub type String = std::string::String;
pub type FixedString = Vec<u8>;
pub type LowCardinality = String;

pub type Date = chrono::NaiveDate;
pub type DateTime = chrono::NaiveDateTime;
pub type DateTime64 = chrono::NaiveDateTime;

pub type UUID = uuid::Uuid;

pub type Array<T> = Vec<T>;
pub type Nullable<T> = Option<T>;
pub type Tuple = Vec<Value>;
pub type Map = HashMap<String, Value>;

pub type Point = (f64, f64);
pub type Ring = Vec<Point>;
pub type Polygon = Vec<Ring>;
pub type MultiPolygon = Vec<Polygon>;
