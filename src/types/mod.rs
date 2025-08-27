//! ClickHouse data types and core structures

mod numeric;
mod string;
mod datetime;
mod complex;
mod geometric;
mod lowcardinality;
mod network;
mod fixed_string;
mod enum_types;
mod decimal;


pub use numeric::*;
pub use string::*;
pub use datetime::*;
pub use complex::*;
pub use geometric::*;
pub use lowcardinality::*;
pub use network::*;
pub use fixed_string::*;
pub use enum_types::*;
pub use decimal::*;

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
#[derive(Debug, Clone)]
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
    FixedString(Vec<fixed_string::FixedString>),
    /// LowCardinality values
    LowCardinality(lowcardinality::LowCardinality<String>),
    /// Date values
    Date(Vec<chrono::NaiveDate>),
    /// DateTime values
    DateTime(Vec<chrono::NaiveDateTime>),
    /// DateTime64 values
    DateTime64(Vec<chrono::NaiveDateTime>),
    /// UUID values
    UUID(Vec<uuid::Uuid>),
    /// IPv4 values
    IPv4(Vec<network::IPv4>),
    /// IPv6 values
    IPv6(Vec<network::IPv6>),
    /// Decimal32 values
    Decimal32(Vec<decimal::Decimal32>),
    /// Decimal64 values
    Decimal64(Vec<decimal::Decimal64>),
    /// Decimal128 values
    Decimal128(Vec<decimal::Decimal128>),
    /// Enum8 values
    Enum8(Vec<enum_types::Enum8>),
    /// Enum16 values
    Enum16(Vec<enum_types::Enum16>),
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
            ColumnData::IPv4(v) => v.len(),
            ColumnData::IPv6(v) => v.len(),
            ColumnData::Decimal32(v) => v.len(),
            ColumnData::Decimal64(v) => v.len(),
            ColumnData::Decimal128(v) => v.len(),
            ColumnData::Enum8(v) => v.len(),
            ColumnData::Enum16(v) => v.len(),
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
            ColumnData::LowCardinality(v) => Some(Value::String(v.get(index).map_or("", |v| v).to_string())),
            ColumnData::Date(v) => Some(Value::Date(v[index])),
            ColumnData::DateTime(v) => Some(Value::DateTime(v[index])),
            ColumnData::DateTime64(v) => Some(Value::DateTime64(v[index])),
            ColumnData::UUID(v) => Some(Value::UUID(v[index])),
            ColumnData::IPv4(v) => Some(Value::IPv4(v[index].clone())),
            ColumnData::IPv6(v) => Some(Value::IPv6(v[index].clone())),
            ColumnData::Decimal32(v) => Some(Value::Decimal32(v[index].clone())),
            ColumnData::Decimal64(v) => Some(Value::Decimal64(v[index].clone())),
            ColumnData::Decimal128(v) => Some(Value::Decimal128(v[index].clone())),
            ColumnData::Enum8(v) => Some(Value::Enum8(v[index].clone())),
            ColumnData::Enum16(v) => Some(Value::Enum16(v[index].clone())),
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
            (ColumnData::LowCardinality(v), Value::LowCardinality(val)) => {
                // For LowCardinality, we need to handle this differently since it's not a simple vector
                // This is a simplified approach - in a real implementation, you'd want to update the existing index
                // For now, we'll just ignore the set operation since LowCardinality doesn't support direct indexing
            },
            (ColumnData::Date(v), Value::Date(val)) => v[index] = val,
            (ColumnData::DateTime(v), Value::DateTime(val)) => v[index] = val,
            (ColumnData::DateTime64(v), Value::DateTime64(val)) => v[index] = val,
            (ColumnData::UUID(v), Value::UUID(val)) => v[index] = val,
            (ColumnData::IPv4(v), Value::IPv4(val)) => v[index] = val,
            (ColumnData::IPv6(v), Value::IPv6(val)) => v[index] = val,
            (ColumnData::Decimal32(v), Value::Decimal32(val)) => v[index] = val,
            (ColumnData::Decimal64(v), Value::Decimal64(val)) => v[index] = val,
            (ColumnData::Decimal128(v), Value::Decimal128(val)) => v[index] = val,
            (ColumnData::Enum8(v), Value::Enum8(val)) => v[index] = val,
            (ColumnData::Enum16(v), Value::Enum16(val)) => v[index] = val,
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
            (ColumnData::LowCardinality(v), Value::String(val)) => v.push(val),
            (ColumnData::Date(v), Value::Date(val)) => v.push(val),
            (ColumnData::DateTime(v), Value::DateTime(val)) => v.push(val),
            (ColumnData::DateTime64(v), Value::DateTime64(val)) => v.push(val),
            (ColumnData::UUID(v), Value::UUID(val)) => v.push(val),
            (ColumnData::IPv4(v), Value::IPv4(val)) => v.push(val),
            (ColumnData::IPv6(v), Value::IPv6(val)) => v.push(val),
            (ColumnData::Decimal32(v), Value::Decimal32(val)) => v.push(val),
            (ColumnData::Decimal64(v), Value::Decimal64(val)) => v.push(val),
            (ColumnData::Decimal128(v), Value::Decimal128(val)) => v.push(val),
            (ColumnData::Enum8(v), Value::Enum8(val)) => v.push(val),
            (ColumnData::Enum16(v), Value::Enum16(val)) => v.push(val),
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
    FixedString(fixed_string::FixedString),
    /// Low cardinality string value
    LowCardinality(lowcardinality::LowCardinality<String>),
    /// Date value
    Date(chrono::NaiveDate),
    /// DateTime value
    DateTime(chrono::NaiveDateTime),
    /// DateTime64 value
    DateTime64(chrono::NaiveDateTime),
    /// UUID value
    UUID(uuid::Uuid),
    /// IPv4 value
    IPv4(network::IPv4),
    /// IPv6 value
    IPv6(network::IPv6),
    /// Decimal32 value
    Decimal32(decimal::Decimal32),
    /// Decimal64 value
    Decimal64(decimal::Decimal64),
    /// Decimal128 value
    Decimal128(decimal::Decimal128),
    /// Enum8 value
    Enum8(enum_types::Enum8),
    /// Enum16 value
    Enum16(enum_types::Enum16),
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
            Value::LowCardinality(v) => write!(f, "{:?}", v),
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
            Value::IPv4(v) => write!(f, "{}", v),
            Value::IPv6(v) => write!(f, "{}", v),
            Value::Decimal32(v) => write!(f, "{}", v),
            Value::Decimal64(v) => write!(f, "{}", v),
            Value::Decimal128(v) => write!(f, "{}", v),
            Value::Enum8(v) => write!(f, "{}", v),
            Value::Enum16(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

// Implement From for basic types to enable automatic conversion
impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Value::UInt8(value)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Value::UInt16(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::UInt32(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::UInt64(value)
    }
}

impl From<u128> for Value {
    fn from(value: u128) -> Self {
        Value::UInt128(value)
    }
}

impl From<u256::U256> for Value {
    fn from(value: u256::U256) -> Self {
        Value::UInt256(value)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Value::Int8(value)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Value::Int16(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<i128> for Value {
    fn from(value: i128) -> Self {
        Value::Int128(value)
    }
}

impl From<i256::I256> for Value {
    fn from(value: i256::I256) -> Self {
        Value::Int256(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float32(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float64(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::FixedString(fixed_string::FixedString::from(value))
    }
}

impl From<chrono::NaiveDate> for Value {
    fn from(value: chrono::NaiveDate) -> Self {
        Value::Date(value)
    }
}

impl From<chrono::NaiveDateTime> for Value {
    fn from(value: chrono::NaiveDateTime) -> Self {
        Value::DateTime(value)
    }
}

impl From<uuid::Uuid> for Value {
    fn from(value: uuid::Uuid) -> Self {
        Value::UUID(value)
    }
}

impl From<network::IPv4> for Value {
    fn from(value: network::IPv4) -> Self {
        Value::IPv4(value)
    }
}

impl From<network::IPv6> for Value {
    fn from(value: network::IPv6) -> Self {
        Value::IPv6(value)
    }
}

impl From<decimal::Decimal32> for Value {
    fn from(value: decimal::Decimal32) -> Self {
        Value::Decimal32(value)
    }
}

impl From<decimal::Decimal64> for Value {
    fn from(value: decimal::Decimal64) -> Self {
        Value::Decimal64(value)
    }
}

impl From<decimal::Decimal128> for Value {
    fn from(value: decimal::Decimal128) -> Self {
        Value::Decimal128(value)
    }
}

impl From<enum_types::Enum8> for Value {
    fn from(value: enum_types::Enum8) -> Self {
        Value::Enum8(value)
    }
}

impl From<enum_types::Enum16> for Value {
    fn from(value: enum_types::Enum16) -> Self {
        Value::Enum16(value)
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Value::Array(value)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(value: HashMap<String, Value>) -> Self {
        Value::Map(value)
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
            Value::IPv4(_) => "IPv4",
            Value::IPv6(_) => "IPv6",
            Value::Decimal32(_) => "Decimal32",
            Value::Decimal64(_) => "Decimal64",
            Value::Decimal128(_) => "Decimal128",
            Value::Enum8(_) => "Enum8",
            Value::Enum16(_) => "Enum16",
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
pub type FixedString = fixed_string::FixedString;

pub type Date = chrono::NaiveDate;
pub type DateTime = chrono::NaiveDateTime;
pub type DateTime64 = chrono::NaiveDateTime;

pub type UUID = uuid::Uuid;
pub type IPv4 = network::IPv4;
pub type IPv6 = network::IPv6;
pub type Decimal32 = decimal::Decimal32;
pub type Decimal64 = decimal::Decimal64;
pub type Decimal128 = decimal::Decimal128;
pub type Enum8 = enum_types::Enum8;
pub type Enum16 = enum_types::Enum16;

pub type Array<T> = Vec<T>;
pub type Nullable<T> = Option<T>;
pub type Tuple = Vec<Value>;
pub type Map = HashMap<String, Value>;

pub type Point = (f64, f64);
pub type Ring = Vec<Point>;
pub type Polygon = Vec<Ring>;
pub type MultiPolygon = Vec<Polygon>;
