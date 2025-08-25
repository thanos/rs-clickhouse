//! Numeric data types for ClickHouse

use super::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// UInt8 type (0 to 255)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UInt8(pub u8);

/// UInt16 type (0 to 65,535)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UInt16(pub u16);

/// UInt32 type (0 to 4,294,967,295)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UInt32(pub u32);

/// UInt64 type (0 to 18,446,744,073,709,551,615)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UInt64(pub u64);

/// UInt128 type (0 to 2^128 - 1)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UInt128(pub u128);

/// UInt256 type (0 to 2^256 - 1)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UInt256(pub u256::U256);

/// Int8 type (-128 to 127)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Int8(pub i8);

/// Int16 type (-32,768 to 32,767)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Int16(pub i16);

/// Int32 type (-2,147,483,648 to 2,147,483,647)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Int32(pub i32);

/// Int64 type (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Int64(pub i64);

/// Int128 type (-2^127 to 2^127 - 1)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Int128(pub i128);

/// Int256 type (-2^255 to 2^255 - 1)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Int256(pub i256::I256);

/// Float32 type (32-bit floating point)
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Float32(pub f32);

/// Float64 type (64-bit floating point)
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Float64(pub f64);

// Implement Display for all numeric types
impl fmt::Display for UInt8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for UInt16 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for UInt32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for UInt64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for UInt128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for UInt256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl fmt::Display for Int8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Int16 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Int32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Int64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Int128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Int256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Float32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Float64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement From traits for conversions
impl From<u8> for UInt8 {
    fn from(value: u8) -> Self {
        UInt8(value)
    }
}

impl From<u16> for UInt16 {
    fn from(value: u16) -> Self {
        UInt16(value)
    }
}

impl From<u32> for UInt32 {
    fn from(value: u32) -> Self {
        UInt32(value)
    }
}

impl From<u64> for UInt64 {
    fn from(value: u64) -> Self {
        UInt64(value)
    }
}

impl From<u128> for UInt128 {
    fn from(value: u128) -> Self {
        UInt128(value)
    }
}

impl From<u256::U256> for UInt256 {
    fn from(value: u256::U256) -> Self {
        UInt256(value)
    }
}

impl From<i8> for Int8 {
    fn from(value: i8) -> Self {
        Int8(value)
    }
}

impl From<i16> for Int16 {
    fn from(value: i16) -> Self {
        Int16(value)
    }
}

impl From<i32> for Int32 {
    fn from(value: i32) -> Self {
        Int32(value)
    }
}

impl From<i64> for Int64 {
    fn from(value: i64) -> Self {
        Int64(value)
    }
}

impl From<i128> for Int128 {
    fn from(value: i128) -> Self {
        Int128(value)
    }
}

impl From<i256::I256> for Int256 {
    fn from(value: i256::I256) -> Self {
        Int256(value)
    }
}

impl From<f32> for Float32 {
    fn from(value: f32) -> Self {
        Float32(value)
    }
}

impl From<f64> for Float64 {
    fn from(value: f64) -> Self {
        Float64(value)
    }
}

// Implement TryFrom for Value conversions
impl TryFrom<Value> for UInt8 {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UInt8(v) => Ok(UInt8(v)),
            Value::UInt16(v) => Ok(UInt8(v as u8)),
            Value::UInt32(v) => Ok(UInt8(v as u8)),
            Value::UInt64(v) => Ok(UInt8(v as u8)),
            Value::Int8(v) => {
                if v >= 0 {
                    Ok(UInt8(v as u8))
                } else {
                    Err("Negative value cannot be converted to UInt8".to_string())
                }
            }
            Value::Int16(v) => {
                if v >= 0 && v <= 255 {
                    Ok(UInt8(v as u8))
                } else {
                    Err("Value out of range for UInt8".to_string())
                }
            }
            Value::Int32(v) => {
                if v >= 0 && v <= 255 {
                    Ok(UInt8(v as u8))
                } else {
                    Err("Value out of range for UInt8".to_string())
                }
            }
            Value::Int64(v) => {
                if v >= 0 && v <= 255 {
                    Ok(UInt8(v as u8))
                } else {
                    Err("Value out of range for UInt8".to_string())
                }
            }
            Value::Float32(v) => {
                if v >= 0.0 && v <= 255.0 && v.fract() == 0.0 {
                    Ok(UInt8(v as u8))
                } else {
                    Err("Float value cannot be converted to UInt8".to_string())
                }
            }
            Value::Float64(v) => {
                if v >= 0.0 && v <= 255.0 && v.fract() == 0.0 {
                    Ok(UInt8(v as u8))
                } else {
                    Err("Float value cannot be converted to UInt8".to_string())
                }
            }
            _ => Err(format!("Cannot convert {} to UInt8", value.type_name())),
        }
    }
}

impl TryFrom<Value> for UInt64 {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UInt8(v) => Ok(UInt64(v as u64)),
            Value::UInt16(v) => Ok(UInt64(v as u64)),
            Value::UInt32(v) => Ok(UInt64(v as u64)),
            Value::UInt64(v) => Ok(UInt64(v)),
            Value::UInt128(v) => Ok(UInt64(v as u64)),
            Value::Int8(v) => {
                if v >= 0 {
                    Ok(UInt64(v as u64))
                } else {
                    Err("Negative value cannot be converted to UInt64".to_string())
                }
            }
            Value::Int16(v) => {
                if v >= 0 {
                    Ok(UInt64(v as u64))
                } else {
                    Err("Negative value cannot be converted to UInt64".to_string())
                }
            }
            Value::Int32(v) => {
                if v >= 0 {
                    Ok(UInt64(v as u64))
                } else {
                    Err("Negative value cannot be converted to UInt64".to_string())
                }
            }
            Value::Int64(v) => {
                if v >= 0 {
                    Ok(UInt64(v as u64))
                } else {
                    Err("Negative value cannot be converted to UInt64".to_string())
                }
            }
            Value::Float32(v) => {
                if v >= 0.0 && v.fract() == 0.0 {
                    Ok(UInt64(v as u64))
                } else {
                    Err("Float value cannot be converted to UInt64".to_string())
                }
            }
            Value::Float64(v) => {
                if v >= 0.0 && v.fract() == 0.0 {
                    Ok(UInt64(v as u64))
                } else {
                    Err("Float value cannot be converted to UInt64".to_string())
                }
            }
            _ => Err(format!("Cannot convert {} to UInt64", value.type_name())),
        }
    }
}

impl TryFrom<Value> for Int64 {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UInt8(v) => Ok(Int64(v as i64)),
            Value::UInt16(v) => Ok(Int64(v as i64)),
            Value::UInt32(v) => Ok(Int64(v as i64)),
            Value::UInt64(v) => Ok(Int64(v as i64)),
            Value::Int8(v) => Ok(Int64(v as i64)),
            Value::Int16(v) => Ok(Int64(v as i64)),
            Value::Int32(v) => Ok(Int64(v as i64)),
            Value::Int64(v) => Ok(Int64(v)),
            Value::Float32(v) => {
                if v.fract() == 0.0 && v >= i64::MIN as f32 && v <= i64::MAX as f32 {
                    Ok(Int64(v as i64))
                } else {
                    Err("Float value cannot be converted to Int64".to_string())
                }
            }
            Value::Float64(v) => {
                if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
                    Ok(Int64(v as i64))
                } else {
                    Err("Float value cannot be converted to Int64".to_string())
                }
            }
            _ => Err(format!("Cannot convert {} to Int64", value.type_name())),
        }
    }
}

impl TryFrom<Value> for Float64 {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UInt8(v) => Ok(Float64(v as f64)),
            Value::UInt16(v) => Ok(Float64(v as f64)),
            Value::UInt32(v) => Ok(Float64(v as f64)),
            Value::UInt64(v) => Ok(Float64(v as f64)),
            Value::UInt128(v) => Ok(Float64(v as f64)),
            Value::Int8(v) => Ok(Float64(v as f64)),
            Value::Int16(v) => Ok(Float64(v as f64)),
            Value::Int32(v) => Ok(Float64(v as f64)),
            Value::Int64(v) => Ok(Float64(v as f64)),
            Value::Int128(v) => Ok(Float64(v as f64)),
            Value::Float32(v) => Ok(Float64(v as f64)),
            Value::Float64(v) => Ok(Float64(v)),
            _ => Err(format!("Cannot convert {} to Float64", value.type_name())),
        }
    }
}

// Implement arithmetic operations
impl std::ops::Add for UInt8 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        UInt8(self.0 + other.0)
    }
}

impl std::ops::Add for UInt64 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        UInt64(self.0 + other.0)
    }
}

impl std::ops::Add for Int64 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Int64(self.0 + other.0)
    }
}

impl std::ops::Add for Float64 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Float64(self.0 + other.0)
    }
}

impl std::ops::Sub for UInt8 {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        UInt8(self.0.saturating_sub(other.0))
    }
}

impl std::ops::Sub for UInt64 {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        UInt64(self.0.saturating_sub(other.0))
    }
}

impl std::ops::Sub for Int64 {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Int64(self.0 - other.0)
    }
}

impl std::ops::Sub for Float64 {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Float64(self.0 - other.0)
    }
}

impl std::ops::Mul for UInt8 {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        UInt8(self.0 * other.0)
    }
}

impl std::ops::Mul for UInt64 {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        UInt64(self.0 * other.0)
    }
}

impl std::ops::Mul for Int64 {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Int64(self.0 * other.0)
    }
}

impl std::ops::Mul for Float64 {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Float64(self.0 * other.0)
    }
}

impl std::ops::Div for UInt8 {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        UInt8(self.0 / other.0)
    }
}

impl std::ops::Div for UInt64 {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        UInt64(self.0 / other.0)
    }
}

impl std::ops::Div for Int64 {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Int64(self.0 / other.0)
    }
}

impl std::ops::Div for Float64 {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Float64(self.0 / other.0)
    }
}

// Implement comparison traits
impl PartialEq<u8> for UInt8 {
    fn eq(&self, other: &u8) -> bool {
        self.0 == *other
    }
}

impl PartialEq<u64> for UInt64 {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<i64> for Int64 {
    fn eq(&self, other: &i64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<f64> for Float64 {
    fn eq(&self, other: &f64) -> bool {
        self.0 == *other
    }
}

// Implement Default traits
impl Default for UInt8 {
    fn default() -> Self {
        UInt8(0)
    }
}

impl Default for UInt16 {
    fn default() -> Self {
        UInt16(0)
    }
}

impl Default for UInt32 {
    fn default() -> Self {
        UInt32(0)
    }
}

impl Default for UInt64 {
    fn default() -> Self {
        UInt64(0)
    }
}

impl Default for UInt128 {
    fn default() -> Self {
        UInt128(0)
    }
}

impl Default for UInt256 {
    fn default() -> Self {
        UInt256(u256::U256::zero())
    }
}

impl Default for Int8 {
    fn default() -> Self {
        Int8(0)
    }
}

impl Default for Int16 {
    fn default() -> Self {
        Int16(0)
    }
}

impl Default for Int32 {
    fn default() -> Self {
        Int32(0)
    }
}

impl Default for Int64 {
    fn default() -> Self {
        Int64(0)
    }
}

impl Default for Int128 {
    fn default() -> Self {
        Int128(0)
    }
}

impl Default for Int256 {
    fn default() -> Self {
        Int256(i256::I256::min_value())
    }
}

impl Default for Float32 {
    fn default() -> Self {
        Float32(0.0)
    }
}

impl Default for Float64 {
    fn default() -> Self {
        Float64(0.0)
    }
}
