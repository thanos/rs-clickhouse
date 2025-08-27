//! Decimal types for ClickHouse
//! 
//! Implements Decimal32, Decimal64, and Decimal128 types with proper
//! precision and scale handling for financial and scientific calculations.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Sub, Mul, Div, Neg};
use std::cmp::{PartialEq, PartialOrd, Ordering};

/// Decimal32 type with 32-bit precision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decimal32 {
    /// The underlying value (scaled by 10^scale)
    value: i32,
    /// The scale (number of decimal places)
    scale: u8,
}

/// Decimal64 type with 64-bit precision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decimal64 {
    /// The underlying value (scaled by 10^scale)
    value: i64,
    /// The scale (number of decimal places)
    scale: u8,
}

/// Decimal128 type with 128-bit precision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decimal128 {
    /// The underlying value (scaled by 10^scale)
    value: i128,
    /// The scale (number of decimal places)
    scale: u8,
}

impl Decimal32 {
    /// Create a new Decimal32 with the specified value and scale
    pub fn new(value: i32, scale: u8) -> Self {
        Self { value, scale }
    }

    /// Create a Decimal32 from a string representation
    pub fn from_str(s: &str, scale: u8) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 2 {
            return Err("Invalid decimal format: too many decimal points".to_string());
        }

        let integer_part = parts[0];
        let decimal_part = if parts.len() == 2 { parts[1] } else { "" };

        // Parse integer part
        let mut int_val: i32 = integer_part.parse()
            .map_err(|_| format!("Invalid integer part: {}", integer_part))?;

        // Handle negative numbers
        let is_negative = int_val < 0;
        if is_negative {
            int_val = int_val.abs();
        }

        // Scale the integer part
        let mut scaled_value = int_val * 10i32.pow(scale as u32);

        // Add decimal part if present
        if !decimal_part.is_empty() {
            if decimal_part.len() > scale as usize {
                return Err(format!("Decimal part too long for scale {}", scale));
            }
            
            let decimal_val: i32 = decimal_part.parse()
                .map_err(|_| format!("Invalid decimal part: {}", decimal_part))?;
            
            let padding = scale as usize - decimal_part.len();
            let decimal_scaled = decimal_val * 10i32.pow(padding as u32);
            scaled_value += decimal_scaled;
        }

        if is_negative {
            scaled_value = -scaled_value;
        }

        Ok(Self { value: scaled_value, scale })
    }

    /// Get the underlying scaled value
    pub fn value(&self) -> i32 {
        self.value
    }

    /// Get the scale
    pub fn scale(&self) -> u8 {
        self.scale
    }

    /// Convert to f64 (may lose precision)
    pub fn to_f64(&self) -> f64 {
        self.value as f64 / 10.0_f64.powi(self.scale as i32)
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        let abs_value = self.value.abs();
        let scale_factor = 10i32.pow(self.scale as u32);
        
        let integer_part = abs_value / scale_factor;
        let decimal_part = abs_value % scale_factor;
        
        let mut result = if self.value < 0 { "-".to_string() } else { String::new() };
        result.push_str(&integer_part.to_string());
        
        if self.scale > 0 {
            result.push('.');
            let decimal_str = format!("{:0width$}", decimal_part, width = self.scale as usize);
            result.push_str(&decimal_str);
        }
        
        result
    }

    /// Round to a new scale
    pub fn round_to_scale(&self, new_scale: u8) -> Result<Self, String> {
        if new_scale >= self.scale {
            return Err("New scale must be less than current scale".to_string());
        }

        let scale_diff = self.scale - new_scale;
        let scale_factor = 10i32.pow(scale_diff as u32);
        
        let rounded_value = if self.value >= 0 {
            (self.value + scale_factor / 2) / scale_factor
        } else {
            (self.value - scale_factor / 2) / scale_factor
        };

        Ok(Self { value: rounded_value, scale: new_scale })
    }
}

impl Decimal64 {
    /// Create a new Decimal64 with the specified value and scale
    pub fn new(value: i64, scale: u8) -> Self {
        Self { value, scale }
    }

    /// Create a Decimal64 from a string representation
    pub fn from_str(s: &str, scale: u8) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 2 {
            return Err("Invalid decimal format: too many decimal points".to_string());
        }

        let integer_part = parts[0];
        let decimal_part = if parts.len() == 2 { parts[1] } else { "" };

        // Parse integer part
        let mut int_val: i64 = integer_part.parse()
            .map_err(|_| format!("Invalid integer part: {}", integer_part))?;

        // Handle negative numbers
        let is_negative = int_val < 0;
        if is_negative {
            int_val = int_val.abs();
        }

        // Scale the integer part
        let mut scaled_value = int_val * 10i64.pow(scale as u32);

        // Add decimal part if present
        if !decimal_part.is_empty() {
            if decimal_part.len() > scale as usize {
                return Err(format!("Decimal part too long for scale {}", scale));
            }
            
            let decimal_val: i64 = decimal_part.parse()
                .map_err(|_| format!("Invalid decimal part: {}", decimal_part))?;
            
            let padding = scale as usize - decimal_part.len();
            let decimal_scaled = decimal_val * 10i64.pow(padding as u32);
            scaled_value += decimal_scaled;
        }

        if is_negative {
            scaled_value = -scaled_value;
        }

        Ok(Self { value: scaled_value, scale })
    }

    /// Get the underlying scaled value
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Get the scale
    pub fn scale(&self) -> u8 {
        self.scale
    }

    /// Convert to f64 (may lose precision)
    pub fn to_f64(&self) -> f64 {
        self.value as f64 / 10.0_f64.powi(self.scale as i32)
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        let abs_value = self.value.abs();
        let scale_factor = 10i64.pow(self.scale as u32);
        
        let integer_part = abs_value / scale_factor;
        let decimal_part = abs_value % scale_factor;
        
        let mut result = if self.value < 0 { "-".to_string() } else { String::new() };
        result.push_str(&integer_part.to_string());
        
        if self.scale > 0 {
            result.push('.');
            let decimal_str = format!("{:0width$}", decimal_part, width = self.scale as usize);
            result.push_str(&decimal_str);
        }
        
        result
    }

    /// Round to a new scale
    pub fn round_to_scale(&self, new_scale: u8) -> Result<Self, String> {
        if new_scale >= self.scale {
            return Err("New scale must be less than current scale".to_string());
        }

        let scale_diff = self.scale - new_scale;
        let scale_factor = 10i64.pow(scale_diff as u32);
        
        let rounded_value = if self.value >= 0 {
            (self.value + scale_factor / 2) / scale_factor
        } else {
            (self.value - scale_factor / 2) / scale_factor
        };

        Ok(Self { value: rounded_value, scale: new_scale })
    }
}

impl Decimal128 {
    /// Create a new Decimal128 with the specified value and scale
    pub fn new(value: i128, scale: u8) -> Self {
        Self { value, scale }
    }

    /// Create a Decimal128 from a string representation
    pub fn from_str(s: &str, scale: u8) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 2 {
            return Err("Invalid decimal format: too many decimal points".to_string());
        }

        let integer_part = parts[0];
        let decimal_part = if parts.len() == 2 { parts[1] } else { "" };

        // Parse integer part
        let mut int_val: i128 = integer_part.parse()
            .map_err(|_| format!("Invalid integer part: {}", integer_part))?;

        // Handle negative numbers
        let is_negative = int_val < 0;
        if is_negative {
            int_val = int_val.abs();
        }

        // Scale the integer part
        let mut scaled_value = int_val * 10i128.pow(scale as u32);

        // Add decimal part if present
        if !decimal_part.is_empty() {
            if decimal_part.len() > scale as usize {
                return Err(format!("Decimal part too long for scale {}", scale));
            }
            
            let decimal_val: i128 = decimal_part.parse()
                .map_err(|_| format!("Invalid decimal part: {}", decimal_part))?;
            
            let padding = scale as usize - decimal_part.len();
            let decimal_scaled = decimal_val * 10i128.pow(padding as u32);
            scaled_value += decimal_scaled;
        }

        if is_negative {
            scaled_value = -scaled_value;
        }

        Ok(Self { value: scaled_value, scale })
    }

    /// Get the underlying scaled value
    pub fn value(&self) -> i128 {
        self.value
    }

    /// Get the scale
    pub fn scale(&self) -> u8 {
        self.scale
    }

    /// Convert to f64 (may lose precision)
    pub fn to_f64(&self) -> f64 {
        self.value as f64 / 10.0_f64.powi(self.scale as i32)
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        let abs_value = self.value.abs();
        let scale_factor = 10i128.pow(self.scale as u32);
        
        let integer_part = abs_value / scale_factor;
        let decimal_part = abs_value % scale_factor;
        
        let mut result = if self.value < 0 { "-".to_string() } else { String::new() };
        result.push_str(&integer_part.to_string());
        
        if self.scale > 0 {
            result.push('.');
            let decimal_str = format!("{:0width$}", decimal_part, width = self.scale as usize);
            result.push_str(&decimal_str);
        }
        
        result
    }

    /// Round to a new scale
    pub fn round_to_scale(&self, new_scale: u8) -> Result<Self, String> {
        if new_scale >= self.scale {
            return Err("New scale must be less than current scale".to_string());
        }

        let scale_diff = self.scale - new_scale;
        let scale_factor = 10i128.pow(scale_diff as u32);
        
        let rounded_value = if self.value >= 0 {
            (self.value + scale_factor / 2) / scale_factor
        } else {
            (self.value - scale_factor / 2) / scale_factor
        };

        Ok(Self { value: rounded_value, scale: new_scale })
    }
}

// Implement Display for all decimal types
impl fmt::Display for Decimal32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl fmt::Display for Decimal64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl fmt::Display for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Implement PartialEq for all decimal types
impl PartialEq for Decimal32 {
    fn eq(&self, other: &Self) -> bool {
        // Normalize to same scale for comparison
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i32.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i32.pow((max_scale - other.scale) as u32);
        self_normalized == other_normalized
    }
}

impl PartialEq for Decimal64 {
    fn eq(&self, other: &Self) -> bool {
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i64.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i64.pow((max_scale - other.scale) as u32);
        self_normalized == other_normalized
    }
}

impl PartialEq for Decimal128 {
    fn eq(&self, other: &Self) -> bool {
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i128.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i128.pow((max_scale - other.scale) as u32);
        self_normalized == other_normalized
    }
}

// Implement PartialOrd for all decimal types
impl PartialOrd for Decimal32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i32.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i32.pow((max_scale - other.scale) as u32);
        Some(self_normalized.cmp(&other_normalized))
    }
}

impl PartialOrd for Decimal64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i64.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i64.pow((max_scale - other.scale) as u32);
        Some(self_normalized.cmp(&other_normalized))
    }
}

impl PartialOrd for Decimal128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i128.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i128.pow((max_scale - other.scale) as u32);
        Some(self_normalized.cmp(&other_normalized))
    }
}

// Implement arithmetic operations for Decimal64 (most commonly used)
impl Add for Decimal64 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i64.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i64.pow((max_scale - other.scale) as u32);
        
        Self {
            value: self_normalized + other_normalized,
            scale: max_scale,
        }
    }
}

impl Sub for Decimal64 {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        let max_scale = std::cmp::max(self.scale, other.scale);
        let self_normalized = self.value * 10i64.pow((max_scale - self.scale) as u32);
        let other_normalized = other.value * 10i64.pow((max_scale - other.scale) as u32);
        
        Self {
            value: self_normalized - other_normalized,
            scale: max_scale,
        }
    }
}

impl Neg for Decimal64 {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self {
            value: -self.value,
            scale: self.scale,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal32_creation() {
        let dec = Decimal32::new(12345, 2);
        assert_eq!(dec.value(), 12345);
        assert_eq!(dec.scale(), 2);
        assert_eq!(dec.to_string(), "123.45");
    }

    #[test]
    fn test_decimal32_from_str() {
        let dec = Decimal32::from_str("123.45", 2).unwrap();
        assert_eq!(dec.value(), 12345);
        assert_eq!(dec.scale(), 2);
        
        let dec2 = Decimal32::from_str("-67.89", 2).unwrap();
        assert_eq!(dec2.value(), -6789);
        assert_eq!(dec2.to_string(), "-67.89");
    }

    #[test]
    fn test_decimal32_from_str_invalid() {
        let result = Decimal32::from_str("123.456", 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too long"));
    }

    #[test]
    fn test_decimal64_creation() {
        let dec = Decimal64::new(123456789, 3);
        assert_eq!(dec.value(), 123456789);
        assert_eq!(dec.scale(), 3);
        assert_eq!(dec.to_string(), "123456.789");
    }

    #[test]
    fn test_decimal64_from_str() {
        let dec = Decimal64::from_str("123456.789", 3).unwrap();
        assert_eq!(dec.value(), 123456789);
        assert_eq!(dec.scale(), 3);
    }

    #[test]
    fn test_decimal128_creation() {
        let dec = Decimal128::new(1234567890123456789, 6);
        assert_eq!(dec.value(), 1234567890123456789);
        assert_eq!(dec.scale(), 6);
        assert_eq!(dec.to_string(), "1234567890123.456789");
    }

    #[test]
    fn test_decimal_equality() {
        let dec1 = Decimal32::new(12345, 2);
        let dec2 = Decimal32::new(123450, 3);
        assert_eq!(dec1, dec2);
        
        let dec3 = Decimal32::new(12346, 2);
        assert_ne!(dec1, dec3);
    }

    #[test]
    fn test_decimal_comparison() {
        let dec1 = Decimal64::new(12345, 2);
        let dec2 = Decimal64::new(12346, 2);
        assert!(dec1 < dec2);
        assert!(dec2 > dec1);
    }

    #[test]
    fn test_decimal_arithmetic() {
        let dec1 = Decimal64::new(12345, 2);
        let dec2 = Decimal64::new(67890, 2);
        
        let sum = dec1.clone() + dec2.clone();
        assert_eq!(sum.to_string(), "802.35");
        
        let diff = dec2.clone() - dec1.clone();
        assert_eq!(diff.to_string(), "555.45");
        
        let neg = -dec1;
        assert_eq!(neg.to_string(), "-123.45");
    }

    #[test]
    fn test_decimal_rounding() {
        let dec = Decimal64::new(123456, 3);
        let rounded = dec.round_to_scale(1).unwrap();
        assert_eq!(rounded.to_string(), "123.5");
        assert_eq!(rounded.scale(), 1);
    }

    #[test]
    fn test_decimal_conversion() {
        let dec = Decimal32::new(12345, 2);
        let f64_val = dec.to_f64();
        assert!((f64_val - 123.45).abs() < 0.001);
    }
}
