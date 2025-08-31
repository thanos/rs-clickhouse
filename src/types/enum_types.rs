//! Enum types for ClickHouse
//! 
//! Implements Enum8 and Enum16 types that store named constant values
//! with efficient storage and validation.

use std::collections::HashMap;
use std::fmt;

/// Enum8 type that stores 8-bit named constants
#[derive(Debug, Clone, PartialEq)]
pub struct Enum8 {
    /// The underlying 8-bit value
    value: i8,
    /// Reference to the enum definition
    definition: EnumDefinition,
}

/// Enum16 type that stores 16-bit named constants
#[derive(Debug, Clone, PartialEq)]
pub struct Enum16 {
    /// The underlying 16-bit value
    value: i16,
    /// Reference to the enum definition
    definition: EnumDefinition,
}

/// Definition of an enum with its name-value mappings
#[derive(Debug, Clone, PartialEq)]
pub struct EnumDefinition {
    /// Name of the enum
    pub name: String,
    /// Mapping from names to values
    pub values: HashMap<String, i16>,
    /// Reverse mapping from values to names
    pub names: HashMap<i16, String>,
}

impl EnumDefinition {
    /// Create a new enum definition
    pub fn new(name: String) -> Self {
        Self {
            name,
            values: HashMap::new(),
            names: HashMap::new(),
        }
    }

    /// Add a name-value pair to the enum
    pub fn add_value(&mut self, name: String, value: i16) -> Result<(), String> {
        if self.values.contains_key(&name) {
            return Err(format!("Enum value '{}' already exists", name));
        }
        if self.names.contains_key(&value) {
            return Err(format!("Enum value {} already exists", value));
        }
        
        self.values.insert(name.clone(), value);
        self.names.insert(value, name);
        Ok(())
    }

    /// Get the value for a given name
    pub fn get_value(&self, name: &str) -> Option<i16> {
        self.values.get(name).copied()
    }

    /// Get the name for a given value
    pub fn get_name(&self, value: i16) -> Option<&String> {
        self.names.get(&value)
    }

    /// Check if a name exists in the enum
    pub fn has_name(&self, name: &str) -> bool {
        self.values.contains_key(name)
    }

    /// Check if a value exists in the enum
    pub fn has_value(&self, value: i16) -> bool {
        self.names.contains_key(&value)
    }

    /// Get all names in the enum
    pub fn names(&self) -> Vec<&String> {
        self.values.keys().collect()
    }

    /// Get all values in the enum
    pub fn values(&self) -> Vec<i16> {
        self.names.keys().copied().collect()
    }

    /// Get the number of enum values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the enum is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Enum8 {
    /// Create a new Enum8 value
    pub fn new(value: i8, definition: EnumDefinition) -> Result<Self, String> {
        if !definition.has_value(value as i16) {
            return Err(format!("Value {} is not valid for enum {}", value, definition.name));
        }
        Ok(Self { value, definition })
    }

    /// Create an Enum8 from a name
    pub fn from_name(name: &str, definition: EnumDefinition) -> Result<Self, String> {
        let value = definition.get_value(name)
            .ok_or_else(|| format!("Name '{}' not found in enum {}", name, definition.name))?;
        
        if value > i8::MAX as i16 || value < i8::MIN as i16 {
            return Err(format!("Value {} is out of range for Enum8", value));
        }
        
        Ok(Self { value: value as i8, definition })
    }

    /// Get the underlying value
    pub fn value(&self) -> i8 {
        self.value
    }

    /// Get the name for this value
    pub fn name(&self) -> Option<&String> {
        self.definition.get_name(self.value as i16)
    }

    /// Get the enum definition
    pub fn definition(&self) -> &EnumDefinition {
        &self.definition
    }
}

impl Enum16 {
    /// Create a new Enum16 value
    pub fn new(value: i16, definition: EnumDefinition) -> Result<Self, String> {
        if !definition.has_value(value) {
            return Err(format!("Value {} is not valid for enum {}", value, definition.name));
        }
        Ok(Self { value, definition })
    }

    /// Create an Enum16 from a name
    pub fn from_name(name: &str, definition: EnumDefinition) -> Result<Self, String> {
        let value = definition.get_value(name)
            .ok_or_else(|| format!("Name '{}' not found in enum {}", name, definition.name))?;
        
        Ok(Self { value, definition })
    }

    /// Get the underlying value
    pub fn value(&self) -> i16 {
        self.value
    }

    /// Get the name for this value
    pub fn name(&self) -> Option<&String> {
        self.definition.get_name(self.value)
    }

    /// Get the enum definition
    pub fn definition(&self) -> &EnumDefinition {
        &self.definition
    }
}

impl fmt::Display for Enum8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = self.name() {
            write!(f, "{}", name)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

impl fmt::Display for Enum16 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = self.name() {
            write!(f, "{}", name)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

impl From<i8> for Enum8 {
    fn from(value: i8) -> Self {
        // Use a default definition for conversion
        let mut def = EnumDefinition::new(String::new());
        def.add_value("Default".to_string(), value as i16).unwrap_or_default();
        Self { value, definition: def }
    }
}

impl From<i16> for Enum16 {
    fn from(value: i16) -> Self {
        // Use a default definition for conversion
        let mut def = EnumDefinition::new(String::new());
        def.add_value("Default".to_string(), value).unwrap_or_default();
        Self { value, definition: def }
    }
}

impl From<Enum8> for i8 {
    fn from(e: Enum8) -> Self {
        e.value
    }
}

impl From<Enum16> for i16 {
    fn from(e: Enum16) -> Self {
        e.value
    }
}

impl PartialEq<i8> for Enum8 {
    fn eq(&self, other: &i8) -> bool {
        self.value == *other
    }
}

impl PartialEq<i16> for Enum16 {
    fn eq(&self, other: &i16) -> bool {
        self.value == *other
    }
}

impl PartialEq<Enum8> for i8 {
    fn eq(&self, other: &Enum8) -> bool {
        *self == other.value
    }
}

impl PartialEq<Enum16> for i16 {
    fn eq(&self, other: &Enum16) -> bool {
        *self == other.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_enum() -> EnumDefinition {
        let mut def = EnumDefinition::new("TestEnum".to_string());
        def.add_value("Zero".to_string(), 0).unwrap();
        def.add_value("One".to_string(), 1).unwrap();
        def.add_value("Two".to_string(), 2).unwrap();
        def.add_value("MinusOne".to_string(), -1).unwrap();
        def
    }

    #[test]
    fn test_enum_definition_creation() {
        let mut def = EnumDefinition::new("Test".to_string());
        assert_eq!(def.name, "Test");
        assert!(def.is_empty());
        
        def.add_value("First".to_string(), 1).unwrap();
        assert_eq!(def.len(), 1);
        assert!(def.has_name("First"));
        assert!(def.has_value(1));
    }

    #[test]
    fn test_enum_definition_duplicate_name() {
        let mut def = create_test_enum();
        let result = def.add_value("One".to_string(), 5);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_enum_definition_duplicate_value() {
        let mut def = create_test_enum();
        let result = def.add_value("Five".to_string(), 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_enum8_creation() {
        let def = create_test_enum();
        let enum_val = Enum8::new(1, def).unwrap();
        assert_eq!(enum_val.value(), 1);
        assert_eq!(enum_val.name(), Some(&"One".to_string()));
    }

    #[test]
    fn test_enum8_from_name() {
        let def = create_test_enum();
        let enum_val = Enum8::from_name("Two", def).unwrap();
        assert_eq!(enum_val.value(), 2);
        assert_eq!(enum_val.name(), Some(&"Two".to_string()));
    }

    #[test]
    fn test_enum8_invalid_value() {
        let def = create_test_enum();
        let result = Enum8::new(5, def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not valid"));
    }

    #[test]
    fn test_enum8_invalid_name() {
        let def = create_test_enum();
        let result = Enum8::from_name("Invalid", def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_enum16_creation() {
        let def = create_test_enum();
        let enum_val = Enum16::new(-1, def).unwrap();
        assert_eq!(enum_val.value(), -1);
        assert_eq!(enum_val.name(), Some(&"MinusOne".to_string()));
    }

    #[test]
    fn test_enum16_from_name() {
        let def = create_test_enum();
        let enum_val = Enum16::from_name("Zero", def).unwrap();
        assert_eq!(enum_val.value(), 0);
        assert_eq!(enum_val.name(), Some(&"Zero".to_string()));
    }

    #[test]
    fn test_enum_display() {
        let def = create_test_enum();
        let enum_val = Enum8::new(1, def.clone()).unwrap();
        assert_eq!(enum_val.to_string(), "One");
        
        let enum_val2 = Enum16::new(2, def).unwrap();
        assert_eq!(enum_val2.to_string(), "Two");
    }

    #[test]
    fn test_enum_conversions() {
        let def = create_test_enum();
        let enum_val = Enum8::new(1, def.clone()).unwrap();
        let int_val: i8 = enum_val.into();
        assert_eq!(int_val, 1);
        
        let enum_val2 = Enum16::new(2, def).unwrap();
        let int_val2: i16 = enum_val2.into();
        assert_eq!(int_val2, 2);
    }

    #[test]
    fn test_enum_comparisons() {
        let def = create_test_enum();
        let enum_val = Enum8::new(1, def).unwrap();
        
        assert_eq!(enum_val, 1);
        assert_eq!(1, enum_val);
        assert_ne!(enum_val, 2);
    }

    #[test]
    fn test_enum_definition_methods() {
        let def = create_test_enum();
        
        assert_eq!(def.len(), 4);
        assert!(!def.is_empty());
        
        let names: Vec<&String> = def.names();
        assert_eq!(names.len(), 4);
        assert!(names.contains(&&"One".to_string()));
        
        let values = def.values();
        assert_eq!(values.len(), 4);
        assert!(values.contains(&1));
    }
}
