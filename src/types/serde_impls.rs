use serde::{Deserialize, Deserializer, Serialize, Serializer};
use u256::U256;
use i256::I256;

impl Serialize for U256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as a hex string
        let hex_str = format!("0x{:x}", self);
        hex_str.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for U256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_str = String::deserialize(deserializer)?;
        // Remove 0x prefix if present
        let hex_str = hex_str.trim_start_matches("0x");
        
        U256::from_str_radix(hex_str, 16)
            .map_err(|e| serde::de::Error::custom(format!("Failed to parse U256: {}", e)))
    }
}

impl Serialize for I256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as a hex string with sign
        let sign = if self.is_negative() { "-" } else { "" };
        let hex_str = format!("{}{:x}", sign, self.abs());
        hex_str.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for I256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_str = String::deserialize(deserializer)?;
        let hex_str = hex_str.trim_start_matches("0x");
        
        if hex_str.starts_with('-') {
            let abs_hex = &hex_str[1..];
            U256::from_str_radix(abs_hex, 16)
                .map(|u| I256::from_negative(u))
                .map_err(|e| serde::de::Error::custom(format!("Failed to parse I256: {}", e)))
        } else {
            U256::from_str_radix(hex_str, 16)
                .map(|u| I256::from_positive(u))
                .map_err(|e| serde::de::Error::custom(format!("Failed to parse I256: {}", e)))
        }
    }
}
