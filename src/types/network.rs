//! Network address types for ClickHouse
//! 
//! Implements IPv4 and IPv6 address types with proper serialization
//! and deserialization support.

use std::net::{Ipv4Addr, Ipv6Addr};
use serde::{Deserialize, Serialize};

/// IPv4 address type for ClickHouse
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IPv4(pub Ipv4Addr);

impl IPv4 {
    /// Create a new IPv4 address
    pub fn new(addr: Ipv4Addr) -> Self {
        Self(addr)
    }

    /// Create from octets
    pub fn from_octets(a: u8, b: u8, c: u8, d: u8) -> Self {
        Self(Ipv4Addr::new(a, b, c, d))
    }

    /// Create from string
    pub fn from_str(s: &str) -> Result<Self, String> {
        s.parse::<Ipv4Addr>()
            .map(IPv4)
            .map_err(|e| format!("Invalid IPv4 address: {}", e))
    }

    /// Get the underlying Ipv4Addr
    pub fn as_addr(&self) -> Ipv4Addr {
        self.0
    }

    /// Convert to u32 (network byte order)
    pub fn to_u32(&self) -> u32 {
        u32::from(self.0)
    }

    /// Create from u32 (network byte order)
    pub fn from_u32(addr: u32) -> Self {
        Self(Ipv4Addr::from(addr))
    }

    /// Check if this is a private address
    pub fn is_private(&self) -> bool {
        self.0.is_private()
    }

    /// Check if this is a loopback address
    pub fn is_loopback(&self) -> bool {
        self.0.is_loopback()
    }

    /// Check if this is a multicast address
    pub fn is_multicast(&self) -> bool {
        self.0.is_multicast()
    }

    /// Check if this is a broadcast address
    pub fn is_broadcast(&self) -> bool {
        self.0.is_broadcast()
    }
}

impl Default for IPv4 {
    fn default() -> Self {
        Self(Ipv4Addr::UNSPECIFIED)
    }
}

impl From<Ipv4Addr> for IPv4 {
    fn from(addr: Ipv4Addr) -> Self {
        Self(addr)
    }
}

impl From<IPv4> for Ipv4Addr {
    fn from(ip: IPv4) -> Self {
        ip.0
    }
}

impl From<[u8; 4]> for IPv4 {
    fn from(octets: [u8; 4]) -> Self {
        Self(Ipv4Addr::from(octets))
    }
}

impl From<IPv4> for [u8; 4] {
    fn from(ip: IPv4) -> Self {
        ip.0.octets()
    }
}

impl std::fmt::Display for IPv4 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for IPv4 {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        IPv4::from_str(s)
    }
}

/// IPv6 address type for ClickHouse
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IPv6(pub Ipv6Addr);

impl IPv6 {
    /// Create a new IPv6 address
    pub fn new(addr: Ipv6Addr) -> Self {
        Self(addr)
    }

    /// Create from segments
    pub fn from_segments(segments: [u16; 8]) -> Self {
        Self(Ipv6Addr::from(segments))
    }

    /// Create from string
    pub fn from_str(s: &str) -> Result<Self, String> {
        s.parse::<Ipv6Addr>()
            .map(IPv6)
            .map_err(|e| format!("Invalid IPv6 address: {}", e))
    }

    /// Get the underlying Ipv6Addr
    pub fn as_addr(&self) -> Ipv6Addr {
        self.0
    }

    /// Convert to segments
    pub fn to_segments(&self) -> [u16; 8] {
        self.0.segments()
    }

    /// Check if this is a private address
    pub fn is_private(&self) -> bool {
        // IPv6 doesn't have a direct is_private method, so we check for unique local addresses
        self.0.is_unique_local()
    }

    /// Check if this is a loopback address
    pub fn is_loopback(&self) -> bool {
        self.0.is_loopback()
    }

    /// Check if this is a multicast address
    pub fn is_multicast(&self) -> bool {
        self.0.is_multicast()
    }

    /// Check if this is an unspecified address
    pub fn is_unspecified(&self) -> bool {
        self.0.is_unspecified()
    }

    /// Check if this is a unique local address
    pub fn is_unique_local(&self) -> bool {
        self.0.is_unique_local()
    }
}

impl Default for IPv6 {
    fn default() -> Self {
        Self(Ipv6Addr::UNSPECIFIED)
    }
}

impl From<Ipv6Addr> for IPv6 {
    fn from(addr: Ipv6Addr) -> Self {
        Self(addr)
    }
}

impl From<IPv6> for Ipv6Addr {
    fn from(ip: IPv6) -> Self {
        ip.0
    }
}

impl From<[u16; 8]> for IPv6 {
    fn from(segments: [u16; 8]) -> Self {
        Self(Ipv6Addr::from(segments))
    }
}

impl From<IPv6> for [u16; 8] {
    fn from(ip: IPv6) -> Self {
        ip.0.segments()
    }
}

impl std::fmt::Display for IPv6 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for IPv6 {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        IPv6::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_ipv4_basic_operations() {
        let ip = IPv4::from_octets(192, 168, 1, 1);
        assert_eq!(ip.to_string(), "192.168.1.1");
        assert_eq!(ip.as_addr(), Ipv4Addr::new(192, 168, 1, 1));
        
        let ip2 = IPv4::from_str("10.0.0.1").unwrap();
        assert_eq!(ip2.to_string(), "10.0.0.1");
    }

    #[test]
    fn test_ipv4_conversions() {
        let ip = IPv4::from_octets(192, 168, 1, 1);
        let u32_val = ip.to_u32();
        let ip2 = IPv4::from_u32(u32_val);
        assert_eq!(ip, ip2);
        
        let octets: [u8; 4] = ip.clone().into();
        assert_eq!(octets, [192, 168, 1, 1]);
        
        let ip3 = IPv4::from(octets);
        assert_eq!(ip, ip3);
    }

    #[test]
    fn test_ipv4_properties() {
        let localhost = IPv4::from_octets(127, 0, 0, 1);
        assert!(localhost.is_loopback());
        
        let private_ip = IPv4::from_octets(192, 168, 1, 1);
        assert!(private_ip.is_private());
        
        let multicast = IPv4::from_octets(224, 0, 0, 1);
        assert!(multicast.is_multicast());
    }

    #[test]
    fn test_ipv6_basic_operations() {
        let ip = IPv6::from_segments([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1]);
        assert_eq!(ip.to_string(), "2001:db8::1");
        
        let ip2 = IPv6::from_str("::1").unwrap();
        assert_eq!(ip2.to_string(), "::1");
    }

    #[test]
    fn test_ipv6_conversions() {
        let ip = IPv6::from_segments([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1]);
        let segments = ip.to_segments();
        let ip2 = IPv6::from_segments(segments);
        assert_eq!(ip, ip2);
        
        let segments: [u16; 8] = ip.clone().into();
        assert_eq!(segments, [0x2001, 0xdb8, 0, 0, 0, 0, 0, 1]);
        
        let ip3 = IPv6::from(segments);
        assert_eq!(ip, ip3);
    }

    #[test]
    fn test_ipv6_properties() {
        let localhost = IPv6::from_str("::1").unwrap();
        assert!(localhost.is_loopback());
        
        let unspecified = IPv6::from_str("::").unwrap();
        assert!(unspecified.is_unspecified());
        
        let unique_local = IPv6::from_str("fc00::1").unwrap();
        assert!(unique_local.is_unique_local());
    }

    #[test]
    fn test_ipv4_from_str_error() {
        let result = IPv4::from_str("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid IPv4 address"));
    }

    #[test]
    fn test_ipv6_from_str_error() {
        let result = IPv6::from_str("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid IPv6 address"));
    }

    #[test]
    fn test_ipv4_default() {
        let ip = IPv4::default();
        assert_eq!(ip.to_string(), "0.0.0.0");
    }

    #[test]
    fn test_ipv6_default() {
        let ip = IPv6::default();
        assert_eq!(ip.to_string(), "::");
    }
}
