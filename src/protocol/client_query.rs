//! Client Query message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use crate::types::{Block, Value};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Client Query message for executing SQL queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientQuery {
    /// Query ID (optional)
    pub query_id: Option<String>,
    /// Client info
    pub client_info: Option<String>,
    /// Query kind
    pub query_kind: QueryKind,
    /// Initial user
    pub initial_user: Option<String>,
    /// Initial query ID
    pub initial_query_id: Option<String>,
    /// Initial address
    pub initial_address: Option<String>,
    /// Quota key
    pub quota_key: Option<String>,
    /// OS user
    pub os_user: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Client name
    pub client_name: Option<String>,
    /// Client version
    pub client_version: Option<String>,
    /// Client version major
    pub client_version_major: Option<u64>,
    /// Client version minor
    pub client_version_minor: Option<u64>,
    /// Client version patch
    pub client_version_patch: Option<u64>,
    /// Client revision
    pub client_revision: Option<u64>,
    /// Interface
    pub interface: Option<String>,
    /// HTTP user agent
    pub http_user_agent: Option<String>,
    /// HTTP referer
    pub http_referer: Option<String>,
    /// Forward
    pub forward: Option<String>,
    /// Forwarded for
    pub forwarded_for: Option<String>,
    /// Forwarded proto
    pub forwarded_proto: Option<String>,
    /// Forwarded host
    pub forwarded_host: Option<String>,
    /// Forwarded port
    pub forwarded_port: Option<u16>,
    /// Forwarded server
    pub forwarded_server: Option<String>,
    /// Forwarded URI
    pub forwarded_uri: Option<String>,
    /// Forwarded method
    pub forwarded_method: Option<String>,
    /// Forwarded path
    pub forwarded_path: Option<String>,
    /// Forwarded query
    pub forwarded_query: Option<String>,
    /// Forwarded fragment
    pub forwarded_fragment: Option<String>,
    /// Forwarded username
    pub forwarded_username: Option<String>,
    /// Forwarded password
    pub forwarded_password: Option<String>,
    /// Forwarded auth
    pub forwarded_auth: Option<String>,
    /// Forwarded cert
    pub forwarded_cert: Option<String>,
    /// Forwarded SSL
    pub forwarded_ssl: Option<String>,
    /// Forwarded SSL verify
    pub forwarded_ssl_verify: Option<String>,
    /// Forwarded SSL client cert
    pub forwarded_ssl_client_cert: Option<String>,
    /// Forwarded SSL client key
    pub forwarded_ssl_client_key: Option<String>,
    /// Forwarded SSL CA cert
    pub forwarded_ssl_ca_cert: Option<String>,
    /// Forwarded SSL CA path
    pub forwarded_ssl_ca_path: Option<String>,
    /// Forwarded SSL CRL file
    pub forwarded_ssl_crl_file: Option<String>,
    /// Forwarded SSL CRL path
    pub forwarded_ssl_crl_path: Option<String>,
    /// Forwarded SSL verify depth
    pub forwarded_ssl_verify_depth: Option<u32>,
    /// Forwarded SSL session cache
    pub forwarded_ssl_session_cache: Option<String>,
    /// Forwarded SSL session timeout
    pub forwarded_ssl_session_timeout: Option<u32>,
    /// Forwarded SSL session tickets
    pub forwarded_ssl_session_tickets: Option<String>,
    /// Forwarded SSL session ticket lifetime hint
    pub forwarded_ssl_session_ticket_lifetime_hint: Option<u32>,
    /// Forwarded SSL session ticket lifetime hint seconds
    pub forwarded_ssl_session_ticket_lifetime_hint_seconds: Option<u32>,
    /// Forwarded SSL session ticket lifetime hint minutes
    pub forwarded_ssl_session_ticket_lifetime_hint_minutes: Option<u32>,
    /// Forwarded SSL session ticket lifetime hint hours
    pub forwarded_ssl_session_ticket_lifetime_hint_hours: Option<u32>,
    /// Forwarded SSL session ticket lifetime hint days
    pub forwarded_ssl_session_ticket_lifetime_hint_days: Option<u32>,
    /// Forwarded SSL session ticket lifetime hint weeks
    pub forwarded_ssl_session_ticket_lifetime_hint_weeks: Option<u32>,
    /// Forwarded SSL session ticket lifetime hint months
    pub forwarded_ssl_session_ticket_lifetime_hint_months: Option<u32>,
    /// Forwarded SSL session ticket lifetime hint years
    pub forwarded_ssl_session_ticket_lifetime_hint_years: Option<u32>,
    /// SQL query string
    pub sql: String,
    /// Query settings
    pub settings: HashMap<String, Value>,
    /// Stage
    pub stage: QueryProcessingStage,
    /// Compression
    pub compression: bool,
    /// Query data
    pub data: Option<Block>,
}

/// Query kind enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryKind {
    /// Initial query
    Initial = 0,
    /// Secondary query
    Secondary = 1,
}

impl QueryKind {
    /// Get the query kind as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryKind::Initial => "initial",
            QueryKind::Secondary => "secondary",
        }
    }

    /// Get the query kind from a string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "initial" => Some(QueryKind::Initial),
            "secondary" => Some(QueryKind::Secondary),
            _ => None,
        }
    }
}

/// Query processing stage enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryProcessingStage {
    /// Complete
    Complete = 0,
    /// Fetch columns
    FetchColumns = 1,
    /// With mergeable state
    WithMergeableState = 2,
    /// With mergeable state and finalize
    WithMergeableStateAndFinalize = 3,
}

impl QueryProcessingStage {
    /// Get the stage as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryProcessingStage::Complete => "complete",
            QueryProcessingStage::FetchColumns => "fetch_columns",
            QueryProcessingStage::WithMergeableState => "with_mergeable_state",
            QueryProcessingStage::WithMergeableStateAndFinalize => "with_mergeable_state_and_finalize",
        }
    }

    /// Get the stage from a string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "complete" => Some(QueryProcessingStage::Complete),
            "fetch_columns" => Some(QueryProcessingStage::FetchColumns),
            "with_mergeable_state" => Some(QueryProcessingStage::WithMergeableState),
            "with_mergeable_state_and_finalize" => Some(QueryProcessingStage::WithMergeableStateAndFinalize),
            _ => None,
        }
    }
}

impl ClientQuery {
    /// Create a new Client Query message
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            query_id: None,
            client_info: None,
            query_kind: QueryKind::Initial,
            initial_user: None,
            initial_query_id: None,
            initial_address: None,
            quota_key: None,
            os_user: None,
            client_hostname: None,
            client_name: None,
            client_version: None,
            client_version_major: None,
            client_version_minor: None,
            client_version_patch: None,
            client_revision: None,
            interface: None,
            http_user_agent: None,
            http_referer: None,
            forward: None,
            forwarded_for: None,
            forwarded_proto: None,
            forwarded_host: None,
            forwarded_port: None,
            forwarded_server: None,
            forwarded_uri: None,
            forwarded_method: None,
            forwarded_path: None,
            forwarded_query: None,
            forwarded_fragment: None,
            forwarded_username: None,
            forwarded_password: None,
            forwarded_auth: None,
            forwarded_cert: None,
            forwarded_ssl: None,
            forwarded_ssl_verify: None,
            forwarded_ssl_client_cert: None,
            forwarded_ssl_client_key: None,
            forwarded_ssl_ca_cert: None,
            forwarded_ssl_ca_path: None,
            forwarded_ssl_crl_file: None,
            forwarded_ssl_crl_path: None,
            forwarded_ssl_verify_depth: None,
            forwarded_ssl_session_cache: None,
            forwarded_ssl_session_timeout: None,
            forwarded_ssl_session_tickets: None,
            forwarded_ssl_session_ticket_lifetime_hint: None,
            forwarded_ssl_session_ticket_lifetime_hint_seconds: None,
            forwarded_ssl_session_ticket_lifetime_hint_minutes: None,
            forwarded_ssl_session_ticket_lifetime_hint_hours: None,
            forwarded_ssl_session_ticket_lifetime_hint_days: None,
            forwarded_ssl_session_ticket_lifetime_hint_weeks: None,
            forwarded_ssl_session_ticket_lifetime_hint_months: None,
            forwarded_ssl_session_ticket_lifetime_hint_years: None,
            sql: sql.into(),
            settings: HashMap::new(),
            stage: QueryProcessingStage::Complete,
            compression: false,
            data: None,
        }
    }

    /// Set query ID
    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.query_id = Some(query_id.into());
        self
    }

    /// Set client info
    pub fn with_client_info(mut self, info: impl Into<String>) -> Self {
        self.client_info = Some(info.into());
        self
    }

    /// Set query kind
    pub fn with_query_kind(mut self, kind: QueryKind) -> Self {
        self.query_kind = kind;
        self
    }

    /// Set initial user
    pub fn with_initial_user(mut self, user: impl Into<String>) -> Self {
        self.initial_user = Some(user.into());
        self
    }

    /// Set initial query ID
    pub fn with_initial_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.initial_query_id = Some(query_id.into());
        self
    }

    /// Set initial address
    pub fn with_initial_address(mut self, address: impl Into<String>) -> Self {
        self.initial_address = Some(address.into());
        self
    }

    /// Set quota key
    pub fn with_quota_key(mut self, key: impl Into<String>) -> Self {
        self.quota_key = Some(key.into());
        self
    }

    /// Set OS user
    pub fn with_os_user(mut self, user: impl Into<String>) -> Self {
        self.os_user = Some(user.into());
        self
    }

    /// Set client hostname
    pub fn with_client_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.client_hostname = Some(hostname.into());
        self
    }

    /// Set client name
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }

    /// Set client version
    pub fn with_client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = Some(version.into());
        self
    }

    /// Set client version numbers
    pub fn with_client_version_numbers(
        mut self,
        major: u64,
        minor: u64,
        patch: u64,
        revision: u64,
    ) -> Self {
        self.client_version_major = Some(major);
        self.client_version_minor = Some(minor);
        self.client_version_patch = Some(patch);
        self.client_revision = Some(revision);
        self
    }

    /// Set interface
    pub fn with_interface(mut self, interface: impl Into<String>) -> Self {
        self.interface = Some(interface.into());
        self
    }

    /// Set HTTP user agent
    pub fn with_http_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.http_user_agent = Some(user_agent.into());
        self
    }

    /// Set HTTP referer
    pub fn with_http_referer(mut self, referer: impl Into<String>) -> Self {
        self.http_referer = Some(referer.into());
        self
    }

    /// Set forward
    pub fn with_forward(mut self, forward: impl Into<String>) -> Self {
        self.forward = Some(forward.into());
        self
    }

    /// Set forwarded for
    pub fn with_forwarded_for(mut self, forwarded_for: impl Into<String>) -> Self {
        self.forwarded_for = Some(forwarded_for.into());
        self
    }

    /// Set forwarded proto
    pub fn with_forwarded_proto(mut self, proto: impl Into<String>) -> Self {
        self.forwarded_proto = Some(proto.into());
        self
    }

    /// Set forwarded host
    pub fn with_forwarded_host(mut self, host: impl Into<String>) -> Self {
        self.forwarded_host = Some(host.into());
        self
    }

    /// Set forwarded port
    pub fn with_forwarded_port(mut self, port: u16) -> Self {
        self.forwarded_port = Some(port);
        self
    }

    /// Set forwarded server
    pub fn with_forwarded_server(mut self, server: impl Into<String>) -> Self {
        self.forwarded_server = Some(server.into());
        self
    }

    /// Set forwarded URI
    pub fn with_forwarded_uri(mut self, uri: impl Into<String>) -> Self {
        self.forwarded_uri = Some(uri.into());
        self
    }

    /// Set forwarded method
    pub fn with_forwarded_method(mut self, method: impl Into<String>) -> Self {
        self.forwarded_method = Some(method.into());
        self
    }

    /// Set forwarded path
    pub fn with_forwarded_path(mut self, path: impl Into<String>) -> Self {
        self.forwarded_path = Some(path.into());
        self
    }

    /// Set forwarded query
    pub fn with_forwarded_query(mut self, query: impl Into<String>) -> Self {
        self.forwarded_query = Some(query.into());
        self
    }

    /// Set forwarded fragment
    pub fn with_forwarded_fragment(mut self, fragment: impl Into<String>) -> Self {
        self.forwarded_fragment = Some(fragment.into());
        self
    }

    /// Set forwarded username
    pub fn with_forwarded_username(mut self, username: impl Into<String>) -> Self {
        self.forwarded_username = Some(username.into());
        self
    }

    /// Set forwarded password
    pub fn with_forwarded_password(mut self, password: impl Into<String>) -> Self {
        self.forwarded_password = Some(password.into());
        self
    }

    /// Set forwarded auth
    pub fn with_forwarded_auth(mut self, auth: impl Into<String>) -> Self {
        self.forwarded_auth = Some(auth.into());
        self
    }

    /// Set forwarded cert
    pub fn with_forwarded_cert(mut self, cert: impl Into<String>) -> Self {
        self.forwarded_cert = Some(cert.into());
        self
    }

    /// Set forwarded SSL
    pub fn with_forwarded_ssl(mut self, ssl: impl Into<String>) -> Self {
        self.forwarded_ssl = Some(ssl.into());
        self
    }

    /// Set forwarded SSL verify
    pub fn with_forwarded_ssl_verify(mut self, verify: impl Into<String>) -> Self {
        self.forwarded_ssl_verify = Some(verify.into());
        self
    }

    /// Set forwarded SSL client cert
    pub fn with_forwarded_ssl_client_cert(mut self, cert: impl Into<String>) -> Self {
        self.forwarded_ssl_client_cert = Some(cert.into());
        self
    }

    /// Set forwarded SSL client key
    pub fn with_forwarded_ssl_client_key(mut self, key: impl Into<String>) -> Self {
        self.forwarded_ssl_client_key = Some(key.into());
        self
    }

    /// Set forwarded SSL CA cert
    pub fn with_forwarded_ssl_ca_cert(mut self, cert: impl Into<String>) -> Self {
        self.forwarded_ssl_ca_cert = Some(cert.into());
        self
    }

    /// Set forwarded SSL CA path
    pub fn with_forwarded_ssl_ca_path(mut self, path: impl Into<String>) -> Self {
        self.forwarded_ssl_ca_path = Some(path.into());
        self
    }

    /// Set forwarded SSL CRL file
    pub fn with_forwarded_ssl_crl_file(mut self, file: impl Into<String>) -> Self {
        self.forwarded_ssl_crl_file = Some(file.into());
        self
    }

    /// Set forwarded SSL CRL path
    pub fn with_forwarded_ssl_crl_path(mut self, path: impl Into<String>) -> Self {
        self.forwarded_ssl_crl_path = Some(path.into());
        self
    }

    /// Set forwarded SSL verify depth
    pub fn with_forwarded_ssl_verify_depth(mut self, depth: u32) -> Self {
        self.forwarded_ssl_verify_depth = Some(depth);
        self
    }

    /// Set forwarded SSL session cache
    pub fn with_forwarded_ssl_session_cache(mut self, cache: impl Into<String>) -> Self {
        self.forwarded_ssl_session_cache = Some(cache.into());
        self
    }

    /// Set forwarded SSL session timeout
    pub fn with_forwarded_ssl_session_timeout(mut self, timeout: u32) -> Self {
        self.forwarded_ssl_session_timeout = Some(timeout);
        self
    }

    /// Set forwarded SSL session tickets
    pub fn with_forwarded_ssl_session_tickets(mut self, tickets: impl Into<String>) -> Self {
        self.forwarded_ssl_session_tickets = Some(tickets.into());
        self
    }

    /// Set forwarded SSL session ticket lifetime hint
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint(mut self, hint: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint = Some(hint);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in seconds
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_seconds(mut self, seconds: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint_seconds = Some(seconds);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in minutes
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_minutes(mut self, minutes: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint_minutes = Some(minutes);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in hours
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_hours(mut self, hours: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint_hours = Some(hours);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in days
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_days(mut self, days: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint_days = Some(days);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in weeks
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_weeks(mut self, weeks: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint_weeks = Some(weeks);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in months
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_months(mut self, months: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint_months = Some(months);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in years
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_years(mut self, years: u32) -> Self {
        self.forwarded_ssl_session_ticket_lifetime_hint_years = Some(years);
        self
    }

    /// Add a setting
    pub fn with_setting(mut self, key: impl Into<String>, value: Value) -> Self {
        self.settings.insert(key.into(), value);
        self
    }

    /// Set stage
    pub fn with_stage(mut self, stage: QueryProcessingStage) -> Self {
        self.stage = stage;
        self
    }

    /// Set compression
    pub fn with_compression(mut self, compression: bool) -> Self {
        self.compression = compression;
        self
    }

    /// Set data
    pub fn with_data(mut self, data: Block) -> Self {
        self.data = Some(data);
        self
    }

    /// Get the SQL query
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the query ID
    pub fn query_id(&self) -> Option<&str> {
        self.query_id.as_deref()
    }

    /// Get the query kind
    pub fn query_kind(&self) -> QueryKind {
        self.query_kind
    }

    /// Get the stage
    pub fn stage(&self) -> QueryProcessingStage {
        self.stage
    }

    /// Check if compression is enabled
    pub fn is_compressed(&self) -> bool {
        self.compression
    }

    /// Check if data is present
    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    /// Get the data
    pub fn data(&self) -> Option<&Block> {
        self.data.as_ref()
    }

    /// Get the settings
    pub fn settings(&self) -> &HashMap<String, Value> {
        &self.settings
    }
}

impl Packet for ClientQuery {
    fn packet_type(&self) -> PacketType {
        PacketType::ClientQuery
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write query ID
        if let Some(ref query_id) = self.query_id {
            buf.put_u64_le(query_id.len() as u64);
            buf.extend_from_slice(query_id.as_bytes());
        } else {
            buf.put_u64_le(0);
        }

        // Write client info
        if let Some(ref client_info) = self.client_info {
            buf.put_u64_le(client_info.len() as u64);
            buf.extend_from_slice(client_info.as_bytes());
        } else {
            buf.put_u64_le(0);
        }

        // Write query kind
        buf.put_u64_le(self.query_kind as u64);

        // Write stage
        buf.put_u64_le(self.stage as u64);

        // Write compression
        buf.put_u64_le(if self.compression { 1 } else { 0 });

        // Write SQL query
        buf.put_u64_le(self.sql.len() as u64);
        buf.extend_from_slice(self.sql.as_bytes());

        // Write settings (simplified for now)
        buf.put_u64_le(self.settings.len() as u64);
        for (key, value) in &self.settings {
            buf.put_u64_le(key.len() as u64);
            buf.extend_from_slice(key.as_bytes());
            // For now, just write the value as a string
            let value_str = format!("{:?}", value);
            buf.put_u64_le(value_str.len() as u64);
            buf.extend_from_slice(value_str.as_bytes());
        }

        // Write data (if present)
        if let Some(ref _data) = self.data {
            buf.put_u64_le(1); // Has data
            // For now, just write a placeholder for the block
            buf.put_u64_le(0); // Block size placeholder
        } else {
            buf.put_u64_le(0); // No data
        }

        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        // Read query ID
        let query_id_len = buf.get_u64_le() as usize;
        let query_id = if query_id_len > 0 {
            if buf.remaining() < query_id_len {
                return Err(Error::Protocol("Insufficient data for query ID".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(query_id_len)).to_string())
        } else {
            None
        };

        // Read client info
        let client_info_len = buf.get_u64_le() as usize;
        let client_info = if client_info_len > 0 {
            if buf.remaining() < client_info_len {
                return Err(Error::Protocol("Insufficient data for client info".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(client_info_len)).to_string())
        } else {
            None
        };

        // Read query kind
        let query_kind_value = buf.get_u64_le();
        let query_kind = match query_kind_value {
            0 => QueryKind::Initial,
            1 => QueryKind::Secondary,
            _ => return Err(Error::Protocol("Invalid query kind".to_string())),
        };

        // Read stage
        let stage_value = buf.get_u64_le();
        let stage = match stage_value {
            0 => QueryProcessingStage::Complete,
            1 => QueryProcessingStage::FetchColumns,
            2 => QueryProcessingStage::WithMergeableState,
            3 => QueryProcessingStage::WithMergeableStateAndFinalize,
            _ => return Err(Error::Protocol("Invalid query processing stage".to_string())),
        };

        // Read compression
        let compression_value = buf.get_u64_le();
        let compression = compression_value != 0;

        // Read SQL query
        let sql_len = buf.get_u64_le() as usize;
        if buf.remaining() < sql_len {
            return Err(Error::Protocol("Insufficient data for SQL query".to_string()));
        }
        let sql = String::from_utf8_lossy(&buf.copy_to_bytes(sql_len)).to_string();

        // Read settings (simplified for now)
        let settings_len = buf.get_u64_le() as usize;
        let mut settings = HashMap::new();
        for _ in 0..settings_len {
            let key_len = buf.get_u64_le() as usize;
            if buf.remaining() < key_len {
                return Err(Error::Protocol("Insufficient data for setting key".to_string()));
            }
            let key = String::from_utf8_lossy(&buf.copy_to_bytes(key_len)).to_string();

            let value_len = buf.get_u64_le() as usize;
            if buf.remaining() < value_len {
                return Err(Error::Protocol("Insufficient data for setting value".to_string()));
            }
            let value_str = String::from_utf8_lossy(&buf.copy_to_bytes(value_len)).to_string();
            // For now, just use a placeholder value
            settings.insert(key, Value::String(value_str));
        }

        // Read data (simplified for now)
        let has_data = buf.get_u64_le() != 0;
        let data = if has_data {
            let _block_size = buf.get_u64_le(); // Skip block size for now
            None // Placeholder
        } else {
            None
        };

        Ok(Self {
            query_id,
            client_info,
            query_kind,
            initial_user: None,
            initial_query_id: None,
            initial_address: None,
            quota_key: None,
            os_user: None,
            client_hostname: None,
            client_name: None,
            client_version: None,
            client_version_major: None,
            client_version_minor: None,
            client_version_patch: None,
            client_revision: None,
            interface: None,
            http_user_agent: None,
            http_referer: None,
            forward: None,
            forwarded_for: None,
            forwarded_proto: None,
            forwarded_host: None,
            forwarded_port: None,
            forwarded_server: None,
            forwarded_uri: None,
            forwarded_method: None,
            forwarded_path: None,
            forwarded_query: None,
            forwarded_fragment: None,
            forwarded_username: None,
            forwarded_password: None,
            forwarded_auth: None,
            forwarded_cert: None,
            forwarded_ssl: None,
            forwarded_ssl_verify: None,
            forwarded_ssl_client_cert: None,
            forwarded_ssl_client_key: None,
            forwarded_ssl_ca_cert: None,
            forwarded_ssl_ca_path: None,
            forwarded_ssl_crl_file: None,
            forwarded_ssl_crl_path: None,
            forwarded_ssl_verify_depth: None,
            forwarded_ssl_session_cache: None,
            forwarded_ssl_session_timeout: None,
            forwarded_ssl_session_tickets: None,
            forwarded_ssl_session_ticket_lifetime_hint: None,
            forwarded_ssl_session_ticket_lifetime_hint_seconds: None,
            forwarded_ssl_session_ticket_lifetime_hint_minutes: None,
            forwarded_ssl_session_ticket_lifetime_hint_hours: None,
            forwarded_ssl_session_ticket_lifetime_hint_days: None,
            forwarded_ssl_session_ticket_lifetime_hint_weeks: None,
            forwarded_ssl_session_ticket_lifetime_hint_months: None,
            forwarded_ssl_session_ticket_lifetime_hint_years: None,
            sql,
            settings,
            stage,
            compression,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_client_query_new() {
        let query = ClientQuery::new("SELECT * FROM table");
        assert_eq!(query.sql, "SELECT * FROM table");
        assert_eq!(query.query_kind, QueryKind::Initial);
        assert_eq!(query.stage, QueryProcessingStage::Complete);
        assert!(!query.compression);
        assert!(!query.has_data());
    }

    #[test]
    fn test_client_query_with_query_id() {
        let query = ClientQuery::new("SELECT * FROM table").with_query_id("test-query-123");
        assert_eq!(query.query_id(), Some("test-query-123"));
    }

    #[test]
    fn test_client_query_with_query_kind() {
        let query = ClientQuery::new("SELECT * FROM table").with_query_kind(QueryKind::Secondary);
        assert_eq!(query.query_kind(), QueryKind::Secondary);
    }

    #[test]
    fn test_client_query_with_stage() {
        let query = ClientQuery::new("SELECT * FROM table")
            .with_stage(QueryProcessingStage::FetchColumns);
        assert_eq!(query.stage(), QueryProcessingStage::FetchColumns);
    }

    #[test]
    fn test_client_query_with_compression() {
        let query = ClientQuery::new("SELECT * FROM table").with_compression(true);
        assert!(query.is_compressed());
    }

    #[test]
    fn test_client_query_with_setting() {
        let query = ClientQuery::new("SELECT * FROM table")
            .with_setting("max_memory_usage", Value::UInt64(1000000));
        assert_eq!(query.settings().len(), 1);
        assert_eq!(
            query.settings().get("max_memory_usage"),
            Some(&Value::UInt64(1000000))
        );
    }

    #[test]
    fn test_query_kind_conversion() {
        assert_eq!(QueryKind::from_str("initial"), Some(QueryKind::Initial));
        assert_eq!(QueryKind::from_str("secondary"), Some(QueryKind::Secondary));
        assert_eq!(QueryKind::from_str("unknown"), None);
    }

    #[test]
    fn test_query_processing_stage_conversion() {
        assert_eq!(
            QueryProcessingStage::from_str("complete"),
            Some(QueryProcessingStage::Complete)
        );
        assert_eq!(
            QueryProcessingStage::from_str("fetch_columns"),
            Some(QueryProcessingStage::FetchColumns)
        );
        assert_eq!(
            QueryProcessingStage::from_str("with_mergeable_state"),
            Some(QueryProcessingStage::WithMergeableState)
        );
        assert_eq!(
            QueryProcessingStage::from_str("with_mergeable_state_and_finalize"),
            Some(QueryProcessingStage::WithMergeableStateAndFinalize)
        );
        assert_eq!(QueryProcessingStage::from_str("unknown"), None);
    }

    #[test]
    fn test_client_query_packet_type() {
        let query = ClientQuery::new("SELECT * FROM table");
        assert_eq!(query.packet_type(), PacketType::ClientQuery);
    }

    #[test]
    fn test_client_query_serialize_deserialize() {
        let original = ClientQuery::new("SELECT * FROM table")
            .with_query_id("test-query-123")
            .with_query_kind(QueryKind::Secondary)
            .with_stage(QueryProcessingStage::FetchColumns)
            .with_compression(true)
            .with_setting("max_memory_usage", Value::UInt64(1000000));

        let mut buf = BytesMut::new();
        original.serialize(&mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = ClientQuery::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.sql, deserialized.sql);
        assert_eq!(original.query_id, deserialized.query_id);
        assert_eq!(original.query_kind, deserialized.query_kind);
        assert_eq!(original.stage, deserialized.stage);
        assert_eq!(original.compression, deserialized.compression);
        assert_eq!(original.settings.len(), deserialized.settings.len());
    }
}
