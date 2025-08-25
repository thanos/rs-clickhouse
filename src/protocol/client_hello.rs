//! Client Hello message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io;

/// Client Hello message sent when establishing a connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHello {
    /// Client name (e.g., "clickhouse-rust-client")
    pub client_name: String,
    /// Client version major
    pub client_version_major: u64,
    /// Client version minor
    pub client_version_minor: u64,
    /// Client version patch
    pub client_version_patch: u64,
    /// Client revision
    pub client_revision: u64,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Protocol version
    pub protocol_version: u64,
    /// Client query info
    pub client_query_info: Option<String>,
    /// Client query info version
    pub client_query_info_version: Option<u64>,
    /// Client query info kind
    pub client_query_info_kind: Option<String>,
    /// Client query info initial user
    pub client_query_info_initial_user: Option<String>,
    /// Client query info initial query id
    pub client_query_info_initial_query_id: Option<String>,
    /// Client query info initial address
    pub client_query_info_initial_address: Option<String>,
    /// Client query info quota key
    pub client_query_info_quota_key: Option<String>,
    /// Client query info os user
    pub client_query_info_os_user: Option<String>,
    /// Client query info client hostname
    pub client_query_info_client_hostname: Option<String>,
    /// Client query info client name
    pub client_query_info_client_name: Option<String>,
    /// Client query info client version
    pub client_query_info_client_version: Option<String>,
    /// Client query info client version major
    pub client_query_info_client_version_major: Option<u64>,
    /// Client query info client version minor
    pub client_query_info_client_version_minor: Option<u64>,
    /// Client query info client version patch
    pub client_query_info_client_version_patch: Option<u64>,
    /// Client query info client revision
    pub client_query_info_client_revision: Option<u64>,
    /// Client query info interface
    pub client_query_info_interface: Option<String>,
    /// Client query info http user agent
    pub client_query_info_http_user_agent: Option<String>,
    /// Client query info http referer
    pub client_query_info_http_referer: Option<String>,
    /// Client query info forward
    pub client_query_info_forward: Option<String>,
    /// Client query info forwarded for
    pub client_query_info_forwarded_for: Option<String>,
    /// Client query info forwarded proto
    pub client_query_info_forwarded_proto: Option<String>,
    /// Client query info forwarded host
    pub client_query_info_forwarded_host: Option<String>,
    /// Client query info forwarded port
    pub client_query_info_forwarded_port: Option<u16>,
    /// Client query info forwarded server
    pub client_query_info_forwarded_server: Option<String>,
    /// Client query info forwarded uri
    pub client_query_info_forwarded_uri: Option<String>,
    /// Client query info forwarded method
    pub client_query_info_forwarded_method: Option<String>,
    /// Client query info forwarded path
    pub client_query_info_forwarded_path: Option<String>,
    /// Client query info forwarded query
    pub client_query_info_forwarded_query: Option<String>,
    /// Client query info forwarded fragment
    pub client_query_info_forwarded_fragment: Option<String>,
    /// Client query info forwarded username
    pub client_query_info_forwarded_username: Option<String>,
    /// Client query info forwarded password
    pub client_query_info_forwarded_password: Option<String>,
    /// Client query info forwarded auth
    pub client_query_info_forwarded_auth: Option<String>,
    /// Client query info forwarded cert
    pub client_query_info_forwarded_cert: Option<String>,
    /// Client query info forwarded ssl
    pub client_query_info_forwarded_ssl: Option<String>,
    /// Client query info forwarded ssl verify
    pub client_query_info_forwarded_ssl_verify: Option<String>,
    /// Client query info forwarded ssl client cert
    pub client_query_info_forwarded_ssl_client_cert: Option<String>,
    /// Client query info forwarded ssl client key
    pub client_query_info_forwarded_ssl_client_key: Option<String>,
    /// Client query info forwarded ssl ca cert
    pub client_query_info_forwarded_ssl_ca_cert: Option<String>,
    /// Client query info forwarded ssl ca path
    pub client_query_info_forwarded_ssl_ca_path: Option<String>,
    /// Client query info forwarded ssl crl file
    pub client_query_info_forwarded_ssl_crl_file: Option<String>,
    /// Client query info forwarded ssl crl path
    pub client_query_info_forwarded_ssl_crl_path: Option<String>,
    /// Client query info forwarded ssl verify depth
    pub client_query_info_forwarded_ssl_verify_depth: Option<u32>,
    /// Client query info forwarded ssl session cache
    pub client_query_info_forwarded_ssl_session_cache: Option<String>,
    /// Client query info forwarded ssl session timeout
    pub client_query_info_forwarded_ssl_session_timeout: Option<u32>,
    /// Client query info forwarded ssl session tickets
    pub client_query_info_forwarded_ssl_session_tickets: Option<String>,
    /// Client query info forwarded ssl session ticket lifetime hint
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint: Option<u32>,
    /// Client query info forwarded ssl session ticket lifetime hint seconds
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint_seconds: Option<u32>,
    /// Client query info forwarded ssl session ticket lifetime hint minutes
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint_minutes: Option<u32>,
    /// Client query info forwarded ssl session ticket lifetime hint hours
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint_hours: Option<u32>,
    /// Client query info forwarded ssl session ticket lifetime hint days
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint_days: Option<u32>,
    /// Client query info forwarded ssl session ticket lifetime hint weeks
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint_weeks: Option<u32>,
    /// Client query info forwarded ssl session ticket lifetime hint months
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint_months: Option<u32>,
    /// Client query info forwarded ssl session ticket lifetime hint years
    pub client_query_info_forwarded_ssl_session_ticket_lifetime_hint_years: Option<u32>,
}

impl ClientHello {
    /// Create a new Client Hello message
    pub fn new(
        client_name: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            client_name: client_name.into(),
            client_version_major: 1,
            client_version_minor: 0,
            client_version_patch: 0,
            client_revision: 1,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            protocol_version: super::constants::DEFAULT_PROTOCOL_VERSION,
            client_query_info: None,
            client_query_info_version: None,
            client_query_info_kind: None,
            client_query_info_initial_user: None,
            client_query_info_initial_query_id: None,
            client_query_info_initial_address: None,
            client_query_info_quota_key: None,
            client_query_info_os_user: None,
            client_query_info_client_hostname: None,
            client_query_info_client_name: None,
            client_query_info_client_version: None,
            client_query_info_client_version_major: None,
            client_query_info_client_version_minor: None,
            client_query_info_client_version_patch: None,
            client_query_info_client_revision: None,
            client_query_info_interface: None,
            client_query_info_http_user_agent: None,
            client_query_info_http_referer: None,
            client_query_info_forward: None,
            client_query_info_forwarded_for: None,
            client_query_info_forwarded_proto: None,
            client_query_info_forwarded_host: None,
            client_query_info_forwarded_port: None,
            client_query_info_forwarded_server: None,
            client_query_info_forwarded_uri: None,
            client_query_info_forwarded_method: None,
            client_query_info_forwarded_path: None,
            client_query_info_forwarded_query: None,
            client_query_info_forwarded_fragment: None,
            client_query_info_forwarded_username: None,
            client_query_info_forwarded_password: None,
            client_query_info_forwarded_auth: None,
            client_query_info_forwarded_cert: None,
            client_query_info_forwarded_ssl: None,
            client_query_info_forwarded_ssl_verify: None,
            client_query_info_forwarded_ssl_client_cert: None,
            client_query_info_forwarded_ssl_client_key: None,
            client_query_info_forwarded_ssl_ca_cert: None,
            client_query_info_forwarded_ssl_ca_path: None,
            client_query_info_forwarded_ssl_crl_file: None,
            client_query_info_forwarded_ssl_crl_path: None,
            client_query_info_forwarded_ssl_verify_depth: None,
            client_query_info_forwarded_ssl_session_cache: None,
            client_query_info_forwarded_ssl_session_timeout: None,
            client_query_info_forwarded_ssl_session_tickets: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_seconds: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_minutes: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_hours: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_days: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_weeks: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_months: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_years: None,
        }
    }

    /// Set client version
    pub fn with_version(mut self, major: u64, minor: u64, patch: u64, revision: u64) -> Self {
        self.client_version_major = major;
        self.client_version_minor = minor;
        self.client_version_patch = patch;
        self.client_revision = revision;
        self
    }

    /// Set protocol version
    pub fn with_protocol_version(mut self, version: u64) -> Self {
        self.protocol_version = version;
        self
    }

    /// Set client query info
    pub fn with_client_query_info(mut self, info: impl Into<String>) -> Self {
        self.client_query_info = Some(info.into());
        self
    }

    /// Set client query info version
    pub fn with_client_query_info_version(mut self, version: u64) -> Self {
        self.client_query_info_version = Some(version);
        self
    }

    /// Set client query info kind
    pub fn with_client_query_info_kind(mut self, kind: impl Into<String>) -> Self {
        self.client_query_info_kind = Some(kind.into());
        self
    }

    /// Set initial user
    pub fn with_initial_user(mut self, user: impl Into<String>) -> Self {
        self.client_query_info_initial_user = Some(user.into());
        self
    }

    /// Set initial query ID
    pub fn with_initial_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.client_query_info_initial_query_id = Some(query_id.into());
        self
    }

    /// Set initial address
    pub fn with_initial_address(mut self, address: impl Into<String>) -> Self {
        self.client_query_info_initial_address = Some(address.into());
        self
    }

    /// Set quota key
    pub fn with_quota_key(mut self, key: impl Into<String>) -> Self {
        self.client_query_info_quota_key = Some(key.into());
        self
    }

    /// Set OS user
    pub fn with_os_user(mut self, user: impl Into<String>) -> Self {
        self.client_query_info_os_user = Some(user.into());
        self
    }

    /// Set client hostname
    pub fn with_client_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.client_query_info_client_hostname = Some(hostname.into());
        self
    }

    /// Set client name
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_query_info_client_name = Some(name.into());
        self
    }

    /// Set client version
    pub fn with_client_version(mut self, version: impl Into<String>) -> Self {
        self.client_query_info_client_version = Some(version.into());
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
        self.client_query_info_client_version_major = Some(major);
        self.client_query_info_client_version_minor = Some(minor);
        self.client_query_info_client_version_patch = Some(patch);
        self.client_query_info_client_revision = Some(revision);
        self
    }

    /// Set interface
    pub fn with_interface(mut self, interface: impl Into<String>) -> Self {
        self.client_query_info_interface = Some(interface.into());
        self
    }

    /// Set HTTP user agent
    pub fn with_http_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.client_query_info_http_user_agent = Some(user_agent.into());
        self
    }

    /// Set HTTP referer
    pub fn with_http_referer(mut self, referer: impl Into<String>) -> Self {
        self.client_query_info_http_referer = Some(referer.into());
        self
    }

    /// Set forward
    pub fn with_forward(mut self, forward: impl Into<String>) -> Self {
        self.client_query_info_forward = Some(forward.into());
        self
    }

    /// Set forwarded for
    pub fn with_forwarded_for(mut self, forwarded_for: impl Into<String>) -> Self {
        self.client_query_info_forwarded_for = Some(forwarded_for.into());
        self
    }

    /// Set forwarded proto
    pub fn with_forwarded_proto(mut self, proto: impl Into<String>) -> Self {
        self.client_query_info_forwarded_proto = Some(proto.into());
        self
    }

    /// Set forwarded host
    pub fn with_forwarded_host(mut self, host: impl Into<String>) -> Self {
        self.client_query_info_forwarded_host = Some(host.into());
        self
    }

    /// Set forwarded port
    pub fn with_forwarded_port(mut self, port: u16) -> Self {
        self.client_query_info_forwarded_port = Some(port);
        self
    }

    /// Set forwarded server
    pub fn with_forwarded_server(mut self, server: impl Into<String>) -> Self {
        self.client_query_info_forwarded_server = Some(server.into());
        self
    }

    /// Set forwarded URI
    pub fn with_forwarded_uri(mut self, uri: impl Into<String>) -> Self {
        self.client_query_info_forwarded_uri = Some(uri.into());
        self
    }

    /// Set forwarded method
    pub fn with_forwarded_method(mut self, method: impl Into<String>) -> Self {
        self.client_query_info_forwarded_method = Some(method.into());
        self
    }

    /// Set forwarded path
    pub fn with_forwarded_path(mut self, path: impl Into<String>) -> Self {
        self.client_query_info_forwarded_path = Some(path.into());
        self
    }

    /// Set forwarded query
    pub fn with_forwarded_query(mut self, query: impl Into<String>) -> Self {
        self.client_query_info_forwarded_query = Some(query.into());
        self
    }

    /// Set forwarded fragment
    pub fn with_forwarded_fragment(mut self, fragment: impl Into<String>) -> Self {
        self.client_query_info_forwarded_fragment = Some(fragment.into());
        self
    }

    /// Set forwarded username
    pub fn with_forwarded_username(mut self, username: impl Into<String>) -> Self {
        self.client_query_info_forwarded_username = Some(username.into());
        self
    }

    /// Set forwarded password
    pub fn with_forwarded_password(mut self, password: impl Into<String>) -> Self {
        self.client_query_info_forwarded_password = Some(password.into());
        self
    }

    /// Set forwarded auth
    pub fn with_forwarded_auth(mut self, auth: impl Into<String>) -> Self {
        self.client_query_info_forwarded_auth = Some(auth.into());
        self
    }

    /// Set forwarded cert
    pub fn with_forwarded_cert(mut self, cert: impl Into<String>) -> Self {
        self.client_query_info_forwarded_cert = Some(cert.into());
        self
    }

    /// Set forwarded SSL
    pub fn with_forwarded_ssl(mut self, ssl: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl = Some(ssl.into());
        self
    }

    /// Set forwarded SSL verify
    pub fn with_forwarded_ssl_verify(mut self, verify: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_verify = Some(verify.into());
        self
    }

    /// Set forwarded SSL client cert
    pub fn with_forwarded_ssl_client_cert(mut self, cert: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_client_cert = Some(cert.into());
        self
    }

    /// Set forwarded SSL client key
    pub fn with_forwarded_ssl_client_key(mut self, key: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_client_key = Some(key.into());
        self
    }

    /// Set forwarded SSL CA cert
    pub fn with_forwarded_ssl_ca_cert(mut self, cert: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_ca_cert = Some(cert.into());
        self
    }

    /// Set forwarded SSL CA path
    pub fn with_forwarded_ssl_ca_path(mut self, path: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_ca_path = Some(path.into());
        self
    }

    /// Set forwarded SSL CRL file
    pub fn with_forwarded_ssl_crl_file(mut self, file: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_crl_file = Some(file.into());
        self
    }

    /// Set forwarded SSL CRL path
    pub fn with_forwarded_ssl_crl_path(mut self, path: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_crl_path = Some(path.into());
        self
    }

    /// Set forwarded SSL verify depth
    pub fn with_forwarded_ssl_verify_depth(mut self, depth: u32) -> Self {
        self.client_query_info_forwarded_ssl_verify_depth = Some(depth);
        self
    }

    /// Set forwarded SSL session cache
    pub fn with_forwarded_ssl_session_cache(mut self, cache: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_session_cache = Some(cache.into());
        self
    }

    /// Set forwarded SSL session timeout
    pub fn with_forwarded_ssl_session_timeout(mut self, timeout: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_timeout = Some(timeout);
        self
    }

    /// Set forwarded SSL session tickets
    pub fn with_forwarded_ssl_session_tickets(mut self, tickets: impl Into<String>) -> Self {
        self.client_query_info_forwarded_ssl_session_tickets = Some(tickets.into());
        self
    }

    /// Set forwarded SSL session ticket lifetime hint
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint(mut self, hint: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint = Some(hint);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in seconds
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_seconds(mut self, seconds: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint_seconds = Some(seconds);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in minutes
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_minutes(mut self, minutes: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint_minutes = Some(minutes);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in hours
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_hours(mut self, hours: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint_hours = Some(hours);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in days
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_days(mut self, days: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint_days = Some(days);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in weeks
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_weeks(mut self, weeks: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint_weeks = Some(weeks);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in months
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_months(mut self, months: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint_months = Some(months);
        self
    }

    /// Set forwarded SSL session ticket lifetime hint in years
    pub fn with_forwarded_ssl_session_ticket_lifetime_hint_years(mut self, years: u32) -> Self {
        self.client_query_info_forwarded_ssl_session_ticket_lifetime_hint_years = Some(years);
        self
    }

    /// Get the client version string
    pub fn client_version_string(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.client_version_major,
            self.client_version_minor,
            self.client_version_patch,
            self.client_revision
        )
    }

    /// Get the protocol version string
    pub fn protocol_version_string(&self) -> String {
        format!("{}", self.protocol_version)
    }
}

impl Packet for ClientHello {
    fn packet_type(&self) -> PacketType {
        PacketType::ClientHello
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write client name
        buf.put_u64_le(self.client_name.len() as u64);
        buf.extend_from_slice(self.client_name.as_bytes());

        // Write client version
        buf.put_u64_le(self.client_version_major);
        buf.put_u64_le(self.client_version_minor);
        buf.put_u64_le(self.client_version_patch);
        buf.put_u64_le(self.client_revision);

        // Write database
        buf.put_u64_le(self.database.len() as u64);
        buf.extend_from_slice(self.database.as_bytes());

        // Write username
        buf.put_u64_le(self.username.len() as u64);
        buf.extend_from_slice(self.username.as_bytes());

        // Write password
        buf.put_u64_le(self.password.len() as u64);
        buf.extend_from_slice(self.password.as_bytes());

        // Write protocol version
        buf.put_u64_le(self.protocol_version);

        // Write client query info (simplified for now)
        if let Some(ref info) = self.client_query_info {
            buf.put_u64_le(info.len() as u64);
            buf.extend_from_slice(info.as_bytes());
        } else {
            buf.put_u64_le(0);
        }

        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        // Read client name
        let name_len = buf.get_u64_le() as usize;
        if buf.remaining() < name_len {
            return Err(Error::Protocol("Insufficient data for client name".to_string()));
        }
        let client_name = String::from_utf8_lossy(&buf.copy_to_bytes(name_len)).to_string();

        // Read client version
        let client_version_major = buf.get_u64_le();
        let client_version_minor = buf.get_u64_le();
        let client_version_patch = buf.get_u64_le();
        let client_revision = buf.get_u64_le();

        // Read database
        let db_len = buf.get_u64_le() as usize;
        if buf.remaining() < db_len {
            return Err(Error::Protocol("Insufficient data for database".to_string()));
        }
        let database = String::from_utf8_lossy(&buf.copy_to_bytes(db_len)).to_string();

        // Read username
        let user_len = buf.get_u64_le() as usize;
        if buf.remaining() < user_len {
            return Err(Error::Protocol("Insufficient data for username".to_string()));
        }
        let username = String::from_utf8_lossy(&buf.copy_to_bytes(user_len)).to_string();

        // Read password
        let pass_len = buf.get_u64_le() as usize;
        if buf.remaining() < pass_len {
            return Err(Error::Protocol("Insufficient data for password".to_string()));
        }
        let password = String::from_utf8_lossy(&buf.copy_to_bytes(pass_len)).to_string();

        // Read protocol version
        let protocol_version = buf.get_u64_le();

        // Read client query info (simplified for now)
        let info_len = buf.get_u64_le() as usize;
        let client_query_info = if info_len > 0 {
            if buf.remaining() < info_len {
                return Err(Error::Protocol("Insufficient data for client query info".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(info_len)).to_string())
        } else {
            None
        };

        Ok(Self {
            client_name,
            client_version_major,
            client_version_minor,
            client_version_patch,
            client_revision,
            database,
            username,
            password,
            protocol_version,
            client_query_info,
            client_query_info_version: None,
            client_query_info_kind: None,
            client_query_info_initial_user: None,
            client_query_info_initial_query_id: None,
            client_query_info_initial_address: None,
            client_query_info_quota_key: None,
            client_query_info_os_user: None,
            client_query_info_client_hostname: None,
            client_query_info_client_name: None,
            client_query_info_client_version: None,
            client_query_info_client_version_major: None,
            client_query_info_client_version_minor: None,
            client_query_info_client_version_patch: None,
            client_query_info_client_revision: None,
            client_query_info_interface: None,
            client_query_info_http_user_agent: None,
            client_query_info_http_referer: None,
            client_query_info_forward: None,
            client_query_info_forwarded_for: None,
            client_query_info_forwarded_proto: None,
            client_query_info_forwarded_host: None,
            client_query_info_forwarded_port: None,
            client_query_info_forwarded_server: None,
            client_query_info_forwarded_uri: None,
            client_query_info_forwarded_method: None,
            client_query_info_forwarded_path: None,
            client_query_info_forwarded_query: None,
            client_query_info_forwarded_fragment: None,
            client_query_info_forwarded_username: None,
            client_query_info_forwarded_password: None,
            client_query_info_forwarded_auth: None,
            client_query_info_forwarded_cert: None,
            client_query_info_forwarded_ssl: None,
            client_query_info_forwarded_ssl_verify: None,
            client_query_info_forwarded_ssl_client_cert: None,
            client_query_info_forwarded_ssl_client_key: None,
            client_query_info_forwarded_ssl_ca_cert: None,
            client_query_info_forwarded_ssl_ca_path: None,
            client_query_info_forwarded_ssl_crl_file: None,
            client_query_info_forwarded_ssl_crl_path: None,
            client_query_info_forwarded_ssl_verify_depth: None,
            client_query_info_forwarded_ssl_session_cache: None,
            client_query_info_forwarded_ssl_session_timeout: None,
            client_query_info_forwarded_ssl_session_tickets: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_seconds: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_minutes: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_hours: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_days: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_weeks: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_months: None,
            client_query_info_forwarded_ssl_session_ticket_lifetime_hint_years: None,
        })
    }
}

impl Default for ClientHello {
    fn default() -> Self {
        Self::new(
            "clickhouse-rust-client",
            super::constants::DEFAULT_DATABASE,
            super::constants::DEFAULT_USERNAME,
            super::constants::DEFAULT_PASSWORD,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Packet;

    #[test]
    fn test_client_hello_new() {
        let hello = ClientHello::new("test-client", "test-db", "test-user", "test-pass");
        assert_eq!(hello.client_name, "test-client");
        assert_eq!(hello.database, "test-db");
        assert_eq!(hello.username, "test-user");
        assert_eq!(hello.password, "test-pass");
        assert_eq!(hello.protocol_version, super::super::constants::DEFAULT_PROTOCOL_VERSION);
    }

    #[test]
    fn test_client_hello_with_version() {
        let hello = ClientHello::new("test-client", "test-db", "test-user", "test-pass")
            .with_version(2, 1, 3, 42);
        assert_eq!(hello.client_version_major, 2);
        assert_eq!(hello.client_version_minor, 1);
        assert_eq!(hello.client_version_patch, 3);
        assert_eq!(hello.client_revision, 42);
    }

    #[test]
    fn test_client_hello_with_protocol_version() {
        let hello = ClientHello::new("test-client", "test-db", "test-user", "test-pass")
            .with_protocol_version(54328);
        assert_eq!(hello.protocol_version, 54328);
    }

    #[test]
    fn test_client_hello_client_version_string() {
        let hello = ClientHello::new("test-client", "test-db", "test-user", "test-pass")
            .with_version(2, 1, 3, 42);
        assert_eq!(hello.client_version_string(), "2.1.3.42");
    }

    #[test]
    fn test_client_hello_protocol_version_string() {
        let hello = ClientHello::new("test-client", "test-db", "test-user", "test-pass")
            .with_protocol_version(54328);
        assert_eq!(hello.protocol_version_string(), "54328");
    }

    #[test]
    fn test_client_hello_packet_type() {
        let hello = ClientHello::new("test-client", "test-db", "test-user", "test-pass");
        assert_eq!(hello.packet_type(), PacketType::ClientHello);
    }

    #[test]
    fn test_client_hello_serialize_deserialize() {
        let original = ClientHello::new("test-client", "test-db", "test-user", "test-pass")
            .with_version(2, 1, 3, 42)
            .with_protocol_version(54328);

        let mut buf = BytesMut::new();
        Packet::serialize(&original, &mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = <ClientHello as Packet>::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.client_name, deserialized.client_name);
        assert_eq!(original.client_version_major, deserialized.client_version_major);
        assert_eq!(original.client_version_minor, deserialized.client_version_minor);
        assert_eq!(original.client_version_patch, deserialized.client_version_patch);
        assert_eq!(original.client_revision, deserialized.client_revision);
        assert_eq!(original.database, deserialized.database);
        assert_eq!(original.username, deserialized.username);
        assert_eq!(original.password, deserialized.password);
        assert_eq!(original.protocol_version, deserialized.protocol_version);
    }
}
