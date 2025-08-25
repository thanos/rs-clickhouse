//! ClickHouse native protocol implementation

mod client_hello;
mod client_query;
mod client_data;
mod server_hello;
mod server_data;
mod server_exception;
mod server_progress;
mod server_pong;
mod server_end_of_stream;

pub use client_hello::ClientHello;
pub use client_query::ClientQuery;
pub use client_data::ClientData;
pub use server_hello::ServerHello;
pub use server_data::ServerData;
pub use server_exception::ServerException;
pub use server_progress::ServerProgress;
pub use server_pong::ServerPong;
pub use server_end_of_stream::ServerEndOfStream;

use crate::error::{Error, Result};
use crate::types::{Block, Value};
use bytes::BytesMut;
use std::io;

/// ClickHouse protocol packet types
#[repr(u64)]
pub enum PacketType {
    /// Client hello packet
    ClientHello = 0,
    /// Client query packet
    ClientQuery = 1,
    /// Client data packet
    ClientData = 2,
    /// Client cancel packet
    ClientCancel = 3,
    /// Client ping packet
    ClientPing = 4,
    /// Client tables status request packet
    ClientTablesStatusRequest = 5,
    /// Client keep alive packet
    ClientKeepAlive = 6,
    /// Client scp packet
    ClientScp = 7,
    /// Client query with external tables packet
    ClientQueryWithExternalTables = 8,
    /// Client query with external tables packet
    ClientQueryWithExternalTables2 = 9,
    /// Server hello packet
    ServerHello = 100,
    /// Server data packet
    ServerData = 101,
    /// Server exception packet
    ServerException = 102,
    /// Server progress packet
    ServerProgress = 103,
    /// Server pong packet
    ServerPong = 104,
    /// Server end of stream packet
    ServerEndOfStream = 105,
    /// Server profile info packet
    ServerProfileInfo = 106,
    /// Server totals packet
    ServerTotals = 107,
    /// Server extremes packet
    ServerExtremes = 108,
    /// Server tables status response packet
    ServerTablesStatusResponse = 109,
    /// Server log packet
    ServerLog = 110,
    /// Server table columns packet
    ServerTableColumns = 111,
    /// Server part UUIDs packet
    ServerPartUUIDs = 112,
    /// Server read task request packet
    ServerReadTaskRequest = 113,
    /// Server profile events packet
    ServerProfileEvents = 114,
    /// Server timezone update packet
    ServerTimezoneUpdate = 115,
    /// Server query plan packet
    ServerQueryPlan = 116,
    /// Server query plan packet
    ServerQueryPlan2 = 117,
}

impl PacketType {
    /// Get the packet type from a u64 value
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            0 => Some(PacketType::ClientHello),
            1 => Some(PacketType::ClientQuery),
            2 => Some(PacketType::ClientData),
            3 => Some(PacketType::ClientCancel),
            4 => Some(PacketType::ClientPing),
            5 => Some(PacketType::ClientTablesStatusRequest),
            6 => Some(PacketType::ClientKeepAlive),
            7 => Some(PacketType::ClientScp),
            8 => Some(PacketType::ClientQueryWithExternalTables),
            9 => Some(PacketType::ClientQueryWithExternalTables2),
            100 => Some(PacketType::ServerHello),
            101 => Some(PacketType::ServerData),
            102 => Some(PacketType::ServerException),
            103 => Some(PacketType::ServerProgress),
            104 => Some(PacketType::ServerPong),
            105 => Some(PacketType::ServerEndOfStream),
            106 => Some(PacketType::ServerProfileInfo),
            107 => Some(PacketType::ServerTotals),
            108 => Some(PacketType::ServerExtremes),
            109 => Some(PacketType::ServerTablesStatusResponse),
            110 => Some(PacketType::ServerLog),
            111 => Some(PacketType::ServerTableColumns),
            112 => Some(PacketType::ServerPartUUIDs),
            113 => Some(PacketType::ServerReadTaskRequest),
            114 => Some(PacketType::ServerProfileEvents),
            115 => Some(PacketType::ServerTimezoneUpdate),
            116 => Some(PacketType::ServerQueryPlan),
            117 => Some(PacketType::ServerQueryPlan2),
            _ => None,
        }
    }

    /// Convert to u64
    pub fn to_u64(&self) -> u64 {
        match self {
            PacketType::ClientHello => 0,
            PacketType::ClientQuery => 1,
            PacketType::ClientData => 2,
            PacketType::ClientCancel => 3,
            PacketType::ClientPing => 4,
            PacketType::ClientTablesStatusRequest => 5,
            PacketType::ClientKeepAlive => 6,
            PacketType::ClientScp => 7,
            PacketType::ClientQueryWithExternalTables => 8,
            PacketType::ClientQueryWithExternalTables2 => 9,
            PacketType::ServerHello => 100,
            PacketType::ServerData => 101,
            PacketType::ServerException => 102,
            PacketType::ServerProgress => 103,
            PacketType::ServerPong => 104,
            PacketType::ServerEndOfStream => 105,
            PacketType::ServerProfileInfo => 106,
            PacketType::ServerTotals => 107,
            PacketType::ServerExtremes => 108,
            PacketType::ServerTablesStatusResponse => 109,
            PacketType::ServerLog => 110,
            PacketType::ServerTableColumns => 111,
            PacketType::ServerPartUUIDs => 112,
            PacketType::ServerReadTaskRequest => 113,
            PacketType::ServerProfileEvents => 114,
            PacketType::ServerTimezoneUpdate => 115,
            PacketType::ServerQueryPlan => 116,
            PacketType::ServerQueryPlan2 => 117,
        }
    }
}

/// Protocol packet trait
pub trait Packet {
    /// Get the packet type
    fn packet_type(&self) -> PacketType;

    /// Serialize the packet to bytes
    fn serialize(&self, buf: &mut BytesMut) -> Result<()>;

    /// Deserialize the packet from bytes
    fn deserialize(buf: &mut BytesMut) -> Result<Self>
    where
        Self: Sized;
}

/// Protocol reader for reading packets from a stream
pub struct ProtocolReader<R> {
    reader: R,
    buffer: BytesMut,
}

impl<R> ProtocolReader<R>
where
    R: io::Read,
{
    /// Create a new protocol reader
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: BytesMut::new(),
        }
    }

    /// Read a packet from the stream
    pub fn read_packet(&mut self) -> Result<Box<dyn Packet>> {
        // Read packet header (type + size)
        let mut header = [0u8; 16];
        self.reader.read_exact(&mut header)?;

        let packet_type = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let packet_size = u64::from_le_bytes(header[8..16].try_into().unwrap());

        // Read packet body
        self.buffer.resize(packet_size as usize, 0);
        self.reader.read_exact(&mut self.buffer[..packet_size as usize])?;

        // Deserialize packet based on type
        let packet: Box<dyn Packet> = match PacketType::from_u64(packet_type) {
            Some(PacketType::ServerHello) => {
                Box::new(ServerHello::deserialize(&mut self.buffer)?)
            }
            Some(PacketType::ServerData) => {
                Box::new(ServerData::deserialize(&mut self.buffer)?)
            }
            Some(PacketType::ServerException) => {
                Box::new(ServerException::deserialize(&mut self.buffer)?)
            }
            Some(PacketType::ServerProgress) => {
                Box::new(ServerProgress::deserialize(&mut self.buffer)?)
            }
            Some(PacketType::ServerPong) => {
                Box::new(ServerPong::deserialize(&mut self.buffer)?)
            }
            Some(PacketType::ServerEndOfStream) => {
                Box::new(ServerEndOfStream::deserialize(&mut self.buffer)?)
            }
            _ => {
                return Err(Error::Protocol(format!(
                    "Unknown packet type: {}",
                    packet_type
                )));
            }
        };

        Ok(packet)
    }
}

/// Protocol writer for writing packets to a stream
pub struct ProtocolWriter<W> {
    writer: W,
    buffer: BytesMut,
}

impl<W> ProtocolWriter<W>
where
   W: io::Write,
{
    /// Create a new protocol writer
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            buffer: BytesMut::new(),
        }
    }

    /// Write a packet to the stream
    pub fn write_packet(&mut self, packet: &dyn Packet) -> Result<()> {
        // Clear buffer
        self.buffer.clear();

        // Serialize packet
        packet.serialize(&mut self.buffer)?;

        // Write packet header
        let packet_type = packet.packet_type().to_u64();
        let packet_size = self.buffer.len() as u64;

        self.writer.write_all(&packet_type.to_le_bytes())?;
        self.writer.write_all(&packet_size.to_le_bytes())?;

        // Write packet body
        self.writer.write_all(&self.buffer)?;
        self.writer.flush()?;

        Ok(())
    }
}

/// Protocol constants
pub mod constants {
    /// Default protocol version
    pub const DEFAULT_PROTOCOL_VERSION: u64 = 54428;
    
    /// Default database name
    pub const DEFAULT_DATABASE: &str = "default";
    
    /// Default username
    pub const DEFAULT_USERNAME: &str = "default";
    
    /// Default password
    pub const DEFAULT_PASSWORD: &str = "";
    
    /// Default client name
    pub const DEFAULT_CLIENT_NAME: &str = "clickhouse-rs";
    
    /// Default client version
    pub const DEFAULT_CLIENT_VERSION: u64 = 1;
    
    /// Maximum packet size
    pub const MAX_PACKET_SIZE: usize = 1024 * 1024 * 1024; // 1GB
    
    /// Default compression threshold
    pub const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024; // 1KB
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_type_conversion() {
        assert_eq!(PacketType::ClientHello.to_u64(), 0);
        assert_eq!(PacketType::ClientQuery.to_u64(), 1);
        assert_eq!(PacketType::ServerData.to_u64(), 1);
        assert_eq!(PacketType::ServerException.to_u64(), 2);

        assert_eq!(PacketType::from_u64(0), Some(PacketType::ClientHello));
        assert_eq!(PacketType::from_u64(1), Some(PacketType::ClientQuery));
        assert_eq!(PacketType::from_u64(100), None);
    }
}
