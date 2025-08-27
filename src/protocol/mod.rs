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
use bytes::{BytesMut, BufMut};
use std::io;

/// ClickHouse protocol packet types
#[repr(u64)]
#[derive(PartialEq, Debug, Clone, Copy)]
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
    use std::io::{Cursor, ErrorKind};

    #[test]
    fn test_packet_type_conversion() {
        assert_eq!(PacketType::ClientHello.to_u64(), 0);
        assert_eq!(PacketType::ClientQuery.to_u64(), 1);
        assert_eq!(PacketType::ServerData.to_u64(), 101);
        assert_eq!(PacketType::ServerException.to_u64(), 102);

        assert_eq!(PacketType::from_u64(0), Some(PacketType::ClientHello));
        assert_eq!(PacketType::from_u64(1), Some(PacketType::ClientQuery));
        assert_eq!(PacketType::from_u64(100), Some(PacketType::ServerHello));
    }

    #[test]
    fn test_all_packet_types() {
        // Test all client packet types
        assert_eq!(PacketType::ClientHello.to_u64(), 0);
        assert_eq!(PacketType::ClientQuery.to_u64(), 1);
        assert_eq!(PacketType::ClientData.to_u64(), 2);
        assert_eq!(PacketType::ClientCancel.to_u64(), 3);
        assert_eq!(PacketType::ClientPing.to_u64(), 4);
        assert_eq!(PacketType::ClientTablesStatusRequest.to_u64(), 5);
        assert_eq!(PacketType::ClientKeepAlive.to_u64(), 6);
        assert_eq!(PacketType::ClientScp.to_u64(), 7);
        assert_eq!(PacketType::ClientQueryWithExternalTables.to_u64(), 8);
        assert_eq!(PacketType::ClientQueryWithExternalTables2.to_u64(), 9);

        // Test all server packet types
        assert_eq!(PacketType::ServerHello.to_u64(), 100);
        assert_eq!(PacketType::ServerData.to_u64(), 101);
        assert_eq!(PacketType::ServerException.to_u64(), 102);
        assert_eq!(PacketType::ServerProgress.to_u64(), 103);
        assert_eq!(PacketType::ServerPong.to_u64(), 104);
        assert_eq!(PacketType::ServerEndOfStream.to_u64(), 105);
        assert_eq!(PacketType::ServerProfileInfo.to_u64(), 106);
        assert_eq!(PacketType::ServerTotals.to_u64(), 107);
        assert_eq!(PacketType::ServerExtremes.to_u64(), 108);
        assert_eq!(PacketType::ServerTablesStatusResponse.to_u64(), 109);
        assert_eq!(PacketType::ServerLog.to_u64(), 110);
        assert_eq!(PacketType::ServerTableColumns.to_u64(), 111);
        assert_eq!(PacketType::ServerPartUUIDs.to_u64(), 112);
        assert_eq!(PacketType::ServerReadTaskRequest.to_u64(), 113);
        assert_eq!(PacketType::ServerProfileEvents.to_u64(), 114);
        assert_eq!(PacketType::ServerTimezoneUpdate.to_u64(), 115);
        assert_eq!(PacketType::ServerQueryPlan.to_u64(), 116);
        assert_eq!(PacketType::ServerQueryPlan2.to_u64(), 117);

        // Test from_u64 for all valid values
        assert_eq!(PacketType::from_u64(0), Some(PacketType::ClientHello));
        assert_eq!(PacketType::from_u64(1), Some(PacketType::ClientQuery));
        assert_eq!(PacketType::from_u64(2), Some(PacketType::ClientData));
        assert_eq!(PacketType::from_u64(3), Some(PacketType::ClientCancel));
        assert_eq!(PacketType::from_u64(4), Some(PacketType::ClientPing));
        assert_eq!(PacketType::from_u64(5), Some(PacketType::ClientTablesStatusRequest));
        assert_eq!(PacketType::from_u64(6), Some(PacketType::ClientKeepAlive));
        assert_eq!(PacketType::from_u64(7), Some(PacketType::ClientScp));
        assert_eq!(PacketType::from_u64(8), Some(PacketType::ClientQueryWithExternalTables));
        assert_eq!(PacketType::from_u64(9), Some(PacketType::ClientQueryWithExternalTables2));
        assert_eq!(PacketType::from_u64(100), Some(PacketType::ServerHello));
        assert_eq!(PacketType::from_u64(101), Some(PacketType::ServerData));
        assert_eq!(PacketType::from_u64(102), Some(PacketType::ServerException));
        assert_eq!(PacketType::from_u64(103), Some(PacketType::ServerProgress));
        assert_eq!(PacketType::from_u64(104), Some(PacketType::ServerPong));
        assert_eq!(PacketType::from_u64(105), Some(PacketType::ServerEndOfStream));
        assert_eq!(PacketType::from_u64(106), Some(PacketType::ServerProfileInfo));
        assert_eq!(PacketType::from_u64(107), Some(PacketType::ServerTotals));
        assert_eq!(PacketType::from_u64(108), Some(PacketType::ServerExtremes));
        assert_eq!(PacketType::from_u64(109), Some(PacketType::ServerTablesStatusResponse));
        assert_eq!(PacketType::from_u64(110), Some(PacketType::ServerLog));
        assert_eq!(PacketType::from_u64(111), Some(PacketType::ServerTableColumns));
        assert_eq!(PacketType::from_u64(112), Some(PacketType::ServerPartUUIDs));
        assert_eq!(PacketType::from_u64(113), Some(PacketType::ServerReadTaskRequest));
        assert_eq!(PacketType::from_u64(114), Some(PacketType::ServerProfileEvents));
        assert_eq!(PacketType::from_u64(115), Some(PacketType::ServerTimezoneUpdate));
        assert_eq!(PacketType::from_u64(116), Some(PacketType::ServerQueryPlan));
        assert_eq!(PacketType::from_u64(117), Some(PacketType::ServerQueryPlan2));

        // Test invalid values
        assert_eq!(PacketType::from_u64(10), None);
        assert_eq!(PacketType::from_u64(99), None);
        assert_eq!(PacketType::from_u64(118), None);
        assert_eq!(PacketType::from_u64(u64::MAX), None);
    }

    #[test]
    fn test_protocol_reader_new() {
        let data = b"test data";
        let cursor = Cursor::new(data);
        let reader = ProtocolReader::new(cursor);
        
        assert_eq!(reader.buffer.len(), 0);
    }

    #[test]
    fn test_protocol_writer_new() {
        let data = Vec::new();
        let writer = ProtocolWriter::new(data);
        
        assert_eq!(writer.buffer.len(), 0);
    }

    #[test]
    fn test_protocol_writer_write_packet() {
        let mut writer = ProtocolWriter::new(Vec::new());
        
        // Create a mock packet for testing
        let mock_packet = MockPacket {
            packet_type: PacketType::ClientHello,
            data: b"test".to_vec(),
        };
        
        let result = writer.write_packet(&mock_packet);
        assert!(result.is_ok());
        
        // Verify the written data contains header and body
        let written_data = writer.writer;
        assert_eq!(written_data.len(), 20); // 16 bytes header (8 bytes packet type + 8 bytes size) + 4 bytes body
    }

    #[test]
    fn test_protocol_reader_read_packet_unknown_type() {
        let mut data = Vec::new();
        
        // Write an unknown packet type (999)
        data.extend_from_slice(&999u64.to_le_bytes());
        data.extend_from_slice(&4u64.to_le_bytes()); // size
        data.extend_from_slice(b"test");
        
        let mut reader = ProtocolReader::new(Cursor::new(data));
        let result = reader.read_packet();
        
        assert!(result.is_err());
        if let Err(Error::Protocol(msg)) = result {
            assert!(msg.contains("Unknown packet type: 999"));
        } else {
            panic!("Expected Protocol error");
        }
    }

    #[test]
    fn test_protocol_reader_read_packet_io_error() {
        // Create a reader that will fail on read
        let failing_reader = FailingReader;
        let mut reader = ProtocolReader::new(failing_reader);
        
        let result = reader.read_packet();
        assert!(result.is_err());
        if let Err(Error::Network(_)) = result {
            // Expected
        } else {
            panic!("Expected Network error");
        }
    }

    #[test]
    fn test_protocol_reader_read_packet_server_hello() {
        let mut data = Vec::new();
        
        // Write ServerHello packet type (100)
        data.extend_from_slice(&100u64.to_le_bytes());
        data.extend_from_slice(&8u64.to_le_bytes()); // size
        data.extend_from_slice(b"testdata");
        
        let mut reader = ProtocolReader::new(Cursor::new(data));
        let result = reader.read_packet();
        
        // This should fail because the test data is not valid ServerHello data
        // but it should at least attempt to deserialize it
        assert!(result.is_err());
    }

    #[test]
    fn test_protocol_reader_read_packet_server_data() {
        let mut data = Vec::new();
        
        // ProtocolReader reads 16 bytes for header first, then the body
        // Header: 8 bytes packet type + 8 bytes size
        data.extend_from_slice(&101u64.to_le_bytes()); // ServerData packet type
        data.extend_from_slice(&23u64.to_le_bytes());  // size (23 bytes for minimal ServerData)
        
        // Body: valid ServerData format (23 bytes)
        // Block info: 1 byte (is_overflows) + 4 bytes (bucket_num) + 1 byte (is_bucket_number)
        data.push(0); // is_overflows = false
        data.extend_from_slice(&(-1i32).to_le_bytes()); // bucket_num = -1 (no bucket)
        data.push(0); // is_bucket_number = false
        
        // Compression method: 8 bytes (length) + 0 bytes (empty string)
        data.extend_from_slice(&0u64.to_le_bytes()); // length = 0
        
        // Compression level: 1 byte
        data.push(0); // level = 0 (none)
        
        // Block size: 8 bytes
        data.extend_from_slice(&0u64.to_le_bytes()); // block_size = 0
        
        // Verify our data structure: 16 bytes header + 23 bytes body = 39 bytes total
        assert_eq!(data.len(), 39);
        
        let mut reader = ProtocolReader::new(Cursor::new(data));
        let result = reader.read_packet();
        
        // This should now succeed because we have valid ServerData format
        assert!(result.is_ok());
    }

    #[test]
    fn test_protocol_reader_read_packet_server_exception() {
        let mut data = Vec::new();
        
        // ProtocolReader reads 16 bytes for header first, then the body
        // Header: 8 bytes packet type + 8 bytes size
        data.extend_from_slice(&102u64.to_le_bytes()); // ServerException packet type
        data.extend_from_slice(&44u64.to_le_bytes());  // size (44 bytes for minimal ServerException)
        
        // Body: valid ServerException format (44 bytes)
        // Message: 8 bytes (length) + 4 bytes (content)
        data.extend_from_slice(&4u64.to_le_bytes()); // message length = 4
        data.extend_from_slice(b"test"); // message content
        
        // Code: 4 bytes
        data.extend_from_slice(&1000u32.to_le_bytes()); // exception code
        
        // Name: 8 bytes (length) + 4 bytes (content)
        data.extend_from_slice(&4u64.to_le_bytes()); // name length = 4
        data.extend_from_slice(b"Test"); // name content
        
        // Stack trace: 8 bytes (length) + 0 bytes (empty)
        data.extend_from_slice(&0u64.to_le_bytes()); // stack trace length = 0
        
        // Nested exception: 8 bytes (flag) + 0 bytes (no nested)
        data.extend_from_slice(&0u64.to_le_bytes()); // no nested exception
        
        // Verify our data structure: 16 bytes header + 44 bytes body = 60 bytes total
        assert_eq!(data.len(), 60);
        
        let mut reader = ProtocolReader::new(Cursor::new(data));
        let result = reader.read_packet();
        
        // This should now succeed because we have valid ServerException format
        assert!(result.is_ok());
    }

    #[test]
    fn test_protocol_reader_read_packet_server_progress() {
        let mut data = Vec::new();
        
        // ProtocolReader reads 16 bytes for header first, then the body
        // Header: 8 bytes packet type + 8 bytes size
        data.extend_from_slice(&103u64.to_le_bytes()); // ServerProgress packet type
        data.extend_from_slice(&112u64.to_le_bytes()); // size (112 bytes for ServerProgress)
        
        // Body: valid ServerProgress format (112 bytes)
        // All fields are u64 (8 bytes) except threads and peak_threads which are u32 (4 bytes)
        data.extend_from_slice(&0u64.to_le_bytes()); // rows
        data.extend_from_slice(&0u64.to_le_bytes()); // bytes
        data.extend_from_slice(&0u64.to_le_bytes()); // total_rows
        data.extend_from_slice(&0u64.to_le_bytes()); // elapsed_ns
        data.extend_from_slice(&0u64.to_le_bytes()); // read_rows_per_second
        data.extend_from_slice(&0u64.to_le_bytes()); // read_bytes_per_second
        data.extend_from_slice(&0u64.to_le_bytes()); // total_rows_approx
        data.extend_from_slice(&0u64.to_le_bytes()); // written_rows
        data.extend_from_slice(&0u64.to_le_bytes()); // written_bytes
        data.extend_from_slice(&0u64.to_le_bytes()); // written_rows_per_second
        data.extend_from_slice(&0u64.to_le_bytes()); // written_bytes_per_second
        data.extend_from_slice(&0u64.to_le_bytes()); // memory_usage
        data.extend_from_slice(&0u64.to_le_bytes()); // peak_memory_usage
        data.extend_from_slice(&1u32.to_le_bytes()); // threads
        data.extend_from_slice(&1u32.to_le_bytes()); // peak_threads
        
        // Verify our data structure: 16 bytes header + 112 bytes body = 128 bytes total
        assert_eq!(data.len(), 128);
        
        let mut reader = ProtocolReader::new(Cursor::new(data));
        let result = reader.read_packet();
        
        // This should now succeed because we have valid ServerProgress format
        assert!(result.is_ok());
    }

    #[test]
    fn test_protocol_reader_read_packet_server_pong() {
        let mut data = Vec::new();
        
        // ProtocolReader reads 16 bytes for header first, then the body
        // Header: 8 bytes packet type + 8 bytes size
        data.extend_from_slice(&104u64.to_le_bytes()); // ServerPong packet type
        data.extend_from_slice(&41u64.to_le_bytes());  // size (41 bytes for minimal ServerPong)
        
        // Body: valid ServerPong format (41 bytes)
        // Timestamp: 8 bytes
        data.extend_from_slice(&1000000000u64.to_le_bytes()); // timestamp
        
        // Uptime: 8 bytes
        data.extend_from_slice(&3600u64.to_le_bytes()); // uptime
        
        // Version: 8 bytes (length) + 5 bytes (content)
        data.extend_from_slice(&5u64.to_le_bytes()); // version length = 5
        data.extend_from_slice(b"1.0.0"); // version content (5 bytes)
        
        // Server name: 8 bytes (length) + 4 bytes (content)
        data.extend_from_slice(&4u64.to_le_bytes()); // server name length = 4
        data.extend_from_slice(b"Test"); // server name content (4 bytes)
        
        // Verify our data structure: 16 bytes header + 41 bytes body = 57 bytes total
        assert_eq!(data.len(), 57);
        
        let mut reader = ProtocolReader::new(Cursor::new(data));
        let result = reader.read_packet();
        
        // This should now succeed because we have valid ServerPong format
        assert!(result.is_ok());
    }

    #[test]
    fn test_protocol_reader_read_packet_server_end_of_stream() {
        let mut data = Vec::new();
        
        // ProtocolReader reads 16 bytes for header first, then the body
        // Header: 8 bytes packet type + 8 bytes size
        data.extend_from_slice(&105u64.to_le_bytes()); // ServerEndOfStream packet type
        data.extend_from_slice(&32u64.to_le_bytes());  // size (32 bytes for minimal ServerEndOfStream)
        
        // Body: valid ServerEndOfStream format (32 bytes)
        // Query ID: 8 bytes (length) + 0 bytes (empty)
        data.extend_from_slice(&0u64.to_le_bytes()); // query ID length = 0
        
        // Final stats: 8 bytes (flag) + 0 bytes (no final stats)
        data.extend_from_slice(&0u64.to_le_bytes()); // no final stats
        
        // End reason: 8 bytes
        data.extend_from_slice(&0u64.to_le_bytes()); // EndReason::Normal
        
        // Message: 8 bytes (length) + 0 bytes (empty)
        data.extend_from_slice(&0u64.to_le_bytes()); // message length = 0
        
        // Verify our data structure: 16 bytes header + 32 bytes body = 48 bytes total
        assert_eq!(data.len(), 48);
        
        let mut reader = ProtocolReader::new(Cursor::new(data));
        let result = reader.read_packet();
        
        // This should now succeed because we have valid ServerEndOfStream format
        assert!(result.is_ok());
    }

    #[test]
    fn test_protocol_constants() {
        // Test constants module values
        assert_eq!(constants::DEFAULT_PROTOCOL_VERSION, 54428);
        assert_eq!(constants::DEFAULT_DATABASE, "default");
        assert_eq!(constants::DEFAULT_USERNAME, "default");
        assert_eq!(constants::DEFAULT_PASSWORD, "");
        assert_eq!(constants::DEFAULT_CLIENT_NAME, "clickhouse-rs");
        assert_eq!(constants::DEFAULT_CLIENT_VERSION, 1);
        assert_eq!(constants::MAX_PACKET_SIZE, 1024 * 1024 * 1024);
        assert_eq!(constants::DEFAULT_COMPRESSION_THRESHOLD, 1024);
    }

    #[test]
    fn test_protocol_writer_buffer_operations() {
        let mut writer = ProtocolWriter::new(Vec::new());
        
        // Test buffer clearing
        writer.buffer.put_slice(b"test data");
        assert_eq!(writer.buffer.len(), 9);
        
        writer.buffer.clear();
        assert_eq!(writer.buffer.len(), 0);
    }

    #[test]
    fn test_protocol_reader_buffer_operations() {
        let data = b"test data";
        let cursor = Cursor::new(data);
        let mut reader = ProtocolReader::new(cursor);
        
        // Test buffer resizing
        reader.buffer.resize(10, 0);
        assert_eq!(reader.buffer.len(), 10);
        
        // Test buffer clearing
        reader.buffer.clear();
        assert_eq!(reader.buffer.len(), 0);
    }

    #[test]
    fn test_packet_type_debug() {
        // Test Debug trait implementation
        let packet_type = PacketType::ClientHello;
        let debug_str = format!("{:?}", packet_type);
        assert_eq!(debug_str, "ClientHello");
    }

    #[test]
    fn test_packet_type_partial_eq() {
        // Test PartialEq trait implementation
        assert_eq!(PacketType::ClientHello, PacketType::ClientHello);
        assert_ne!(PacketType::ClientHello, PacketType::ClientQuery);
        assert_eq!(PacketType::ServerData, PacketType::ServerData);
        assert_ne!(PacketType::ServerData, PacketType::ServerException);
    }

    // Mock implementations for testing
    struct MockPacket {
        packet_type: PacketType,
        data: Vec<u8>,
    }

    impl Packet for MockPacket {
        fn packet_type(&self) -> PacketType {
            self.packet_type
        }

        fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
            buf.put_slice(&self.data);
            Ok(())
        }

        fn deserialize(_buf: &mut BytesMut) -> Result<Self> {
            unimplemented!("Not needed for this test")
        }
    }

    struct FailingReader;

    impl io::Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(ErrorKind::ConnectionRefused, "Connection failed"))
        }
    }
}
