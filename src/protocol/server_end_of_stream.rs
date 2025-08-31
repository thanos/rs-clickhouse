//! Server End of Stream message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Server End of Stream message indicating the end of data transmission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerEndOfStream {
    /// Query ID (optional)
    pub query_id: Option<String>,
    /// Final statistics
    pub final_stats: Option<FinalStats>,
    /// End reason
    pub reason: EndReason,
    /// Additional message (optional)
    pub message: Option<String>,
}

/// Final query statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalStats {
    /// Total rows read
    pub total_rows_read: u64,
    /// Total bytes read
    pub total_bytes_read: u64,
    /// Total rows written
    pub total_rows_written: u64,
    /// Total bytes written
    pub total_bytes_written: u64,
    /// Total elapsed time in nanoseconds
    pub total_elapsed_ns: u64,
    /// Peak memory usage
    pub peak_memory_usage: u64,
    /// Peak threads used
    pub peak_threads: u32,
}

impl FinalStats {
    /// Create new final stats
    pub fn new() -> Self {
        Self {
            total_rows_read: 0,
            total_bytes_read: 0,
            total_rows_written: 0,
            total_bytes_written: 0,
            total_elapsed_ns: 0,
            peak_memory_usage: 0,
            peak_threads: 0,
        }
    }

    /// Set total rows read
    pub fn with_total_rows_read(mut self, total_rows_read: u64) -> Self {
        self.total_rows_read = total_rows_read;
        self
    }

    /// Set total bytes read
    pub fn with_total_bytes_read(mut self, total_bytes_read: u64) -> Self {
        self.total_bytes_read = total_bytes_read;
        self
    }

    /// Set total rows written
    pub fn with_total_rows_written(mut self, total_rows_written: u64) -> Self {
        self.total_rows_written = total_rows_written;
        self
    }

    /// Set total bytes written
    pub fn with_total_bytes_written(mut self, total_bytes_written: u64) -> Self {
        self.total_bytes_written = total_bytes_written;
        self
    }

    /// Set total elapsed time in nanoseconds
    pub fn with_total_elapsed_ns(mut self, total_elapsed_ns: u64) -> Self {
        self.total_elapsed_ns = total_elapsed_ns;
        self
    }

    /// Set peak memory usage
    pub fn with_peak_memory_usage(mut self, peak_memory_usage: u64) -> Self {
        self.peak_memory_usage = peak_memory_usage;
        self
    }

    /// Set peak threads
    pub fn with_peak_threads(mut self, peak_threads: u32) -> Self {
        self.peak_threads = peak_threads;
        self
    }

    /// Get total elapsed time in seconds
    pub fn total_elapsed_seconds(&self) -> f64 {
        self.total_elapsed_ns as f64 / 1_000_000_000.0
    }

    /// Get total elapsed time in milliseconds
    pub fn total_elapsed_millis(&self) -> u64 {
        self.total_elapsed_ns / 1_000_000
    }

    /// Get memory usage in MB
    pub fn peak_memory_usage_mb(&self) -> f64 {
        self.peak_memory_usage as f64 / (1024.0 * 1024.0)
    }

    /// Get total bytes read in MB
    pub fn total_bytes_read_mb(&self) -> f64 {
        self.total_bytes_read as f64 / (1024.0 * 1024.0)
    }

    /// Get total bytes written in MB
    pub fn total_bytes_written_mb(&self) -> f64 {
        self.total_bytes_written as f64 / (1024.0 * 1024.0)
    }

    /// Calculate read throughput in rows per second
    pub fn read_throughput_rows_per_second(&self) -> f64 {
        if self.total_elapsed_seconds() > 0.0 {
            self.total_rows_read as f64 / self.total_elapsed_seconds()
        } else {
            0.0
        }
    }

    /// Calculate read throughput in MB per second
    pub fn read_throughput_mb_per_second(&self) -> f64 {
        if self.total_elapsed_seconds() > 0.0 {
            self.total_bytes_read_mb() / self.total_elapsed_seconds()
        } else {
            0.0
        }
    }

    /// Calculate write throughput in rows per second
    pub fn write_throughput_rows_per_second(&self) -> f64 {
        if self.total_elapsed_seconds() > 0.0 {
            self.total_rows_written as f64 / self.total_elapsed_seconds()
        } else {
            0.0
        }
    }

    /// Calculate write throughput in MB per second
    pub fn write_throughput_mb_per_second(&self) -> f64 {
        if self.total_elapsed_seconds() > 0.0 {
            self.total_bytes_written_mb() / self.total_elapsed_seconds()
        } else {
            0.0
        }
    }
}

impl Default for FinalStats {
    fn default() -> Self {
        Self::new()
    }
}

/// End reason enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndReason {
    /// Normal completion
    Normal = 0,
    /// Query cancelled
    Cancelled = 1,
    /// Query failed
    Failed = 2,
    /// Query timeout
    Timeout = 3,
    /// Server shutdown
    ServerShutdown = 4,
    /// Connection lost
    ConnectionLost = 5,
    /// User requested
    UserRequested = 6,
}

impl EndReason {
    /// Get the end reason as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            EndReason::Normal => "normal",
            EndReason::Cancelled => "cancelled",
            EndReason::Failed => "failed",
            EndReason::Timeout => "timeout",
            EndReason::ServerShutdown => "server_shutdown",
            EndReason::ConnectionLost => "connection_lost",
            EndReason::UserRequested => "user_requested",
        }
    }

    /// Get the end reason from a string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "normal" => Some(EndReason::Normal),
            "cancelled" => Some(EndReason::Cancelled),
            "failed" => Some(EndReason::Failed),
            "timeout" => Some(EndReason::Timeout),
            "server_shutdown" => Some(EndReason::ServerShutdown),
            "connection_lost" => Some(EndReason::ConnectionLost),
            "user_requested" => Some(EndReason::UserRequested),
            _ => None,
        }
    }

    /// Check if the end reason indicates success
    pub fn is_success(&self) -> bool {
        matches!(self, EndReason::Normal)
    }

    /// Check if the end reason indicates failure
    pub fn is_failure(&self) -> bool {
        matches!(self, EndReason::Failed | EndReason::Timeout | EndReason::ServerShutdown | EndReason::ConnectionLost)
    }

    /// Check if the end reason indicates cancellation
    pub fn is_cancelled(&self) -> bool {
        matches!(self, EndReason::Cancelled | EndReason::UserRequested)
    }
}

impl ServerEndOfStream {
    /// Create a new Server End of Stream message
    pub fn new(reason: EndReason) -> Self {
        Self {
            query_id: None,
            final_stats: None,
            reason,
            message: None,
        }
    }

    /// Set query ID
    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.query_id = Some(query_id.into());
        self
    }

    /// Set final stats
    pub fn with_final_stats(mut self, final_stats: FinalStats) -> Self {
        self.final_stats = Some(final_stats);
        self
    }

    /// Set message
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Get the query ID
    pub fn query_id(&self) -> Option<&str> {
        self.query_id.as_deref()
    }

    /// Get the final stats
    pub fn final_stats(&self) -> Option<&FinalStats> {
        self.final_stats.as_ref()
    }

    /// Get the end reason
    pub fn reason(&self) -> EndReason {
        self.reason
    }

    /// Get the message
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Check if the stream ended successfully
    pub fn is_success(&self) -> bool {
        self.reason.is_success()
    }

    /// Check if the stream ended with failure
    pub fn is_failure(&self) -> bool {
        self.reason.is_failure()
    }

    /// Check if the stream was cancelled
    pub fn is_cancelled(&self) -> bool {
        self.reason.is_cancelled()
    }

    /// Check if final stats are available
    pub fn has_final_stats(&self) -> bool {
        self.final_stats.is_some()
    }
}

impl Packet for ServerEndOfStream {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerEndOfStream
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write query ID
        if let Some(ref query_id) = self.query_id {
            buf.put_u64_le(query_id.len() as u64);
            buf.extend_from_slice(query_id.as_bytes());
        } else {
            buf.put_u64_le(0);
        }

        // Write final stats
        if let Some(ref final_stats) = self.final_stats {
            buf.put_u64_le(1); // Has final stats
            buf.put_u64_le(final_stats.total_rows_read);
            buf.put_u64_le(final_stats.total_bytes_read);
            buf.put_u64_le(final_stats.total_rows_written);
            buf.put_u64_le(final_stats.total_bytes_written);
            buf.put_u64_le(final_stats.total_elapsed_ns);
            buf.put_u64_le(final_stats.peak_memory_usage);
            buf.put_u32_le(final_stats.peak_threads);
        } else {
            buf.put_u64_le(0); // No final stats
        }

        // Write end reason
        buf.put_u64_le(self.reason as u64);

        // Write message
        if let Some(ref message) = self.message {
            buf.put_u64_le(message.len() as u64);
            buf.extend_from_slice(message.as_bytes());
        } else {
            buf.put_u64_le(0);
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

        // Read final stats
        let has_final_stats = buf.get_u64_le() != 0;
        let final_stats = if has_final_stats {
            let total_rows_read = buf.get_u64_le();
            let total_bytes_read = buf.get_u64_le();
            let total_rows_written = buf.get_u64_le();
            let total_bytes_written = buf.get_u64_le();
            let total_elapsed_ns = buf.get_u64_le();
            let peak_memory_usage = buf.get_u64_le();
            let peak_threads = buf.get_u32_le();

            Some(FinalStats {
                total_rows_read,
                total_bytes_read,
                total_rows_written,
                total_bytes_written,
                total_elapsed_ns,
                peak_memory_usage,
                peak_threads,
            })
        } else {
            None
        };

        // Read end reason
        let reason_value = buf.get_u64_le();
        let reason = match reason_value {
            0 => EndReason::Normal,
            1 => EndReason::Cancelled,
            2 => EndReason::Failed,
            3 => EndReason::Timeout,
            4 => EndReason::ServerShutdown,
            5 => EndReason::ConnectionLost,
            6 => EndReason::UserRequested,
            _ => return Err(Error::Protocol("Invalid end reason".to_string())),
        };

        // Read message
        let message_len = buf.get_u64_le() as usize;
        let message = if message_len > 0 {
            if buf.remaining() < message_len {
                return Err(Error::Protocol("Insufficient data for message".to_string()));
            }
            Some(String::from_utf8_lossy(&buf.copy_to_bytes(message_len)).to_string())
        } else {
            None
        };

        Ok(Self {
            query_id,
            final_stats,
            reason,
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Packet;

    #[test]
    fn test_server_end_of_stream_new() {
        let eos = ServerEndOfStream::new(EndReason::Normal);
        assert_eq!(eos.reason(), EndReason::Normal);
        assert!(eos.is_success());
        assert!(!eos.is_failure());
        assert!(!eos.is_cancelled());
        assert!(eos.query_id().is_none());
        assert!(eos.final_stats().is_none());
        assert!(eos.message().is_none());
    }

    #[test]
    fn test_server_end_of_stream_with_query_id() {
        let eos = ServerEndOfStream::new(EndReason::Normal)
            .with_query_id("test-query-123");
        assert_eq!(eos.query_id(), Some("test-query-123"));
    }

    #[test]
    fn test_server_end_of_stream_with_final_stats() {
        let stats = FinalStats::new()
            .with_total_rows_read(1000)
            .with_total_bytes_read(1024);
        let eos = ServerEndOfStream::new(EndReason::Normal)
            .with_final_stats(stats);
        assert!(eos.has_final_stats());
        assert_eq!(eos.final_stats().unwrap().total_rows_read, 1000);
    }

    #[test]
    fn test_server_end_of_stream_with_message() {
        let eos = ServerEndOfStream::new(EndReason::Normal)
            .with_message("Query completed successfully");
        assert_eq!(eos.message(), Some("Query completed successfully"));
    }

    #[test]
    fn test_end_reason_conversion() {
        assert_eq!(EndReason::from_str("normal"), Some(EndReason::Normal));
        assert_eq!(EndReason::from_str("cancelled"), Some(EndReason::Cancelled));
        assert_eq!(EndReason::from_str("failed"), Some(EndReason::Failed));
        assert_eq!(EndReason::from_str("timeout"), Some(EndReason::Timeout));
        assert_eq!(EndReason::from_str("server_shutdown"), Some(EndReason::ServerShutdown));
        assert_eq!(EndReason::from_str("connection_lost"), Some(EndReason::ConnectionLost));
        assert_eq!(EndReason::from_str("user_requested"), Some(EndReason::UserRequested));
        assert_eq!(EndReason::from_str("unknown"), None);
    }

    #[test]
    fn test_end_reason_checks() {
        assert!(EndReason::Normal.is_success());
        assert!(!EndReason::Normal.is_failure());
        assert!(!EndReason::Normal.is_cancelled());

        assert!(!EndReason::Failed.is_success());
        assert!(EndReason::Failed.is_failure());
        assert!(!EndReason::Failed.is_cancelled());

        assert!(!EndReason::Cancelled.is_success());
        assert!(!EndReason::Cancelled.is_failure());
        assert!(EndReason::Cancelled.is_cancelled());
    }

    #[test]
    fn test_final_stats_new() {
        let stats = FinalStats::new();
        assert_eq!(stats.total_rows_read, 0);
        assert_eq!(stats.total_bytes_read, 0);
        assert_eq!(stats.total_rows_written, 0);
        assert_eq!(stats.total_bytes_written, 0);
        assert_eq!(stats.total_elapsed_ns, 0);
        assert_eq!(stats.peak_memory_usage, 0);
        assert_eq!(stats.peak_threads, 0);
    }

    #[test]
    fn test_final_stats_conversions() {
        let stats = FinalStats::new()
            .with_total_elapsed_ns(1_500_000_000)
            .with_peak_memory_usage(1024 * 1024)
            .with_total_bytes_read(1024 * 1024)
            .with_total_bytes_written(512 * 1024);

        assert_eq!(stats.total_elapsed_seconds(), 1.5);
        assert_eq!(stats.total_elapsed_millis(), 1500);
        assert_eq!(stats.peak_memory_usage_mb(), 1.0);
        assert_eq!(stats.total_bytes_read_mb(), 1.0);
        assert_eq!(stats.total_bytes_written_mb(), 0.5);
    }

    #[test]
    fn test_final_stats_throughput() {
        let stats = FinalStats::new()
            .with_total_rows_read(1000)
            .with_total_bytes_read(1024 * 1024)
            .with_total_rows_written(500)
            .with_total_bytes_written(512 * 1024)
            .with_total_elapsed_ns(1_000_000_000);

        assert_eq!(stats.read_throughput_rows_per_second(), 1000.0);
        assert_eq!(stats.read_throughput_mb_per_second(), 1.0);
        assert_eq!(stats.write_throughput_rows_per_second(), 500.0);
        assert_eq!(stats.write_throughput_mb_per_second(), 0.5);
    }

    #[test]
    fn test_server_end_of_stream_packet_type() {
        let eos = ServerEndOfStream::new(EndReason::Normal);
        assert_eq!(eos.packet_type(), PacketType::ServerEndOfStream);
    }

    #[test]
    fn test_server_end_of_stream_serialize_deserialize() {
        let stats = FinalStats::new()
            .with_total_rows_read(1000)
            .with_total_bytes_read(1024);
        let original = ServerEndOfStream::new(EndReason::Normal)
            .with_query_id("test-query-123")
            .with_final_stats(stats)
            .with_message("Query completed successfully");

        let mut buf = BytesMut::new();
        Packet::serialize(&original, &mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = <ServerEndOfStream as Packet>::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.query_id, deserialized.query_id);
        assert_eq!(original.reason, deserialized.reason);
        assert_eq!(original.message, deserialized.message);
        assert!(original.final_stats.is_some());
        assert!(deserialized.final_stats.is_some());
        assert_eq!(original.final_stats.as_ref().unwrap().total_rows_read, 
                   deserialized.final_stats.as_ref().unwrap().total_rows_read);
    }
}
