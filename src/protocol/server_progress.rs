//! Server Progress message for ClickHouse native protocol

use super::{Packet, PacketType};
use crate::error::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Server Progress message for query progress updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerProgress {
    /// Rows read
    pub rows: u64,
    /// Bytes read
    pub bytes: u64,
    /// Total rows to read
    pub total_rows: u64,
    /// Elapsed time in nanoseconds
    pub elapsed_ns: u64,
    /// Read rows per second
    pub read_rows_per_second: u64,
    /// Read bytes per second
    pub read_bytes_per_second: u64,
    /// Total rows to read
    pub total_rows_approx: u64,
    /// Written rows
    pub written_rows: u64,
    /// Written bytes
    pub written_bytes: u64,
    /// Written rows per second
    pub written_rows_per_second: u64,
    /// Written bytes per second
    pub written_bytes_per_second: u64,
    /// Memory usage
    pub memory_usage: u64,
    /// Peak memory usage
    pub peak_memory_usage: u64,
    /// Threads
    pub threads: u32,
    /// Peak threads
    pub peak_threads: u32,
}

impl ServerProgress {
    /// Create a new Server Progress message
    pub fn new() -> Self {
        Self {
            rows: 0,
            bytes: 0,
            total_rows: 0,
            elapsed_ns: 0,
            read_rows_per_second: 0,
            read_bytes_per_second: 0,
            total_rows_approx: 0,
            written_rows: 0,
            written_bytes: 0,
            written_rows_per_second: 0,
            written_bytes_per_second: 0,
            memory_usage: 0,
            peak_memory_usage: 0,
            threads: 0,
            peak_threads: 0,
        }
    }

    /// Set rows read
    pub fn with_rows(mut self, rows: u64) -> Self {
        self.rows = rows;
        self
    }

    /// Set bytes read
    pub fn with_bytes(mut self, bytes: u64) -> Self {
        self.bytes = bytes;
        self
    }

    /// Set total rows
    pub fn with_total_rows(mut self, total_rows: u64) -> Self {
        self.total_rows = total_rows;
        self
    }

    /// Set elapsed time in nanoseconds
    pub fn with_elapsed_ns(mut self, elapsed_ns: u64) -> Self {
        self.elapsed_ns = elapsed_ns;
        self
    }

    /// Set read rows per second
    pub fn with_read_rows_per_second(mut self, read_rows_per_second: u64) -> Self {
        self.read_rows_per_second = read_rows_per_second;
        self
    }

    /// Set read bytes per second
    pub fn with_read_bytes_per_second(mut self, read_bytes_per_second: u64) -> Self {
        self.read_bytes_per_second = read_bytes_per_second;
        self
    }

    /// Set total rows approx
    pub fn with_total_rows_approx(mut self, total_rows_approx: u64) -> Self {
        self.total_rows_approx = total_rows_approx;
        self
    }

    /// Set written rows
    pub fn with_written_rows(mut self, written_rows: u64) -> Self {
        self.written_rows = written_rows;
        self
    }

    /// Set written bytes
    pub fn with_written_bytes(mut self, written_bytes: u64) -> Self {
        self.written_bytes = written_bytes;
        self
    }

    /// Set written rows per second
    pub fn with_written_rows_per_second(mut self, written_rows_per_second: u64) -> Self {
        self.written_rows_per_second = written_rows_per_second;
        self
    }

    /// Set written bytes per second
    pub fn with_written_bytes_per_second(mut self, written_bytes_per_second: u64) -> Self {
        self.written_bytes_per_second = written_bytes_per_second;
        self
    }

    /// Set memory usage
    pub fn with_memory_usage(mut self, memory_usage: u64) -> Self {
        self.memory_usage = memory_usage;
        self
    }

    /// Set peak memory usage
    pub fn with_peak_memory_usage(mut self, peak_memory_usage: u64) -> Self {
        self.peak_memory_usage = peak_memory_usage;
        self
    }

    /// Set threads
    pub fn with_threads(mut self, threads: u32) -> Self {
        self.threads = threads;
        self
    }

    /// Set peak threads
    pub fn with_peak_threads(mut self, peak_threads: u32) -> Self {
        self.peak_threads = peak_threads;
        self
    }

    /// Get the rows read
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Get the bytes read
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Get the total rows
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Get the elapsed time in nanoseconds
    pub fn elapsed_ns(&self) -> u64 {
        self.elapsed_ns
    }

    /// Get the elapsed time in seconds
    pub fn elapsed_seconds(&self) -> f64 {
        self.elapsed_ns as f64 / 1_000_000_000.0
    }

    /// Get the read rows per second
    pub fn read_rows_per_second(&self) -> u64 {
        self.read_rows_per_second
    }

    /// Get the read bytes per second
    pub fn read_bytes_per_second(&self) -> u64 {
        self.read_bytes_per_second
    }

    /// Get the total rows approx
    pub fn total_rows_approx(&self) -> u64 {
        self.total_rows_approx
    }

    /// Get the written rows
    pub fn written_rows(&self) -> u64 {
        self.written_rows
    }

    /// Get the written bytes
    pub fn written_bytes(&self) -> u64 {
        self.written_bytes
    }

    /// Get the written rows per second
    pub fn written_rows_per_second(&self) -> u64 {
        self.written_rows_per_second
    }

    /// Get the written bytes per second
    pub fn written_bytes_per_second(&self) -> u64 {
        self.written_bytes_per_second
    }

    /// Get the memory usage
    pub fn memory_usage(&self) -> u64 {
        self.memory_usage
    }

    /// Get the peak memory usage
    pub fn peak_memory_usage(&self) -> u64 {
        self.peak_memory_usage
    }

    /// Get the threads
    pub fn threads(&self) -> u32 {
        self.threads
    }

    /// Get the peak threads
    pub fn peak_threads(&self) -> u32 {
        self.peak_threads
    }

    /// Calculate progress percentage
    pub fn progress_percentage(&self) -> f64 {
        if self.total_rows == 0 {
            0.0
        } else {
            (self.rows as f64 / self.total_rows as f64) * 100.0
        }
    }

    /// Check if progress is complete
    pub fn is_complete(&self) -> bool {
        self.rows >= self.total_rows
    }

    /// Get memory usage in MB
    pub fn memory_usage_mb(&self) -> f64 {
        self.memory_usage as f64 / (1024.0 * 1024.0)
    }

    /// Get peak memory usage in MB
    pub fn peak_memory_usage_mb(&self) -> f64 {
        self.peak_memory_usage as f64 / (1024.0 * 1024.0)
    }

    /// Get bytes in MB
    pub fn bytes_mb(&self) -> f64 {
        self.bytes as f64 / (1024.0 * 1024.0)
    }

    /// Get written bytes in MB
    pub fn written_bytes_mb(&self) -> f64 {
        self.written_bytes as f64 / (1024.0 * 1024.0)
    }
}

impl Default for ServerProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl Packet for ServerProgress {
    fn packet_type(&self) -> PacketType {
        PacketType::ServerProgress
    }

    fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        // Write rows read
        buf.put_u64_le(self.rows);

        // Write bytes read
        buf.put_u64_le(self.bytes);

        // Write total rows
        buf.put_u64_le(self.total_rows);

        // Write elapsed time in nanoseconds
        buf.put_u64_le(self.elapsed_ns);

        // Write read rows per second
        buf.put_u64_le(self.read_rows_per_second);

        // Write read bytes per second
        buf.put_u64_le(self.read_bytes_per_second);

        // Write total rows approx
        buf.put_u64_le(self.total_rows_approx);

        // Write written rows
        buf.put_u64_le(self.written_rows);

        // Write written bytes
        buf.put_u64_le(self.written_bytes);

        // Write written rows per second
        buf.put_u64_le(self.written_rows_per_second);

        // Write written bytes per second
        buf.put_u64_le(self.written_bytes_per_second);

        // Write memory usage
        buf.put_u64_le(self.memory_usage);

        // Write peak memory usage
        buf.put_u64_le(self.peak_memory_usage);

        // Write threads
        buf.put_u32_le(self.threads);

        // Write peak threads
        buf.put_u32_le(self.peak_threads);

        Ok(())
    }

    fn deserialize(buf: &mut BytesMut) -> Result<Self> {
        // Read rows read
        let rows = buf.get_u64_le();

        // Read bytes read
        let bytes = buf.get_u64_le();

        // Read total rows
        let total_rows = buf.get_u64_le();

        // Read elapsed time in nanoseconds
        let elapsed_ns = buf.get_u64_le();

        // Read read rows per second
        let read_rows_per_second = buf.get_u64_le();

        // Read read bytes per second
        let read_bytes_per_second = buf.get_u64_le();

        // Read total rows approx
        let total_rows_approx = buf.get_u64_le();

        // Read written rows
        let written_rows = buf.get_u64_le();

        // Read written bytes
        let written_bytes = buf.get_u64_le();

        // Read written rows per second
        let written_rows_per_second = buf.get_u64_le();

        // Read written bytes per second
        let written_bytes_per_second = buf.get_u64_le();

        // Read memory usage
        let memory_usage = buf.get_u64_le();

        // Read peak memory usage
        let peak_memory_usage = buf.get_u64_le();

        // Read threads
        let threads = buf.get_u32_le();

        // Read peak threads
        let peak_threads = buf.get_u32_le();

        Ok(Self {
            rows,
            bytes,
            total_rows,
            elapsed_ns,
            read_rows_per_second,
            read_bytes_per_second,
            total_rows_approx,
            written_rows,
            written_bytes,
            written_rows_per_second,
            written_bytes_per_second,
            memory_usage,
            peak_memory_usage,
            threads,
            peak_threads,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_progress_new() {
        let progress = ServerProgress::new();
        assert_eq!(progress.rows(), 0);
        assert_eq!(progress.bytes(), 0);
        assert_eq!(progress.total_rows(), 0);
        assert_eq!(progress.elapsed_ns(), 0);
        assert_eq!(progress.read_rows_per_second(), 0);
        assert_eq!(progress.read_bytes_per_second(), 0);
        assert_eq!(progress.total_rows_approx(), 0);
        assert_eq!(progress.written_rows(), 0);
        assert_eq!(progress.written_bytes(), 0);
        assert_eq!(progress.written_rows_per_second(), 0);
        assert_eq!(progress.written_bytes_per_second(), 0);
        assert_eq!(progress.memory_usage(), 0);
        assert_eq!(progress.peak_memory_usage(), 0);
        assert_eq!(progress.threads(), 0);
        assert_eq!(progress.peak_threads(), 0);
    }

    #[test]
    fn test_server_progress_with_rows() {
        let progress = ServerProgress::new().with_rows(1000);
        assert_eq!(progress.rows(), 1000);
    }

    #[test]
    fn test_server_progress_with_bytes() {
        let progress = ServerProgress::new().with_bytes(1024);
        assert_eq!(progress.bytes(), 1024);
    }

    #[test]
    fn test_server_progress_with_total_rows() {
        let progress = ServerProgress::new().with_total_rows(5000);
        assert_eq!(progress.total_rows(), 5000);
    }

    #[test]
    fn test_server_progress_with_elapsed_ns() {
        let progress = ServerProgress::new().with_elapsed_ns(1_000_000_000);
        assert_eq!(progress.elapsed_ns(), 1_000_000_000);
        assert_eq!(progress.elapsed_seconds(), 1.0);
    }

    #[test]
    fn test_server_progress_progress_percentage() {
        let progress = ServerProgress::new()
            .with_rows(500)
            .with_total_rows(1000);
        assert_eq!(progress.progress_percentage(), 50.0);
    }

    #[test]
    fn test_server_progress_is_complete() {
        let progress = ServerProgress::new()
            .with_rows(1000)
            .with_total_rows(1000);
        assert!(progress.is_complete());
    }

    #[test]
    fn test_server_progress_memory_usage_mb() {
        let progress = ServerProgress::new().with_memory_usage(1024 * 1024);
        assert_eq!(progress.memory_usage_mb(), 1.0);
    }

    #[test]
    fn test_server_progress_bytes_mb() {
        let progress = ServerProgress::new().with_bytes(1024 * 1024);
        assert_eq!(progress.bytes_mb(), 1.0);
    }

    #[test]
    fn test_server_progress_packet_type() {
        let progress = ServerProgress::new();
        assert_eq!(progress.packet_type(), PacketType::ServerProgress);
    }

    #[test]
    fn test_server_progress_serialize_deserialize() {
        let original = ServerProgress::new()
            .with_rows(1000)
            .with_bytes(1024)
            .with_total_rows(5000)
            .with_elapsed_ns(1_000_000_000)
            .with_read_rows_per_second(100)
            .with_read_bytes_per_second(1024)
            .with_total_rows_approx(5000)
            .with_written_rows(500)
            .with_written_bytes(512)
            .with_written_rows_per_second(50)
            .with_written_bytes_per_second(512)
            .with_memory_usage(1024 * 1024)
            .with_peak_memory_usage(2 * 1024 * 1024)
            .with_threads(4)
            .with_peak_threads(8);

        let mut buf = BytesMut::new();
        original.serialize(&mut buf).unwrap();

        let mut read_buf = buf;
        let deserialized = ServerProgress::deserialize(&mut read_buf).unwrap();

        assert_eq!(original.rows, deserialized.rows);
        assert_eq!(original.bytes, deserialized.bytes);
        assert_eq!(original.total_rows, deserialized.total_rows);
        assert_eq!(original.elapsed_ns, deserialized.elapsed_ns);
        assert_eq!(original.read_rows_per_second, deserialized.read_rows_per_second);
        assert_eq!(original.read_bytes_per_second, deserialized.read_bytes_per_second);
        assert_eq!(original.total_rows_approx, deserialized.total_rows_approx);
        assert_eq!(original.written_rows, deserialized.written_rows);
        assert_eq!(original.written_bytes, deserialized.written_bytes);
        assert_eq!(original.written_rows_per_second, deserialized.written_rows_per_second);
        assert_eq!(original.written_bytes_per_second, deserialized.written_bytes_per_second);
        assert_eq!(original.memory_usage, deserialized.memory_usage);
        assert_eq!(original.peak_memory_usage, deserialized.peak_memory_usage);
        assert_eq!(original.threads, deserialized.threads);
        assert_eq!(original.peak_threads, deserialized.peak_threads);
    }
}
