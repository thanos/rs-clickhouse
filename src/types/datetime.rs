//! Date and time data types for ClickHouse

use super::Value;
use chrono::{DateTime as ChronoDateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Date type (date without time)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Date(pub NaiveDate);

/// DateTime type (date with time, no timezone)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateTime(pub NaiveDateTime);

/// DateTime64 type (date with time and subsecond precision, no timezone)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateTime64(pub NaiveDateTime);

impl Date {
    /// Create a new Date from year, month, and day
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, day).map(Date)
    }

    /// Create a new Date from a NaiveDate
    pub fn new(date: NaiveDate) -> Self {
        Date(date)
    }

    /// Get the year
    pub fn year(&self) -> i32 {
        self.0.year()
    }

    /// Get the month
    pub fn month(&self) -> u32 {
        self.0.month()
    }

    /// Get the day
    pub fn day(&self) -> u32 {
        self.0.day()
    }

    /// Get the weekday
    pub fn weekday(&self) -> chrono::Weekday {
        self.0.weekday()
    }

    /// Get the underlying NaiveDate
    pub fn as_naive_date(&self) -> NaiveDate {
        self.0
    }

    /// Format the date using the specified format
    pub fn format<'a>(&self, fmt: &'a str) -> chrono::format::DelayedFormat<chrono::format::StrftimeItems<'a>> {
        self.0.format(fmt)
    }

    /// Get today's date in UTC
    pub fn today() -> Self {
        Date(Utc::now().naive_utc().date())
    }

    /// Add a duration to the date
    pub fn add_days(&self, days: i64) -> Self {
        Date(self.0 + chrono::Duration::days(days))
    }

    /// Subtract a duration from the date
    pub fn sub_days(&self, days: i64) -> Self {
        Date(self.0 - chrono::Duration::days(days))
    }
}

impl DateTime {
    /// Create a new DateTime from year, month, day, hour, minute, and second
    pub fn from_ymd_hms(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> Option<Self> {
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let time = NaiveTime::from_hms_opt(hour, minute, second)?;
        Some(DateTime(NaiveDateTime::new(date, time)))
    }

    /// Create a new DateTime from a NaiveDateTime
    pub fn new(datetime: NaiveDateTime) -> Self {
        DateTime(datetime)
    }

    /// Get the underlying NaiveDateTime
    pub fn as_naive_datetime(&self) -> NaiveDateTime {
        self.0
    }

    /// Get the date part
    pub fn date(&self) -> Date {
        Date(self.0.date())
    }

    /// Get the time part
    pub fn time(&self) -> chrono::NaiveTime {
        self.0.time()
    }

    /// Get the year
    pub fn year(&self) -> i32 {
        self.0.year()
    }

    /// Get the month
    pub fn month(&self) -> u32 {
        self.0.month()
    }

    /// Get the day
    pub fn day(&self) -> u32 {
        self.0.day()
    }

    /// Get the hour
    pub fn hour(&self) -> u32 {
        self.0.hour()
    }

    /// Get the minute
    pub fn minute(&self) -> u32 {
        self.0.minute()
    }

    /// Get the second
    pub fn second(&self) -> u32 {
        self.0.second()
    }

    /// Format the datetime using the specified format
    pub fn format<'a>(&self, fmt: &'a str) -> chrono::format::DelayedFormat<chrono::format::StrftimeItems<'a>> {
        self.0.format(fmt)
    }

    /// Get the current datetime in UTC
    pub fn now() -> Self {
        DateTime(Utc::now().naive_utc())
    }

    /// Add a duration to the datetime
    pub fn add_seconds(&self, seconds: i64) -> Self {
        DateTime(self.0 + chrono::Duration::seconds(seconds))
    }

    /// Subtract a duration from the datetime
    pub fn sub_seconds(&self, seconds: i64) -> Self {
        DateTime(self.0 - chrono::Duration::seconds(seconds))
    }

    /// Convert to UTC DateTime
    pub fn with_timezone<Tz: TimeZone>(&self, tz: Tz) -> chrono::DateTime<Tz> {
        tz.from_local_datetime(&self.0).earliest().unwrap_or_else(|| {
            // Fallback to UTC if conversion fails
            let utc_dt = Utc.from_local_datetime(&self.0).earliest().unwrap();
            utc_dt.with_timezone(&tz)
        })
    }
}

impl DateTime64 {
    /// Create a new DateTime64 from year, month, day, hour, minute, second, and nanoseconds
    pub fn from_ymd_hms_ns(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        nanoseconds: u32,
    ) -> Option<Self> {
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanoseconds)?;
        Some(DateTime64(NaiveDateTime::new(date, time)))
    }

    /// Create a new DateTime64 from a NaiveDateTime
    pub fn new(datetime: NaiveDateTime) -> Self {
        DateTime64(datetime)
    }

    /// Get the underlying NaiveDateTime
    pub fn as_naive_datetime(&self) -> NaiveDateTime {
        self.0
    }

    /// Get the date part
    pub fn date(&self) -> Date {
        Date(self.0.date())
    }

    /// Get the time part
    pub fn time(&self) -> chrono::NaiveTime {
        self.0.time()
    }

    /// Get nanoseconds
    pub fn nanosecond(&self) -> u32 {
        self.0.nanosecond()
    }

    /// Format the datetime64 using the specified format
    pub fn format<'a>(&self, fmt: &'a str) -> chrono::format::DelayedFormat<chrono::format::StrftimeItems<'a>> {
        self.0.format(fmt)
    }

    /// Get the current datetime64 in UTC
    pub fn now() -> Self {
        DateTime64(Utc::now().naive_utc())
    }

    /// Add a duration to the datetime64
    pub fn add_nanoseconds(&self, nanoseconds: i64) -> Self {
        DateTime64(self.0 + chrono::Duration::nanoseconds(nanoseconds))
    }

    /// Subtract a duration from the datetime64
    pub fn sub_nanoseconds(&self, nanoseconds: i64) -> Self {
        DateTime64(self.0 - chrono::Duration::nanoseconds(nanoseconds))
    }
}

// Implement Display for all datetime types
impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%d"))
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%d %H:%M:%S"))
    }
}

impl fmt::Display for DateTime64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%d %H:%M:%S%.9f"))
    }
}

// Implement From traits for conversions
impl From<NaiveDate> for Date {
    fn from(date: NaiveDate) -> Self {
        Date(date)
    }
}

impl From<NaiveDateTime> for DateTime {
    fn from(datetime: NaiveDateTime) -> Self {
        DateTime(datetime)
    }
}

impl From<NaiveDateTime> for DateTime64 {
    fn from(datetime: NaiveDateTime) -> Self {
        DateTime64(datetime)
    }
}

impl From<Date> for NaiveDate {
    fn from(date: Date) -> Self {
        date.0
    }
}

impl From<DateTime> for NaiveDateTime {
    fn from(datetime: DateTime) -> Self {
        datetime.0
    }
}

impl From<DateTime64> for NaiveDateTime {
    fn from(datetime: DateTime64) -> Self {
        datetime.0
    }
}

// Implement TryFrom for Value conversions
impl TryFrom<Value> for Date {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Date(date) => Ok(Date(date)),
            Value::DateTime(datetime) => Ok(Date(datetime.date())),
            Value::DateTime64(datetime) => Ok(Date(datetime.date())),
            Value::String(s) => {
                NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                    .map(Date)
                    .map_err(|e| format!("Invalid date format: {}", e))
            }
            Value::UInt32(timestamp) => {
                // Assume Unix timestamp (days since epoch)
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .checked_add_days(chrono::Days::new(timestamp as u64))
                    .ok_or_else(|| "Invalid date".to_string())?;
                Ok(Date(date))
            }
            Value::UInt64(timestamp) => {
                // Assume Unix timestamp (days since epoch)
                let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .checked_add_days(chrono::Days::new(timestamp))
                    .ok_or_else(|| "Invalid date".to_string())?;
                Ok(Date(date))
            }
            _ => Err(format!("Cannot convert {} to Date", value.type_name())),
        }
    }
}

impl TryFrom<Value> for DateTime {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime(datetime) => Ok(DateTime(datetime)),
            Value::DateTime64(datetime) => Ok(DateTime(datetime)),
            Value::Date(date) => Ok(DateTime(date.and_hms_opt(0, 0, 0).unwrap())),
            Value::String(s) => {
                // Try multiple formats
                let formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S%.f",
                    "%Y-%m-%dT%H:%M:%S%.f",
                ];

                for fmt in &formats {
                    if let Ok(datetime) = NaiveDateTime::parse_from_str(&s, fmt) {
                        return Ok(DateTime(datetime));
                    }
                }

                Err(format!("Invalid datetime format: {}", s))
            }
            Value::UInt64(timestamp) => {
                // Assume Unix timestamp (seconds since epoch)
                let datetime = chrono::DateTime::from_timestamp(timestamp as i64, 0)
                    .ok_or_else(|| "Invalid timestamp".to_string())?
                    .naive_utc();
                Ok(DateTime(datetime))
            }
            _ => Err(format!("Cannot convert {} to DateTime", value.type_name())),
        }
    }
}

impl TryFrom<Value> for DateTime64 {
    type Error = String;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime64(datetime) => Ok(DateTime64(datetime)),
            Value::DateTime(datetime) => Ok(DateTime64(datetime)),
            Value::Date(date) => Ok(DateTime64(date.and_hms_opt(0, 0, 0).unwrap())),
            Value::String(s) => {
                // Try multiple formats
                let formats = [
                    "%Y-%m-%d %H:%M:%S%.9f",
                    "%Y-%m-%dT%H:%M:%S%.9f",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                ];

                for fmt in &formats {
                    if let Ok(datetime) = NaiveDateTime::parse_from_str(&s, fmt) {
                        return Ok(DateTime64(datetime));
                    }
                }

                Err(format!("Invalid datetime64 format: {}", s))
            }
            Value::UInt64(timestamp) => {
                // Assume Unix timestamp (seconds since epoch)
                let datetime = chrono::DateTime::from_timestamp(timestamp as i64, 0)
                    .ok_or_else(|| "Invalid timestamp".to_string())?
                    .naive_utc();
                Ok(DateTime64(datetime))
            }
            _ => Err(format!("Cannot convert {} to DateTime64", value.type_name())),
        }
    }
}

// Implement Default traits
impl Default for Date {
    fn default() -> Self {
        Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
    }
}

impl Default for DateTime {
    fn default() -> Self {
        DateTime(chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc())
    }
}

impl Default for DateTime64 {
    fn default() -> Self {
        DateTime64(chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc())
    }
}

// Implement arithmetic operations
impl std::ops::Add<chrono::Duration> for Date {
    type Output = Self;

    fn add(self, rhs: chrono::Duration) -> Self::Output {
        Date(self.0 + rhs)
    }
}

impl std::ops::Sub<chrono::Duration> for Date {
    type Output = Self;

    fn sub(self, rhs: chrono::Duration) -> Self::Output {
        Date(self.0 - rhs)
    }
}

impl std::ops::Add<chrono::Duration> for DateTime {
    type Output = Self;

    fn add(self, rhs: chrono::Duration) -> Self::Output {
        DateTime(self.0 + rhs)
    }
}

impl std::ops::Sub<chrono::Duration> for DateTime {
    type Output = Self;

    fn sub(self, rhs: chrono::Duration) -> Self::Output {
        DateTime(self.0 - rhs)
    }
}

impl std::ops::Add<chrono::Duration> for DateTime64 {
    type Output = Self;

    fn add(self, rhs: chrono::Duration) -> Self::Output {
        DateTime64(self.0 + rhs)
    }
}

impl std::ops::Sub<chrono::Duration> for DateTime64 {
    type Output = Self;

    fn sub(self, rhs: chrono::Duration) -> Self::Output {
        DateTime64(self.0 - rhs)
    }
}

// Implement comparison traits
impl PartialEq<NaiveDate> for Date {
    fn eq(&self, other: &NaiveDate) -> bool {
        self.0 == *other
    }
}

impl PartialEq<NaiveDateTime> for DateTime {
    fn eq(&self, other: &NaiveDateTime) -> bool {
        self.0 == *other
    }
}

impl PartialEq<NaiveDateTime> for DateTime64 {
    fn eq(&self, other: &NaiveDateTime) -> bool {
        self.0 == *other
    }
}




