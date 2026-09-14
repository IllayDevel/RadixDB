// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Connection-local time-zone parsing and civil/instant conversion.
//!
//! A session time zone is execution policy only. It never becomes part of a
//! persisted `TIMESTAMP` or `TIMESTAMPTZ` payload.

use std::{fmt, str::FromStr};

use chrono::{DateTime, FixedOffset, LocalResult, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;

use crate::{value::parse_civil_timestamp, Error, Result};

const EXPLICIT_TIMESTAMP_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f%:z",
    "%Y-%m-%dT%H:%M:%S%:z",
    "%Y-%m-%dT%H:%M:%S%.fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S%.f%:z",
    "%Y-%m-%d %H:%M:%S%:z",
    "%Y-%m-%d %H:%M:%S%.fZ",
    "%Y-%m-%d %H:%M:%SZ",
];

/// A validated session time zone.
///
/// Named zones use the bundled IANA rules. Fixed offsets intentionally remain
/// available for legacy data whose source contract is an offset rather than a
/// geographic zone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SessionTimeZone {
    /// Coordinated Universal Time.
    #[default]
    Utc,
    /// An offset that does not vary by date.
    Fixed(FixedOffset),
    /// A named IANA time zone with date-dependent rules.
    Named(Tz),
}

impl SessionTimeZone {
    /// Parse `UTC`, a fixed `+HH:MM`/`-HH:MM` offset, or an IANA zone name.
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("UTC") || value.eq_ignore_ascii_case("Z") {
            return Ok(Self::Utc);
        }

        if value.starts_with('+') || value.starts_with('-') {
            let offset = parse_fixed_offset(value)?;
            return Ok(if offset.local_minus_utc() == 0 {
                Self::Utc
            } else {
                Self::Fixed(offset)
            });
        }

        Tz::from_str(value).map(Self::Named).map_err(|_| {
            Error::invalid_argument(format!(
                "invalid time zone '{value}'; expected UTC, a fixed offset such as +07:00, or an IANA name such as Asia/Barnaul"
            ))
        })
    }

    /// Convert a civil date-time to one UTC instant.
    ///
    /// DST gaps and overlaps fail closed because either would require the
    /// engine to invent policy not present in the value.
    pub fn civil_to_instant(self, civil: NaiveDateTime) -> Result<DateTime<Utc>> {
        let local = match self {
            Self::Utc => Utc
                .from_local_datetime(&civil)
                .map(|value| value.with_timezone(&Utc)),
            Self::Fixed(offset) => offset
                .from_local_datetime(&civil)
                .map(|value| value.with_timezone(&Utc)),
            Self::Named(zone) => zone
                .from_local_datetime(&civil)
                .map(|value| value.with_timezone(&Utc)),
        };

        match local {
            LocalResult::Single(value) => Ok(value),
            LocalResult::None => Err(Error::invalid_argument(format!(
                "civil timestamp {civil} does not exist in time zone {self}"
            ))),
            LocalResult::Ambiguous(_, _) => Err(Error::invalid_argument(format!(
                "civil timestamp {civil} is ambiguous in time zone {self}; use an explicit offset"
            ))),
        }
    }

    /// Convert a UTC instant to its unambiguous civil representation.
    pub fn instant_to_civil(self, instant: DateTime<Utc>) -> NaiveDateTime {
        match self {
            Self::Utc => instant.naive_utc(),
            Self::Fixed(offset) => instant.with_timezone(&offset).naive_local(),
            Self::Named(zone) => instant.with_timezone(&zone).naive_local(),
        }
    }

    /// Parse a TIMESTAMPTZ input. An explicit input offset wins; otherwise the
    /// civil text is interpreted using this session zone.
    pub fn parse_instant(self, value: &str) -> Result<DateTime<Utc>> {
        if let Ok(instant) = parse_timestamp_with_explicit_offset(value) {
            return Ok(instant);
        }
        self.civil_to_instant(parse_civil_timestamp(value)?)
    }

    /// Render an instant with the offset selected by this zone at that date.
    pub fn format_instant(self, instant: DateTime<Utc>) -> String {
        match self {
            Self::Utc => instant.to_rfc3339_opts(SecondsFormat::AutoSi, false),
            Self::Fixed(offset) => instant
                .with_timezone(&offset)
                .to_rfc3339_opts(SecondsFormat::AutoSi, false),
            Self::Named(zone) => instant
                .with_timezone(&zone)
                .to_rfc3339_opts(SecondsFormat::AutoSi, false),
        }
    }
}

impl fmt::Display for SessionTimeZone {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Utc => formatter.write_str("UTC"),
            Self::Fixed(offset) => {
                let seconds = offset.local_minus_utc();
                let sign = if seconds < 0 { '-' } else { '+' };
                let absolute = seconds.unsigned_abs();
                write!(
                    formatter,
                    "{sign}{:02}:{:02}",
                    absolute / 3_600,
                    (absolute % 3_600) / 60
                )
            }
            Self::Named(zone) => zone.fmt(formatter),
        }
    }
}

fn parse_fixed_offset(value: &str) -> Result<FixedOffset> {
    let bytes = value.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' {
        return Err(Error::invalid_argument(format!(
            "invalid fixed time-zone offset '{value}'; expected +HH:MM or -HH:MM"
        )));
    }
    let hours = parse_two_digits(&bytes[1..3], value)?;
    let minutes = parse_two_digits(&bytes[4..6], value)?;
    if hours > 23 || minutes > 59 {
        return Err(Error::invalid_argument(format!(
            "invalid fixed time-zone offset '{value}'"
        )));
    }
    let magnitude = hours * 3_600 + minutes * 60;
    let seconds = if bytes[0] == b'-' {
        -magnitude
    } else if bytes[0] == b'+' {
        magnitude
    } else {
        return Err(Error::invalid_argument(format!(
            "invalid fixed time-zone offset '{value}'"
        )));
    };
    if seconds == 0 {
        return Ok(FixedOffset::east_opt(0).expect("zero offset is valid"));
    }
    FixedOffset::east_opt(seconds)
        .ok_or_else(|| Error::invalid_argument(format!("invalid fixed time-zone offset '{value}'")))
}

/// Parse only timestamp forms that carry an explicit UTC offset or `Z`.
pub fn parse_timestamp_with_explicit_offset(value: &str) -> Result<DateTime<Utc>> {
    let value = value.trim();
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    for format in EXPLICIT_TIMESTAMP_FORMATS {
        if let Ok(timestamp) = DateTime::parse_from_str(value, format) {
            return Ok(timestamp.with_timezone(&Utc));
        }
    }
    Err(Error::parse(format!(
        "timestamp has no valid explicit time-zone offset: {value}"
    )))
}

fn parse_two_digits(bytes: &[u8], original: &str) -> Result<i32> {
    if bytes.len() != 2 || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(Error::invalid_argument(format!(
            "invalid fixed time-zone offset '{original}'"
        )));
    }
    Ok(i32::from(bytes[0] - b'0') * 10 + i32::from(bytes[1] - b'0'))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn civil(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
    }

    #[test]
    fn parses_and_canonicalizes_supported_zone_forms() {
        assert_eq!(SessionTimeZone::parse("UTC").unwrap().to_string(), "UTC");
        assert_eq!(SessionTimeZone::parse("z").unwrap().to_string(), "UTC");
        assert_eq!(SessionTimeZone::parse("+00:00").unwrap().to_string(), "UTC");
        assert_eq!(
            SessionTimeZone::parse("+07:00").unwrap().to_string(),
            "+07:00"
        );
        assert_eq!(
            SessionTimeZone::parse("Asia/Barnaul").unwrap().to_string(),
            "Asia/Barnaul"
        );
    }

    #[test]
    fn rejects_invalid_fixed_offsets() {
        for value in ["+7:00", "+24:00", "+02:60", "+01", "+aa:00"] {
            assert!(SessionTimeZone::parse(value).is_err(), "{value}");
        }
    }

    #[test]
    fn fixed_offset_round_trip_is_exact() {
        let zone = SessionTimeZone::parse("+07:00").unwrap();
        let civil = civil(2026, 9, 11, 12, 30);
        let instant = zone.civil_to_instant(civil).unwrap();
        assert_eq!(instant.to_rfc3339(), "2026-09-11T05:30:00+00:00");
        assert_eq!(zone.instant_to_civil(instant), civil);
        assert_eq!(zone.format_instant(instant), "2026-09-11T12:30:00+07:00");
    }

    #[test]
    fn explicit_input_offset_wins_over_session_zone() {
        let zone = SessionTimeZone::parse("America/New_York").unwrap();
        let explicit = zone.parse_instant("2026-09-11T12:30:00+07:00").unwrap();
        assert_eq!(explicit.to_rfc3339(), "2026-09-11T05:30:00+00:00");

        let contextual = zone.parse_instant("2026-09-11 12:30:00").unwrap();
        assert_eq!(contextual.to_rfc3339(), "2026-09-11T16:30:00+00:00");
    }

    #[test]
    fn named_zone_applies_historical_rules() {
        let zone = SessionTimeZone::parse("America/New_York").unwrap();
        let winter = zone.civil_to_instant(civil(2026, 1, 15, 12, 0)).unwrap();
        let summer = zone.civil_to_instant(civil(2026, 7, 15, 12, 0)).unwrap();
        assert_eq!(winter.to_rfc3339(), "2026-01-15T17:00:00+00:00");
        assert_eq!(summer.to_rfc3339(), "2026-07-15T16:00:00+00:00");
    }

    #[test]
    fn named_zone_rejects_dst_gap_and_overlap() {
        let zone = SessionTimeZone::parse("America/New_York").unwrap();
        assert!(zone.civil_to_instant(civil(2026, 3, 8, 2, 30)).is_err());
        assert!(zone.civil_to_instant(civil(2026, 11, 1, 1, 30)).is_err());
    }
}
