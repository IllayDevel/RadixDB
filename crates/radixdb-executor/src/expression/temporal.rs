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

//! Temporal arithmetic shared by expression VM instructions.

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeDelta, Timelike, Utc};
use radixdb_core::{DataType, Error, Result, Value};

/// Parsed interval value: either a fixed-length duration or a
/// calendar-relative month count. Months and years require calendar-aware
/// arithmetic because their duration is not constant.
enum IntervalValue {
    Duration(Duration),
    Months(i64),
}

pub(super) fn timestamp_add_days(timestamp: DateTime<Utc>, days: i64) -> Result<Value> {
    let duration = Duration::try_days(days)
        .ok_or_else(|| Error::Type("timestamp day interval overflow".to_string()))?;
    timestamp
        .checked_add_signed(duration)
        .map(Value::Timestamp)
        .ok_or_else(|| Error::Type("timestamp result is out of range".to_string()))
}

pub(super) fn civil_timestamp_add_days(timestamp: &Value, days: i64) -> Result<Value> {
    let timestamp = timestamp
        .as_civil_timestamp()
        .ok_or_else(|| Error::Type("invalid civil timestamp value".to_string()))?;
    let duration = Duration::try_days(days)
        .ok_or_else(|| Error::Type("timestamp day interval overflow".to_string()))?;
    let result = timestamp
        .checked_add_signed(duration)
        .ok_or_else(|| Error::Type("timestamp result is out of range".to_string()))?;
    Value::civil_timestamp(result).map_err(|error| Error::Type(error.to_string()))
}

pub(super) fn timestamp_add_interval(ts: &Value, interval: &Value, add: bool) -> Result<Value> {
    let (timestamp, civil) = match ts {
        Value::Timestamp(timestamp) => (*timestamp, false),
        _ if ts.as_civil_timestamp().is_some() => (
            ts.as_civil_timestamp()
                .expect("civil timestamp was checked")
                .and_utc(),
            true,
        ),
        Value::Null(data_type) => return Ok(Value::Null(*data_type)),
        _ => return Ok(Value::Null(DataType::Timestamp)),
    };

    let interval = match interval {
        Value::Text(interval) => interval.as_ref(),
        Value::Null(_) => return Ok(Value::Null(DataType::Timestamp)),
        _ => return Ok(Value::Null(DataType::Timestamp)),
    };

    match parse_interval(interval)? {
        IntervalValue::Duration(duration) => {
            let duration = if add {
                duration
            } else {
                duration
                    .checked_mul(-1)
                    .ok_or_else(|| Error::Type("fixed interval overflow".to_string()))?
            };
            let result = timestamp
                .checked_add_signed(duration)
                .ok_or_else(|| Error::Type("timestamp result is out of range".to_string()))?;
            temporal_from_datetime(result, civil)
        }
        IntervalValue::Months(months) => {
            let months = if add {
                months
            } else {
                months
                    .checked_neg()
                    .ok_or_else(|| Error::Type("calendar interval overflow".to_string()))?
            };
            let result = calendar_add_months(timestamp, months)
                .ok_or_else(|| Error::Type("timestamp result is out of range".to_string()))?;
            temporal_from_datetime(result, civil)
        }
    }
}

pub(super) fn format_duration_as_interval(duration: TimeDelta) -> String {
    let total_seconds = duration.num_seconds();
    let abs_seconds = total_seconds.abs();

    let days = abs_seconds / 86_400;
    let hours = (abs_seconds % 86_400) / 3_600;
    let minutes = (abs_seconds % 3_600) / 60;
    let seconds = abs_seconds % 60;
    let sign = if total_seconds < 0 { "-" } else { "" };

    if days > 0 {
        format!("{sign}{days} days {hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

fn temporal_from_datetime(timestamp: DateTime<Utc>, civil: bool) -> Result<Value> {
    if civil {
        Value::civil_timestamp(timestamp.naive_utc())
            .map_err(|error| Error::Type(error.to_string()))
    } else {
        Ok(Value::Timestamp(timestamp))
    }
}

/// Calendar-aware month addition preserving time-of-day and nanoseconds.
fn calendar_add_months(timestamp: DateTime<Utc>, months: i64) -> Option<DateTime<Utc>> {
    let total_months = i64::from(timestamp.year())
        .checked_mul(12)?
        .checked_add(i64::from(timestamp.month()) - 1)?
        .checked_add(months)?;
    let new_year_i64 = total_months.div_euclid(12);
    let new_month = (total_months.rem_euclid(12) + 1) as u32;

    let new_year = i32::try_from(new_year_i64).ok()?;
    if !(1..=9999).contains(&new_year) {
        return None;
    }

    let max_day = match new_month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (new_year % 4 == 0 && new_year % 100 != 0) || new_year % 400 == 0 => 29,
        2 => 28,
        _ => return None,
    };
    let date = NaiveDate::from_ymd_opt(new_year, new_month, timestamp.day().min(max_day))?;
    let time = timestamp.time();
    let naive = date.and_hms_nano_opt(
        time.hour(),
        time.minute(),
        time.second(),
        timestamp.timestamp_subsec_nanos(),
    )?;
    Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn parse_interval(interval: &str) -> Result<IntervalValue> {
    let interval = interval.trim();
    let parts: Vec<&str> = interval.split_whitespace().collect();

    if parts.len() < 2 {
        if let Ok(days) = interval.parse::<i64>() {
            return Duration::try_days(days)
                .map(IntervalValue::Duration)
                .ok_or_else(|| Error::Type("interval is out of range".to_string()));
        }
        return Err(Error::Type(format!("invalid interval: {interval}")));
    }

    let value: i64 = parts[0]
        .parse()
        .map_err(|_| Error::Type(format!("invalid interval: {interval}")))?;
    let unit = parts[1];
    let fixed = |duration: Option<Duration>| {
        duration
            .map(IntervalValue::Duration)
            .ok_or_else(|| Error::Type("interval is out of range".to_string()))
    };

    if unit.eq_ignore_ascii_case("year") || unit.eq_ignore_ascii_case("years") {
        value
            .checked_mul(12)
            .map(IntervalValue::Months)
            .ok_or_else(|| Error::Type("calendar interval overflow".to_string()))
    } else if unit.eq_ignore_ascii_case("month") || unit.eq_ignore_ascii_case("months") {
        Ok(IntervalValue::Months(value))
    } else if unit.eq_ignore_ascii_case("week") || unit.eq_ignore_ascii_case("weeks") {
        fixed(Duration::try_weeks(value))
    } else if unit.eq_ignore_ascii_case("day") || unit.eq_ignore_ascii_case("days") {
        fixed(Duration::try_days(value))
    } else if unit.eq_ignore_ascii_case("hour") || unit.eq_ignore_ascii_case("hours") {
        fixed(Duration::try_hours(value))
    } else if unit.eq_ignore_ascii_case("minute")
        || unit.eq_ignore_ascii_case("minutes")
        || unit.eq_ignore_ascii_case("min")
    {
        fixed(Duration::try_minutes(value))
    } else if unit.eq_ignore_ascii_case("second")
        || unit.eq_ignore_ascii_case("seconds")
        || unit.eq_ignore_ascii_case("sec")
    {
        fixed(Duration::try_seconds(value))
    } else if unit.eq_ignore_ascii_case("millisecond")
        || unit.eq_ignore_ascii_case("milliseconds")
        || unit.eq_ignore_ascii_case("ms")
    {
        Ok(IntervalValue::Duration(Duration::milliseconds(value)))
    } else if unit.eq_ignore_ascii_case("microsecond")
        || unit.eq_ignore_ascii_case("microseconds")
        || unit.eq_ignore_ascii_case("us")
    {
        Ok(IntervalValue::Duration(Duration::microseconds(value)))
    } else {
        Err(Error::Type(format!("invalid interval unit: {unit}")))
    }
}
