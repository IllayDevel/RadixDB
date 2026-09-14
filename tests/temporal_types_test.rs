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

use chrono::{NaiveDate, NaiveTime};
use radixdb::{Database, Result};

#[test]
fn temporal_types_keep_distinct_semantics_over_shared_i64_storage() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("temporal-types");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        path.display()
    );
    let civil = NaiveDate::from_ymd_opt(2026, 9, 11)
        .unwrap()
        .and_hms_nano_opt(10, 20, 30, 123_456_789)
        .unwrap();
    let clock = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap();

    {
        let db = Database::open(&dsn)?;
        db.execute(
            "CREATE TABLE temporal_values (\
                id INTEGER PRIMARY KEY, \
                instant TIMESTAMPTZ NOT NULL, \
                instant_long TIMESTAMP WITH TIME ZONE NOT NULL, \
                civil TIMESTAMP NOT NULL, \
                civil_long TIMESTAMP WITHOUT TIME ZONE NOT NULL, \
                clock TIME NOT NULL, \
                clock_long TIME WITHOUT TIME ZONE NOT NULL\
            )",
            (),
        )?;
        db.execute(
            "CREATE INDEX temporal_instant_idx ON temporal_values(instant)",
            (),
        )?;
        db.execute(
            "CREATE INDEX temporal_civil_idx ON temporal_values(civil)",
            (),
        )?;
        db.execute(
            "CREATE INDEX temporal_clock_idx ON temporal_values(clock)",
            (),
        )?;
        db.execute(
            "INSERT INTO temporal_values VALUES (\
                1, \
                TIMESTAMPTZ '2026-09-11T10:20:30.123456789+07:00', \
                TIMESTAMPTZ '2026-09-11T03:20:30.123456789Z', \
                TIMESTAMP '2026-09-11 10:20:30.123456789', \
                TIMESTAMP '2026-09-11T10:20:30.123456789', \
                TIME '23:59:59.999999999', \
                TIME '23:59:59.999999999'\
            )",
            (),
        )?;

        assert_eq!(
            db.query_one::<String, _>("SELECT CAST(instant AS TEXT) FROM temporal_values", ())?,
            "2026-09-11T03:20:30.123456789+00:00"
        );
        assert_eq!(
            db.query_one::<chrono::NaiveDateTime, _>(
                "SELECT civil FROM temporal_values WHERE civil = ?",
                (civil,),
            )?,
            civil
        );
        assert_eq!(
            db.query_one::<chrono::NaiveTime, _>(
                "SELECT clock FROM temporal_values WHERE clock = ?",
                (clock,),
            )?,
            clock
        );
        assert_eq!(
            db.query_one::<String, _>("SELECT TYPEOF(instant) FROM temporal_values", ())?,
            "TIMESTAMPTZ"
        );
        assert_eq!(
            db.query_one::<String, _>("SELECT TYPEOF(civil) FROM temporal_values", ())?,
            "TIMESTAMP"
        );
        assert_eq!(
            db.query_one::<String, _>("SELECT TYPEOF(clock) FROM temporal_values", ())?,
            "TIME"
        );
        assert_eq!(
            db.query_one::<chrono::DateTime<chrono::Utc>, _>(
                "SELECT CIVIL_TO_TIMESTAMPTZ(civil, '+07:00') FROM temporal_values",
                (),
            )?,
            chrono::DateTime::parse_from_rfc3339("2026-09-11T03:20:30.123456789Z")
                .unwrap()
                .with_timezone(&chrono::Utc)
        );
        assert_eq!(
            db.query_one::<chrono::NaiveDateTime, _>(
                "SELECT TIMESTAMPTZ_TO_CIVIL(instant, 'Asia/Barnaul') FROM temporal_values",
                (),
            )?,
            civil
        );
        assert!(db
            .query_one::<chrono::DateTime<chrono::Utc>, _>(
                "SELECT CIVIL_TO_TIMESTAMPTZ(\
                    TIMESTAMP '2026-11-01 01:30:00', 'America/New_York'\
                )",
                (),
            )
            .is_err());
        assert!(db
            .execute(
                "INSERT INTO temporal_values VALUES (\
                    2, TIMESTAMPTZ '2026-09-11T03:20:30Z', \
                    TIMESTAMPTZ '2026-09-11T03:20:30Z', \
                    TIMESTAMP '2026-09-11T10:20:30+07:00', \
                    TIMESTAMP '2026-09-11 10:20:30', \
                    TIME '12:00:00', TIME '12:00:00'\
                )",
                (),
            )
            .is_err());

        let create_sql: String = db
            .query("SHOW CREATE TABLE temporal_values", ())?
            .next()
            .expect("SHOW CREATE TABLE row")?
            .get(1)?;
        assert!(
            create_sql.contains("\"instant\" TIMESTAMPTZ"),
            "{create_sql}"
        );
        assert!(create_sql.contains("\"civil\" TIMESTAMP"), "{create_sql}");
        assert!(create_sql.contains("\"clock\" TIME"), "{create_sql}");
        db.execute("PRAGMA CHECKPOINT", ())?;
    }

    {
        let db = Database::open(&dsn)?;
        assert_eq!(
            db.query_one::<chrono::NaiveDateTime, _>(
                "SELECT civil FROM temporal_values WHERE civil = ?",
                (civil,),
            )?,
            civil
        );
        assert_eq!(
            db.query_one::<chrono::NaiveTime, _>(
                "SELECT clock FROM temporal_values WHERE clock = ?",
                (clock,),
            )?,
            clock
        );
    }

    Ok(())
}
