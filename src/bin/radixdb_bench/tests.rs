#[cfg(test)]
mod tests {
    use super::*;

    fn parse_config(args: &[&str]) -> BenchResult<BenchConfig> {
        BenchConfig::parse(args.iter().map(|value| (*value).to_string()))
    }

    #[test]
    fn default_cli_keeps_full_benchmark_mode() {
        let config = parse_config(&[]).unwrap();

        assert_eq!(config.only_case, None);
        assert_eq!(config.warmup_runs, DEFAULT_ISOLATED_WARMUP_RUNS);
        assert_eq!(config.repeat_runs, DEFAULT_ISOLATED_MEASURED_RUNS);
        assert_eq!(config.pg_profile, PgBenchProfile::Default);
    }

    #[test]
    fn embedded_profile_cli_has_an_exact_twenty_thousand_row_scale() {
        for spelling in ["20k", "20000", "20_000"] {
            let config = parse_config(&["--scale", spelling]).unwrap();
            assert_eq!(config.scale, Scale::TwentyThousand);
            assert_eq!(config.scale.as_str(), "20k");
            assert_eq!(config.scale.total_rows(), 20_000);
            assert!(!config.scale.requires_large_opt_in());
        }
    }

    #[test]
    fn benchmark_server_uses_an_isolated_large_copy_budget() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut config = parse_config(&["--participant", "server"]).unwrap();
        config.root = temp.path().to_path_buf();
        let layout = BenchLayout::new(&config).unwrap();
        layout.ensure(&config.participants).unwrap();

        let runtime = server_config(&config, &layout);
        assert_eq!(
            runtime.copy_max_transaction_bytes,
            BENCH_COPY_TRANSACTION_BYTES
        );
        assert!(
            runtime.copy_max_transaction_bytes
                > radixdb::server::default_copy_max_transaction_bytes()
        );

        write_server_config(&config, &layout, config.server_port).unwrap();
        let written = fs::read_to_string(layout.rd_root.join("server.toml")).unwrap();
        assert!(written.contains(&format!(
            "copy_max_transaction_bytes = {BENCH_COPY_TRANSACTION_BYTES}"
        )));
        assert!(written.contains("storage_cpu_workers = 0"));
        assert!(written.contains("page_cache_level = 0"));
        assert!(written.contains("page_cache_max_bytes = 0"));
        assert!(written.contains("page_cache_memory_reserve = 0"));
        assert!(written.contains("read_queue_depth = 1"));
    }

    #[test]
    fn benchmark_cli_uses_only_version_neutral_storage_flags() {
        let config = parse_config(&[
            "--storage-cpu-workers",
            "1",
            "--page-cache-level",
            "5",
            "--page-cache-max-bytes",
            "1048576",
            "--page-cache-memory-reserve",
            "2097152",
            "--page-cache-wait-ms",
            "600000",
            "--read-queue-depth",
            "4",
        ])
        .unwrap();
        assert_eq!(config.storage_cpu_workers, 1);
        assert_eq!(config.page_cache_level, 5);
        assert_eq!(config.page_cache_max_bytes, 1_048_576);
        assert_eq!(config.page_cache_memory_reserve, 2_097_152);
        assert_eq!(config.page_cache_wait_millis, 600_000);
        assert_eq!(config.read_queue_depth, 4);
    }

    #[test]
    fn benchmark_cli_rejects_page_cache_level_above_ten() {
        let error = parse_config(&["--page-cache-level", "11"])
            .expect_err("out-of-range warmup level must fail closed")
            .to_string();
        assert!(error.contains("expected 0..=10"));
    }

    #[test]
    fn benchmark_cold_cache_gate_requires_existing_data_mode() {
        let error = parse_config(&["--evict-database-page-cache"])
            .expect_err("targeted eviction must not be accepted for fresh import")
            .to_string();
        assert!(error.contains("requires --verify-existing"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn benchmark_page_cache_eviction_is_advisory_and_preserves_files() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("table");
        fs::create_dir(&nested).unwrap();
        let first = dir.path().join("CONTROL.0");
        let second = nested.join("artifact.data");
        fs::write(&first, b"generation").unwrap();
        fs::write(&second, b"immutable-volume").unwrap();

        let (files, bytes) = evict_database_page_cache(dir.path()).unwrap();
        assert_eq!(files, 2);
        assert_eq!(bytes, 10 + 16);
        assert_eq!(fs::read(first).unwrap(), b"generation");
        assert_eq!(fs::read(second).unwrap(), b"immutable-volume");
    }

    #[test]
    fn reference_navigation_is_a_repeatable_postgres_server_case() {
        let config = parse_config(&[
            "--only-case",
            "reference.navigation",
            "--participants",
            "postgres,server",
        ])
        .unwrap();

        assert_eq!(config.only_case, Some(QueryCase::ReferenceNavigation));
        assert_eq!(
            config.participants,
            vec![Participant::Postgres, Participant::Server]
        );
    }

    #[test]
    fn explicit_and_transitive_reference_cases_are_repeatable_server_cases() {
        for (name, expected) in [
            (
                "reference.direct.explicit",
                QueryCase::ReferenceDirectExplicit,
            ),
            (
                "reference.fact_dictionary",
                QueryCase::ReferenceFactDictionary,
            ),
            ("reference.fact_first", QueryCase::ReferenceFactFirst),
            ("reference.target_first", QueryCase::ReferenceTargetFirst),
        ] {
            let config = parse_config(&["--only-case", name, "--participant", "server"]).unwrap();
            assert_eq!(config.only_case, Some(expected));
            assert!(parse_config(&["--only-case", name, "--participant", "all"]).is_err());
        }
    }

    #[test]
    fn reference_fact_group_cardinality_matches_benchmark_seed_domains() {
        assert_eq!(expected_reference_fact_group_count(12_000), 80);
        assert_eq!(expected_reference_fact_group_count(10_000_000), 8_000);
        assert_eq!(expected_reference_fact_group_count(100_000_000), 8_000);
    }

    #[test]
    fn benchmark_reference_chain_is_declared_only_on_the_fact_dictionary_tables() {
        let department = create_table_sql(58, false);
        let employee = create_table_sql(59, false);
        let payment = create_table_sql(60, false);

        assert!(!department.contains("REFERENCES"));
        assert!(employee.contains("parent_id INTEGER NOT NULL REFERENCES bench_object_058(id)"));
        assert!(payment.contains("parent_id INTEGER NOT NULL REFERENCES bench_object_059(id)"));
        assert!(reference_navigation_sql().contains("c.parent_id.payload"));
        assert!(reference_fact_dictionary_sql().contains("c.parent_id.parent_id.payload"));
    }

    #[test]
    fn sqlite_csv_boolean_normalization_preserves_reference_predicates() {
        let connection = SqliteConnection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE imported(active BOOLEAN NOT NULL); \
                 INSERT INTO imported VALUES ('true'), ('false'), (1), (0);",
            )
            .unwrap();

        normalize_sqlite_imported_booleans(&connection, "imported").unwrap();

        assert_eq!(
            sqlite_query_i64(
                &connection,
                "SELECT COUNT(*) FROM imported WHERE active = TRUE"
            )
            .unwrap(),
            2
        );
        assert_eq!(
            sqlite_query_i64(
                &connection,
                "SELECT COUNT(*) FROM imported WHERE active = FALSE"
            )
            .unwrap(),
            2
        );
    }

    #[test]
    fn complete_reference_slice_requires_cross_engine_parity() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = parse_config(&["--participant", "all"]).unwrap();
        config.root = temp.path().to_path_buf();
        let layout = BenchLayout::new(&config).unwrap();
        let mut report = BenchReport::new(&config, &layout, "differential-test");
        let cases = [
            "reference.navigation",
            "reference.direct.explicit",
            "reference.fact_dictionary",
            "reference.fact_first",
            "reference.target_first",
        ];
        for participant in Participant::all() {
            for case in cases {
                report.metrics.push(Metric::measured(
                    participant,
                    case,
                    Duration::from_millis(1),
                    80,
                    Some("80".to_string()),
                ));
            }
        }
        validate_reference_differential(&config, &report).unwrap();

        let sqlite_fact = report
            .metrics
            .iter_mut()
            .find(|metric| {
                metric.participant == Participant::Sqlite.as_str()
                    && metric.case == "reference.fact_dictionary"
            })
            .unwrap();
        sqlite_fact.rows = 0;
        sqlite_fact.checksum = Some("0".to_string());

        let error = validate_reference_differential(&config, &report).unwrap_err();
        assert!(error
            .to_string()
            .contains("reference differential mismatch for reference.fact_dictionary"));
    }

    #[test]
    fn run_id_is_one_portable_filename_component() {
        for valid in ["ci-current-1", "r10.release_20260820", "A0"] {
            validate_run_id(valid).unwrap();
            parse_config(&["--run-id", valid]).unwrap();
        }
        for invalid in [
            "",
            ".",
            "..",
            ".hidden",
            "../escaped",
            "nested/path",
            "windows\\path",
            "/absolute",
            "contains space",
            "line\nbreak",
        ] {
            assert!(
                validate_run_id(invalid).is_err(),
                "run id should be rejected: {invalid:?}"
            );
        }
    }

    #[test]
    fn server_only_layout_does_not_require_postgres_root() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut config = parse_config(&["--participant", "server"]).unwrap();
        config.root = temp.path().join("benchmark");
        let layout = BenchLayout::new(&config).unwrap();

        layout.ensure(&config.participants).unwrap();

        assert!(layout.server_data.is_dir());
        assert!(layout.results_dir.is_dir());
        assert!(!layout.pg_root.exists());
        assert!(!layout.sqlite_root.exists());
    }

    #[test]
    fn postgres_marker_is_bound_to_root_port_and_cluster_identifier() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut config =
            parse_config(&["--participant", "postgres", "--pg-port", "25432"]).unwrap();
        config.root = temp.path().to_path_buf();
        let layout = BenchLayout::new(&config).unwrap();
        fs::create_dir_all(layout.pg_root.join("data")).unwrap();
        let marker = PostgresOwnershipMarker {
            format: POSTGRES_OWNER_FORMAT.to_string(),
            data_dir: layout.pg_root.join("data").canonicalize().unwrap(),
            port: config.pg_port,
            system_identifier: "1234567890123456789".to_string(),
        };
        fs::write(
            layout.pg_root.join(POSTGRES_OWNER_MARKER),
            serde_json::to_vec_pretty(&marker).unwrap(),
        )
        .unwrap();

        let loaded = load_postgres_ownership_marker(&config, &layout).unwrap();
        assert_eq!(loaded.system_identifier, marker.system_identifier);

        config.pg_port += 1;
        let error = load_postgres_ownership_marker(&config, &layout).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match benchmark root/port"));
    }

    #[test]
    fn harness_started_postgres_is_stopped_after_readiness_failure() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("temp dir");
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut config =
            parse_config(&["--participant", "postgres", "--pg-port", &port.to_string()]).unwrap();
        config.root = temp.path().to_path_buf();
        let layout = BenchLayout::new(&config).unwrap();
        fs::create_dir_all(layout.pg_root.join("data")).unwrap();
        let marker = PostgresOwnershipMarker {
            format: POSTGRES_OWNER_FORMAT.to_string(),
            data_dir: layout.pg_root.join("data").canonicalize().unwrap(),
            port,
            system_identifier: "1234567890123456789".to_string(),
        };
        fs::write(
            layout.pg_root.join(POSTGRES_OWNER_MARKER),
            serde_json::to_vec_pretty(&marker).unwrap(),
        )
        .unwrap();
        for (name, body) in [
            ("start.sh", "#!/bin/sh\nexit 0\n"),
            (
                "stop.sh",
                "#!/bin/sh\ntouch \"$(dirname \"$0\")/stopped\"\n",
            ),
        ] {
            let path = layout.pg_root.join(name);
            fs::write(&path, body).unwrap();
            let mut permissions = fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).unwrap();
        }

        let error =
            ensure_postgres_with_timeout(&config, &layout, Duration::from_millis(20)).unwrap_err();

        assert!(error.to_string().contains("cleanup completed"));
        assert!(layout.pg_root.join("stopped").exists());
    }

    #[test]
    fn postgres_cleanup_error_is_combined_with_primary_failure() {
        let error = finish_harness_owned_postgres(
            Err::<(), DynError>("primary failure".into()),
            Err::<(), DynError>("cleanup failure".into()),
        )
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("primary failure"));
        assert!(message.contains("cleanup failure"));
    }

    #[test]
    fn postgres_profile_cli_parses_named_profile() {
        let config = parse_config(&["--pg-profile", "nvme"]).unwrap();

        assert_eq!(config.pg_profile, PgBenchProfile::NvmeLocal);
        assert!(config
            .pg_profile
            .settings()
            .iter()
            .any(|setting| setting.name == "shared_buffers"
                && setting.apply == "postgresql.conf/restart"));
    }

    #[test]
    fn isolated_cli_parses_join_parent_and_repeat_policy() {
        let config = parse_config(&[
            "--only-case",
            "join.parent",
            "--warmup",
            "2",
            "--repeats",
            "5",
        ])
        .unwrap();

        assert_eq!(config.only_case, Some(QueryCase::JoinParent));
        assert_eq!(config.warmup_runs, 2);
        assert_eq!(config.repeat_runs, 5);
        assert_eq!(
            config.participants,
            vec![Participant::Postgres, Participant::Server]
        );
    }

    #[test]
    fn isolated_cli_parses_delete_rollback_with_five_sample_gate() {
        let config = parse_config(&["--only-case", "delete.rollback"]).unwrap();

        assert_eq!(config.only_case, Some(QueryCase::DeleteRollback));
        assert_eq!(config.warmup_runs, 1);
        assert_eq!(config.repeat_runs, 5);
        assert_eq!(
            config.participants,
            vec![Participant::Postgres, Participant::Server]
        );
    }

    #[test]
    fn isolated_cli_parses_server_only_update_rollback_with_five_sample_gate() {
        let config =
            parse_config(&["--only-case", "update.rollback", "--participant", "server"]).unwrap();

        assert_eq!(config.only_case, Some(QueryCase::UpdateRollback));
        assert_eq!(config.warmup_runs, 1);
        assert_eq!(config.repeat_runs, 5);
        assert_eq!(config.participants, vec![Participant::Server]);
    }

    #[test]
    fn sqlite_participant_is_available_for_complete_slice() {
        let config = parse_config(&["--participant", "sqlite"]).unwrap();

        assert_eq!(config.participants, vec![Participant::Sqlite]);
    }

    #[test]
    fn sqlite_rejects_isolated_case() {
        let error =
            parse_config(&["--participant", "sqlite", "--only-case", "join.parent"]).unwrap_err();

        assert!(error.to_string().contains("complete query slice"));
    }

    #[test]
    fn postgres_password_is_optional_and_redacted_from_argv() {
        let config = parse_config(&["--pg-password", "secret-value"]).unwrap();

        assert_eq!(config.pg_password.as_deref(), Some("secret-value"));
        assert_eq!(
            sanitize_argv(&[
                "radixdb-bench".to_string(),
                "--pg-password".to_string(),
                "secret-value".to_string(),
                "--scale".to_string(),
                "10m".to_string(),
            ]),
            vec![
                "radixdb-bench".to_string(),
                "--pg-password".to_string(),
                "<redacted>".to_string(),
                "--scale".to_string(),
                "10m".to_string(),
            ]
        );
        assert_eq!(
            sanitize_argv(&["--pg-password=secret-value".to_string()]),
            vec!["--pg-password=<redacted>".to_string()]
        );
    }

    #[test]
    fn engine_high_water_keeps_full_counter_shape_and_max_values() {
        let first = EngineCountersSnapshot::default();
        let second = EngineCountersSnapshot {
            runtime_profile: RuntimeProfileSnapshot {
                protocol_round_trips: 13,
                ..RuntimeProfileSnapshot::default()
            },
            artifact_pread_calls: 7,
            artifact_pread_bytes: 4096,
            row_materialization_rows: 11,
            protocol_encode_bytes: 8192,
            ..EngineCountersSnapshot::default()
        };

        let high_water = engine_high_water_json(&[
            EngineSnapshotMetric {
                participant: Participant::Server.as_str().to_string(),
                phase: "server.after_reset".to_string(),
                counters: first,
                diagnostics: Vec::new(),
            },
            EngineSnapshotMetric {
                participant: Participant::Server.as_str().to_string(),
                phase: "case.scan.full.after".to_string(),
                counters: second,
                diagnostics: Vec::new(),
            },
        ]);
        let high_water = high_water.as_object().expect("high-water object");

        assert_eq!(high_water.get("artifact_pread_calls").unwrap().as_u64(), Some(7));
        assert_eq!(
            high_water.get("artifact_pread_bytes").unwrap().as_u64(),
            Some(4096)
        );
        assert_eq!(
            high_water.get("row_materialization_rows").unwrap().as_u64(),
            Some(11)
        );
        assert_eq!(
            high_water.get("protocol_encode_bytes").unwrap().as_u64(),
            Some(8192)
        );
        assert_eq!(
            high_water
                .get("runtime_profile.protocol_round_trips")
                .unwrap()
                .as_u64(),
            Some(13)
        );
        assert_eq!(
            high_water.get("artifact_file_open_calls").unwrap().as_u64(),
            Some(0),
            "zero counters must stay present for machine readers"
        );
    }

    #[test]
    fn engine_counter_delta_preserves_absolute_gauges_and_high_water_counters() {
        let before = EngineCountersSnapshot {
            runtime_profile: RuntimeProfileSnapshot {
                wait_calls: 4,
                hash_build_rows: 100,
                ..RuntimeProfileSnapshot::default()
            },
            artifact_pread_calls: 10,
            artifact_columnar_group_applies: 1,
            artifact_columnar_group_row_groups: 3,
            artifact_columnar_group_selected_blocks: 4,
            artifact_columnar_group_input_rows: 100,
            artifact_columnar_group_output_groups: 2,
            artifact_columnar_group_direct_accumulators: 1,
            artifact_columnar_group_hash_accumulators: 0,
            artifact_columnar_group_local_merges: 1,
            artifact_columnar_group_merged_groups: 2,
            artifact_columnar_group_scheduler_runs: 0,
            artifact_columnar_group_scheduled_segments: 0,
            artifact_columnar_group_fallbacks: 1,
            artifact_columnar_group_fallback_group_key: 1,
            protocol_column_batch_pending_opened: 4,
            protocol_column_batch_pending_current: 1,
            protocol_column_batch_pending_max: 3,
            protocol_column_batch_pending_rows_current: 64,
            protocol_column_batch_pending_rows_max: 256,
            protocol_column_batch_pending_bytes_current: 4096,
            protocol_column_batch_pending_bytes_max: 16_384,
            navigation_paths_executed: 2,
            navigation_lookup_batches: 3,
            navigation_repeated_keys_eliminated: 5,
            compaction_spool_write_calls: 3,
            compaction_spool_write_bytes: 4_096,
            compaction_spool_write_nanos: 50_000,
            compaction_spool_read_calls: 2,
            compaction_spool_read_bytes: 2_048,
            compaction_spool_read_nanos: 20_000,
            ..EngineCountersSnapshot::default()
        };
        let after = EngineCountersSnapshot {
            runtime_profile: RuntimeProfileSnapshot {
                wait_calls: 9,
                hash_build_rows: 350,
                ..RuntimeProfileSnapshot::default()
            },
            artifact_pread_calls: 15,
            artifact_columnar_group_applies: 3,
            artifact_columnar_group_row_groups: 9,
            artifact_columnar_group_selected_blocks: 11,
            artifact_columnar_group_input_rows: 350,
            artifact_columnar_group_output_groups: 6,
            artifact_columnar_group_direct_accumulators: 3,
            artifact_columnar_group_hash_accumulators: 1,
            artifact_columnar_group_local_merges: 4,
            artifact_columnar_group_merged_groups: 8,
            artifact_columnar_group_scheduler_runs: 1,
            artifact_columnar_group_scheduled_segments: 3,
            artifact_columnar_group_fallbacks: 3,
            artifact_columnar_group_fallback_group_key: 2,
            protocol_column_batch_pending_opened: 6,
            protocol_column_batch_pending_current: 2,
            protocol_column_batch_pending_max: 3,
            protocol_column_batch_pending_rows_current: 96,
            protocol_column_batch_pending_rows_max: 256,
            protocol_column_batch_pending_bytes_current: 8192,
            protocol_column_batch_pending_bytes_max: 16_384,
            navigation_paths_executed: 7,
            navigation_lookup_batches: 11,
            navigation_repeated_keys_eliminated: 19,
            compaction_spool_write_calls: 8,
            compaction_spool_write_bytes: 12_288,
            compaction_spool_write_nanos: 125_000,
            compaction_spool_read_calls: 6,
            compaction_spool_read_bytes: 8_192,
            compaction_spool_read_nanos: 75_000,
            ..EngineCountersSnapshot::default()
        };

        let delta = engine_counter_delta(&after, &before);

        assert_eq!(
            delta.artifact_pread_calls, 5,
            "ordinary counters must stay deltas"
        );
        assert_eq!(delta.runtime_profile.wait_calls, 5);
        assert_eq!(delta.runtime_profile.hash_build_rows, 250);
        assert_eq!(
            delta.protocol_column_batch_pending_opened, 2,
            "event counters must stay deltas"
        );
        assert_eq!(delta.artifact_columnar_group_applies, 2);
        assert_eq!(delta.artifact_columnar_group_row_groups, 6);
        assert_eq!(delta.artifact_columnar_group_selected_blocks, 7);
        assert_eq!(delta.artifact_columnar_group_input_rows, 250);
        assert_eq!(delta.artifact_columnar_group_output_groups, 4);
        assert_eq!(delta.artifact_columnar_group_direct_accumulators, 2);
        assert_eq!(delta.artifact_columnar_group_hash_accumulators, 1);
        assert_eq!(delta.artifact_columnar_group_local_merges, 3);
        assert_eq!(delta.artifact_columnar_group_merged_groups, 6);
        assert_eq!(delta.artifact_columnar_group_scheduler_runs, 1);
        assert_eq!(delta.artifact_columnar_group_scheduled_segments, 3);
        assert_eq!(delta.artifact_columnar_group_fallbacks, 2);
        assert_eq!(delta.artifact_columnar_group_fallback_group_key, 1);
        assert_eq!(
            delta.protocol_column_batch_pending_current, 2,
            "gauge counters must report the after snapshot"
        );
        assert_eq!(delta.protocol_column_batch_pending_max, 3);
        assert_eq!(delta.protocol_column_batch_pending_rows_current, 96);
        assert_eq!(delta.protocol_column_batch_pending_rows_max, 256);
        assert_eq!(delta.protocol_column_batch_pending_bytes_current, 8192);
        assert_eq!(delta.protocol_column_batch_pending_bytes_max, 16_384);
        assert_eq!(delta.navigation_paths_executed, 5);
        assert_eq!(delta.navigation_lookup_batches, 8);
        assert_eq!(delta.navigation_repeated_keys_eliminated, 14);
        assert_eq!(delta.compaction_spool_write_calls, 5);
        assert_eq!(delta.compaction_spool_write_bytes, 8_192);
        assert_eq!(delta.compaction_spool_write_nanos, 75_000);
        assert_eq!(delta.compaction_spool_read_calls, 4);
        assert_eq!(delta.compaction_spool_read_bytes, 6_144);
        assert_eq!(delta.compaction_spool_read_nanos, 55_000);
    }

    #[test]
    fn client_protocol_counter_delta_tracks_decode_work() {
        let before = ClientProtocolCountersSnapshot {
            frame_decode_calls: 10,
            frame_decode_bytes: 1024,
            frame_decode_nanos: 50_000,
        };
        let after = ClientProtocolCountersSnapshot {
            frame_decode_calls: 13,
            frame_decode_bytes: 4096,
            frame_decode_nanos: 175_000,
        };

        let delta = client_protocol_counter_delta(&after, &before);

        assert_eq!(delta.frame_decode_calls, 3);
        assert_eq!(delta.frame_decode_bytes, 3072);
        assert_eq!(delta.frame_decode_nanos, 125_000);
    }

    #[test]
    fn engine_counter_diagnostics_find_impossible_multiplicities() {
        let mut counters = EngineCountersSnapshot {
            artifact_pread_bytes: 128,
            artifact_payload_decompress_raw_bytes: 256,
            protocol_encode_bytes: 512,
            metadata_pk_count_attempts: 1,
            metadata_pk_count_applied: 2,
            join_pk_probe_keys: 3,
            join_pk_probe_hits: 4,
            ..EngineCountersSnapshot::default()
        };
        counters.metadata_pk_count_fallbacks = 2;
        counters.metadata_pk_count_fallback_snapshot = 1;
        counters.protocol_column_batch_fallbacks = 2;
        counters.protocol_column_batch_fallback_schema = 1;

        let codes = engine_counter_diagnostics(&counters)
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect::<Vec<_>>();

        for expected in [
            "artifact-pread-bytes-without-calls",
            "artifact-decompress-bytes-without-calls",
            "protocol-encode-bytes-without-calls",
            "metadata-pk-applied-over-attempts",
            "metadata-pk-fallback-reason-mismatch",
            "protocol-column-batch-fallback-reason-mismatch",
            "join-pk-hits-over-keys",
        ] {
            assert!(
                codes.iter().any(|code| code == expected),
                "missing diagnostic {expected}; got {codes:?}"
            );
        }
    }

    #[test]
    fn engine_counter_diagnostics_find_impossible_pending_column_batch_states() {
        let counters = EngineCountersSnapshot {
            protocol_column_batch_pending_opened: 1,
            protocol_column_batch_pending_completed: 2,
            protocol_column_batch_pending_current: 3,
            protocol_column_batch_pending_max: 2,
            protocol_column_batch_pending_rows_current: 128,
            protocol_column_batch_pending_rows_max: 64,
            protocol_column_batch_pending_bytes_current: 8192,
            protocol_column_batch_pending_bytes_max: 4096,
            ..EngineCountersSnapshot::default()
        };
        let codes = engine_counter_diagnostics(&counters)
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect::<Vec<_>>();

        for expected in [
            "protocol-column-batch-pending-close-over-open",
            "protocol-column-batch-pending-current-over-open",
            "protocol-column-batch-pending-current-over-max",
            "protocol-column-batch-pending-rows-current-over-max",
            "protocol-column-batch-pending-bytes-current-over-max",
        ] {
            assert!(
                codes.iter().any(|code| code == expected),
                "missing diagnostic {expected}; got {codes:?}"
            );
        }

        let counters = EngineCountersSnapshot {
            protocol_column_batch_pending_rows_current: 1,
            protocol_column_batch_pending_bytes_current: 1,
            protocol_column_batch_pending_rows_max: 1,
            protocol_column_batch_pending_bytes_max: 1,
            ..EngineCountersSnapshot::default()
        };
        let codes = engine_counter_diagnostics(&counters)
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect::<Vec<_>>();

        for expected in [
            "protocol-column-batch-pending-rows-without-current",
            "protocol-column-batch-pending-bytes-without-current",
            "protocol-column-batch-pending-rows-max-without-batches",
            "protocol-column-batch-pending-bytes-max-without-batches",
        ] {
            assert!(
                codes.iter().any(|code| code == expected),
                "missing diagnostic {expected}; got {codes:?}"
            );
        }
    }

    #[test]
    fn isolated_cli_parses_join_parent_selectivity_sweep() {
        let config = parse_config(&[
            "--only-case",
            "join.parent.sweep",
            "--participant",
            "server",
        ])
        .unwrap();

        assert_eq!(config.only_case, Some(QueryCase::JoinParentSweep));
    }

    #[test]
    fn isolated_cli_parses_join_parent_distribution_sweep() {
        let config = parse_config(&[
            "--only-case",
            "join.parent.distribution",
            "--participant",
            "server",
        ])
        .unwrap();

        assert_eq!(config.only_case, Some(QueryCase::JoinParentDistribution));
    }

    #[test]
    fn join_parent_distribution_rejects_postgres_participant() {
        let error = parse_config(&[
            "--only-case",
            "join.parent.distribution",
            "--participant",
            "postgres",
        ])
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("supports only --participant server"));
    }

    #[test]
    fn isolated_cli_parses_server_only_pk_membership_gate() {
        let config = parse_config(&[
            "--only-case",
            "pk.membership",
            "--participant",
            "server",
            "--warmup",
            "7",
            "--repeats",
            "31",
        ])
        .unwrap();

        assert_eq!(config.only_case, Some(QueryCase::PkMembership));
        assert_eq!(config.participants, vec![Participant::Server]);
        assert_eq!(config.warmup_runs, 7);
        assert_eq!(config.repeat_runs, 31);
    }

    #[test]
    fn pk_membership_gate_rejects_postgres_participant() {
        let error = parse_config(&["--only-case", "pk.membership", "--participant", "postgres"])
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("supports only --participant server"));
    }

    #[test]
    fn isolated_cli_rejects_too_few_repeats() {
        let error = parse_config(&["--only-case", "join.parent", "--repeats", "2"]).unwrap_err();

        assert!(error.to_string().contains("at least 5 measured repeats"));
    }

    #[test]
    fn isolated_cli_rejects_unknown_case() {
        let error = parse_config(&["--only-case", "scan.full"]).unwrap_err();

        assert!(error.to_string().contains("unsupported isolated case"));
    }

    #[test]
    fn seed_generation_reuses_valid_manifest_and_regenerates_damaged_file() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut config = parse_config(&["--scale", "dev"]).unwrap();
        config.root = temp.path().to_path_buf();
        let layout = BenchLayout::new(&config).unwrap();
        fs::create_dir_all(&layout.pg_root).unwrap();
        layout.ensure(&config.participants).unwrap();
        let seed_dir = layout.seed_dir(config.scale);

        let first = measure_seed_generation(&config, &seed_dir).expect("first seed generation");
        assert!(first.generated, "first seed should be generated");
        assert_eq!(first.total_rows, Scale::Dev.total_rows());
        assert_eq!(first.files.len(), TABLE_COUNT);
        assert!(seed_dir.join(SEED_MANIFEST_FILE).exists());

        let second = measure_seed_generation(&config, &seed_dir).expect("seed reuse");
        assert!(!second.generated, "valid seed should be reused");
        assert_eq!(second.generated_csv_bytes, first.generated_csv_bytes);
        assert_eq!(
            second.files[0].checksum_fnv1a64,
            first.files[0].checksum_fnv1a64
        );

        let first_csv = seed_csv_path(&seed_dir, 1);
        fs::OpenOptions::new()
            .append(true)
            .open(&first_csv)
            .expect("open seed file")
            .write_all(b"#corrupt\n")
            .expect("corrupt seed file");

        let third = measure_seed_generation(&config, &seed_dir).expect("regenerate damaged seed");
        assert!(third.generated, "damaged seed should be regenerated");
        assert_eq!(third.generated_csv_bytes, first.generated_csv_bytes);
        assert_eq!(
            third.files[0].checksum_fnv1a64,
            first.files[0].checksum_fnv1a64
        );
    }

    #[test]
    fn repeat_summary_preserves_raw_order_and_computes_median_and_min() {
        let summary = RepeatSummary::new(
            CaseProfile::RestartHot,
            vec![11.0],
            vec![9.0, 3.0, 5.0, 7.0, 1.0],
        )
        .unwrap();

        assert_eq!(summary.profile, CaseProfile::RestartHot);
        assert_eq!(summary.warmup_ms, vec![11.0]);
        assert_eq!(summary.measured_ms, vec![9.0, 3.0, 5.0, 7.0, 1.0]);
        assert_eq!(summary.restart_cold_first_ms, Some(11.0));
        assert_eq!(summary.median_ms, 5.0);
        assert_eq!(summary.min_ms, 1.0);
    }

    #[test]
    fn expected_join_parent_count_matches_official_gates() {
        assert_eq!(expected_join_parent_count(10_000_000), 924);
        assert_eq!(expected_join_parent_count(100_000_000), 9_196);
    }

    #[test]
    fn expected_child_bucket_range_matches_generated_rows() {
        for total_rows in [12_000, 10_000_000, 100_000_000] {
            let child_rows = rows_for_table(total_rows, 60);
            for (first, last) in [(10, 10), (10, 19), (10, 109), (10, 508), (1_000, 1_000)] {
                let brute_force = (1..=child_rows)
                    .filter(|id| {
                        let bucket = id % 997;
                        bucket >= first && bucket <= last
                    })
                    .count() as u64;
                assert_eq!(
                    expected_child_bucket_range_count(total_rows, first, last),
                    brute_force,
                    "total_rows={total_rows}, buckets={first}..{last}"
                );
            }
        }
    }

    #[test]
    fn expected_delete_rollback_cardinality_matches_generated_rows() {
        for total_rows in [12_000, 1_000_000, 10_000_000, 100_000_000] {
            let rows = rows_for_table(total_rows, 43);
            let expected = (1..=rows).filter(|id| id % 997 == 18).count() as u64;
            assert_eq!(expected_table_bucket_count(total_rows, 43, 18), expected);

            let expected_sum: i128 = (1..=rows)
                .map(|id| i128::from(GeneratedRow::new(43, id, 0).amount))
                .sum();
            assert_eq!(expected_table_amount_sum(total_rows, 43), expected_sum);
        }
    }

    #[test]
    fn access_path_extraction_keeps_join_and_scan_ids() {
        let paths = extract_access_paths(&[
            "SELECT".to_string(),
            "  Join Access Path: join.index_nested_loop.pk".to_string(),
            "    Access Path: scan.cold_artifact".to_string(),
            "    unrelated detail".to_string(),
        ]);

        assert_eq!(
            paths,
            vec![
                "Join Access Path: join.index_nested_loop.pk",
                "Access Path: scan.cold_artifact",
            ]
        );
    }

    #[test]
    fn descendant_pids_includes_root_and_nested_children() {
        let parents = HashMap::from([(10, 1), (11, 10), (12, 11), (20, 1)]);

        let included = descendant_pids(10, &parents);

        assert!(included.contains(&10));
        assert!(included.contains(&11));
        assert!(included.contains(&12));
        assert!(!included.contains(&20));
    }

    #[test]
    fn process_delta_uses_saturating_counters() {
        let first = ProcessAggregate {
            user_ticks: 100,
            system_ticks: 50,
            read_bytes: 20,
            write_bytes: 100,
            voluntary_context_switches: 10,
            ..ProcessAggregate::default()
        };
        let last = ProcessAggregate {
            rss_bytes: 4096,
            vsize_bytes: 8192,
            user_ticks: 130,
            system_ticks: 45,
            read_bytes: 25,
            write_bytes: 90,
            voluntary_context_switches: 14,
            ..ProcessAggregate::default()
        };

        let delta = subtract_process(last, first);

        assert_eq!(delta.rss_bytes, 4096);
        assert_eq!(delta.vsize_bytes, 8192);
        assert_eq!(delta.user_ticks, 30);
        assert_eq!(delta.system_ticks, 0);
        assert_eq!(delta.read_bytes, 5);
        assert_eq!(delta.write_bytes, 0);
        assert_eq!(delta.voluntary_context_switches, 4);
    }
}
