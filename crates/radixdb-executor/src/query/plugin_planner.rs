#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PluginPlannerProbeSnapshot {
    pub attempts: u64,
    pub plans: u64,
    pub spans: u64,
    pub candidates: u64,
    pub rechecks: u64,
    pub fallbacks: u64,
}

thread_local! {
    static PLUGIN_PLANNER_PROBES: RefCell<Vec<PluginPlannerProbeSnapshot>> = const {
        RefCell::new(Vec::new())
    };
}

pub(crate) fn begin_plugin_planner_probe() {
    PLUGIN_PLANNER_PROBES.with(|probes| {
        probes.borrow_mut().push(PluginPlannerProbeSnapshot::default());
    });
}

pub(crate) fn end_plugin_planner_probe() -> PluginPlannerProbeSnapshot {
    PLUGIN_PLANNER_PROBES.with(|probes| probes.borrow_mut().pop().unwrap_or_default())
}

fn record_plugin_planner_probe(update: impl FnOnce(&mut PluginPlannerProbeSnapshot)) {
    PLUGIN_PLANNER_PROBES.with(|probes| {
        if let Some(probe) = probes.borrow_mut().last_mut() {
            update(probe);
        }
    });
}

fn planner_deadline_unix_ns(timeout_ms: u64) -> u64 {
    if timeout_ms == 0 {
        return u64::MAX;
    }
    let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    u64::try_from(now.as_nanos())
        .unwrap_or(u64::MAX)
        .saturating_add(timeout_ms.saturating_mul(1_000_000))
}

fn plugin_indexed_column(
    expression: &Expression,
    table_name: &str,
    table_alias: Option<&str>,
    columns: &[String],
) -> Option<(usize, String)> {
    let name = match expression {
        Expression::Identifier(identifier) => identifier.value_lower.as_str(),
        Expression::QualifiedIdentifier(identifier)
            if !identifier.is_multi_part_path()
                && (identifier.qualifier.value_lower.eq_ignore_ascii_case(table_name)
                    || table_alias.is_some_and(|alias| {
                        identifier.qualifier.value_lower.eq_ignore_ascii_case(alias)
                    })) =>
        {
            identifier.name.value_lower.as_str()
        }
        _ => return None,
    };
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
        .map(|index| (index, name.to_owned()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CatalogBoundPlannerPlanIdentity {
    catalog_generation: u64,
    table_id: radixdb_catalog::ObjectId,
    table_revision: u64,
    index_id: radixdb_catalog::ObjectId,
    index_revision: u64,
    support_revision: u64,
    function_revision: u64,
    operator_class_revision: u64,
    plugin: radixdb_plugin_host::PlannerPlanIdentity,
}

struct PluginCandidateRows {
    rows: RowVec,
    #[allow(dead_code)]
    identity: CatalogBoundPlannerPlanIdentity,
}

impl Executor {
    /// Ask a catalog-bound support callback for declarative key ranges and
    /// execute those ranges through the generic storage contract. The plugin
    /// never receives a table, index, page, WAL, MVCC or transaction handle.
    fn try_plugin_candidate_scan(
        &self,
        table_name: &str,
        table_alias: Option<&str>,
        table: &dyn radixdb_storage::traits::Table,
        predicate: Option<&Expression>,
        all_columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<Option<PluginCandidateRows>> {
        if table.has_local_changes() || ctx.outer_row().is_some() {
            return Ok(None);
        }
        let Some(Expression::FunctionCall(function)) = predicate else {
            return Ok(None);
        };
        if function.is_distinct || function.filter.is_some() || !function.order_by.is_empty() {
            return Ok(None);
        }

        let mut indexed_argument = None;
        for (argument, expression) in function.arguments.iter().enumerate() {
            if let Some((column_index, column_name)) =
                plugin_indexed_column(expression, table_name, table_alias, all_columns)
            {
                if indexed_argument.is_some() {
                    return Ok(None);
                }
                indexed_argument = Some((argument, column_index, column_name));
            }
        }
        let Some((indexed_argument, column_index, column_name)) = indexed_argument else {
            return Ok(None);
        };

        let mut constants = Vec::with_capacity(function.arguments.len());
        let mut argument_types = Vec::with_capacity(function.arguments.len());
        for (argument, expression) in function.arguments.iter().enumerate() {
            if argument == indexed_argument {
                constants.push(None);
                argument_types.push(Some(table.schema().columns()[column_index].logical_type()));
                continue;
            }
            let Ok(evaluator) = ExpressionEval::compile(expression, &[]) else {
                return Ok(None);
            };
            let Ok(value) = evaluator.with_context(ctx).eval_slice(&Row::new()) else {
                return Ok(None);
            };
            argument_types.push((!value.is_null()).then(|| value.logical_type()));
            constants.push(Some(value));
        }

        let Some((target_function_id, _)) =
            crate::procedural::function::bind_stored_function_dependency(
                self,
                function.function.as_str(),
                &argument_types,
            )?
        else {
            return Ok(None);
        };
        let (catalog, catalog_is_stable) = crate::procedural::transaction_visible_catalog(self)?;
        if !catalog_is_stable {
            return Ok(None);
        }
        let Some(target_function) = catalog.object(target_function_id) else {
            return Err(Error::internal(
                "plugin planner target function disappeared from the pinned catalog",
            ));
        };
        let radixdb_catalog::CatalogPayload::Function(function_payload) =
            target_function.payload()
        else {
            return Err(Error::internal(
                "plugin planner target catalog object is not a function",
            ));
        };
        let Some(function_definition) = function_payload.native_definition() else {
            return Ok(None);
        };
        if !matches!(
            function_definition.result(),
            radixdb_catalog::RoutineResult::Scalar { data_type, .. }
                if data_type.logical_type() == radixdb_core::DataType::Boolean
        ) {
            return Ok(None);
        }

        let Some(table_object) = catalog
            .find_relation(radixdb_catalog::ObjectId::BOOTSTRAP_NAMESPACE, table_name)
            .map_err(|error| Error::internal(format!("catalog table lookup failed: {error}")))?
        else {
            return Ok(None);
        };
        let radixdb_catalog::CatalogPayload::Table(table_payload) = table_object.payload() else {
            return Ok(None);
        };
        let Some(column_object) = catalog
            .find_column(table_object.id(), &column_name)
            .map_err(|error| Error::internal(format!("catalog column lookup failed: {error}")))?
        else {
            return Ok(None);
        };

        for support_object in catalog.objects_of_kind(radixdb_catalog::ObjectKind::PlannerSupport) {
            let radixdb_catalog::CatalogPayload::PlannerSupport(support) = support_object.payload()
            else {
                return Err(Error::internal(
                    "catalog admitted PlannerSupport with a different payload",
                ));
            };
            if support.target_function_id() != Some(target_function_id) {
                continue;
            }
            let Some(operator_class_id) = support.target_operator_class_id() else {
                continue;
            };
            let matching_index = table_payload.index_ids().iter().find_map(|index_id| {
                let index_object = catalog.object(*index_id)?;
                let radixdb_catalog::CatalogPayload::Index(index) = index_object.payload() else {
                    return None;
                };
                (index.operator_class_id() == Some(operator_class_id)
                    && index.key_column_ids() == [column_object.id()]
                    && index.predicate_sql().is_none())
                .then_some(index_object)
            });
            let Some(index_object) = matching_index else {
                continue;
            };
            let Some(operator_class_object) = catalog.object(operator_class_id) else {
                return Err(Error::internal(
                    "planner support operator class disappeared from the pinned catalog",
                ));
            };

            crate::authorization::authorize_routine_invocation(
                self,
                ctx.principal_id(),
                ctx.effective_principal_id(),
                target_function_id,
            )?;
            let normalized = constants
                .iter()
                .enumerate()
                .map(|(argument, value)| {
                    if argument == indexed_argument {
                        radixdb_plugin_host::NormalizedPredicateArgument::IndexedColumn
                    } else {
                        radixdb_plugin_host::NormalizedPredicateArgument::Constant(
                            value.as_ref().expect("non-indexed argument was evaluated"),
                        )
                    }
                })
                .collect::<Vec<_>>();
            record_plugin_planner_probe(|probe| probe.attempts += 1);
            let outcome = self
                .plugin_registry
                .invoke_planner_support(
                    support_object.id().into_bytes(),
                    &normalized,
                    radixdb_plugin_host::InvocationLimits {
                        cancel_check: Some(crate::context::current_query_is_cancelled),
                        deadline_unix_ns: planner_deadline_unix_ns(ctx.timeout_ms()),
                    },
                )
                .map_err(|error| {
                    Error::internal(format!(
                        "planner support {} violated its admitted contract: {error}",
                        support_object.id()
                    ))
                })?;
            let radixdb_plugin_host::PlannerSupportOutcome::Plan(plan) = outcome else {
                record_plugin_planner_probe(|probe| probe.fallbacks += 1);
                return Ok(None);
            };
            let table_rows = table.row_count_hint() as u64;
            if table_rows != 0 && plan.estimated_rows >= table_rows {
                record_plugin_planner_probe(|probe| probe.fallbacks += 1);
                return Ok(None);
            }
            let ranges = plan
                .spans
                .iter()
                .map(|span| radixdb_storage::traits::IndexKeyRange {
                    start: span.start.clone(),
                    end: span.end.clone(),
                })
                .collect::<Vec<_>>();
            let Some(row_ids) = table.collect_row_ids_by_index_ranges(
                index_object.name().display().as_str(),
                &ranges,
            ) else {
                record_plugin_planner_probe(|probe| probe.fallbacks += 1);
                return Ok(None);
            };
            let row_ids = row_ids?;
            let candidate_count = row_ids.len() as u64;
            let mut rows = table.collect_rows_by_ids(&row_ids)?;
            let rechecks = if plan.requires_recheck {
                let mut evaluator = ExpressionEval::compile(
                    predicate.expect("planner predicate is present"),
                    all_columns,
                )?
                .with_context(ctx);
                let input_count = rows.len() as u64;
                let mut filtered = RowVec::with_capacity(rows.len());
                for (row_id, row) in rows {
                    if evaluator.eval_bool_checked(&row)? {
                        filtered.push((row_id, row));
                    }
                }
                rows = filtered;
                input_count
            } else {
                0
            };
            record_plugin_planner_probe(|probe| {
                probe.plans += 1;
                probe.spans += plan.spans.len() as u64;
                probe.candidates += candidate_count;
                probe.rechecks += rechecks;
            });
            return Ok(Some(PluginCandidateRows {
                rows,
                identity: CatalogBoundPlannerPlanIdentity {
                    catalog_generation: catalog.meta().catalog_generation(),
                    table_id: table_object.id(),
                    table_revision: table_object.definition_revision(),
                    index_id: index_object.id(),
                    index_revision: index_object.definition_revision(),
                    support_revision: support_object.definition_revision(),
                    function_revision: target_function.definition_revision(),
                    operator_class_revision: operator_class_object.definition_revision(),
                    plugin: plan.identity,
                },
            }));
        }
        Ok(None)
    }
}
