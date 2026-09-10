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

//! Bound logical relation graph for a SELECT table expression.
//!
//! The parser intentionally preserves SQL text as a binary `JoinSource` tree.
//! Physical planning must not confuse that syntax tree with an execution
//! order. This graph assigns stable relation identities, records the complete
//! relation set on both sides of every edge, and makes outer/derived barriers
//! explicit before any source is opened.

use std::sync::Arc;

use radixdb_sql::ast::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalRelationKind {
    Table,
    Derived,
    Cte,
    Values,
    Function,
    Opaque,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalRelation {
    pub ordinal: usize,
    pub visible_name: Option<String>,
    pub kind: LogicalRelationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinReorderBarrier {
    ReorderableInner,
    Cross,
    Left,
    Right,
    Full,
    NaturalOrUsing,
    Other,
}

impl JoinReorderBarrier {
    pub const fn permits_inner_reorder(self) -> bool {
        matches!(self, Self::ReorderableInner)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalJoinEdge {
    pub ordinal: usize,
    pub left_relations: Arc<[usize]>,
    pub right_relations: Arc<[usize]>,
    pub barrier: JoinReorderBarrier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalJoinGraph {
    pub relations: Arc<[LogicalRelation]>,
    pub edges: Arc<[LogicalJoinEdge]>,
}

impl LogicalJoinGraph {
    pub fn bind(table_expression: &Expression) -> Option<Self> {
        if !matches!(table_expression, Expression::JoinSource(_)) {
            return None;
        }
        let mut builder = LogicalJoinGraphBuilder::default();
        builder.bind_subtree(table_expression);
        Some(Self {
            relations: Arc::from(builder.relations),
            edges: Arc::from(builder.edges),
        })
    }

    pub fn reorderable_edge_count(&self) -> usize {
        self.edges
            .iter()
            .filter(|edge| edge.barrier.permits_inner_reorder())
            .count()
    }
}

#[derive(Default)]
struct LogicalJoinGraphBuilder {
    relations: Vec<LogicalRelation>,
    edges: Vec<LogicalJoinEdge>,
}

impl LogicalJoinGraphBuilder {
    fn bind_subtree(&mut self, expression: &Expression) -> Vec<usize> {
        if let Expression::JoinSource(join) = expression {
            let left_relations = self.bind_subtree(&join.left);
            let right_relations = self.bind_subtree(&join.right);
            self.edges.push(LogicalJoinEdge {
                ordinal: self.edges.len(),
                left_relations: Arc::from(left_relations.clone()),
                right_relations: Arc::from(right_relations.clone()),
                barrier: classify_barrier(join),
            });
            let mut relations = left_relations;
            relations.extend(right_relations);
            relations
        } else {
            let ordinal = self.relations.len();
            let (visible_name, kind) = classify_relation(expression);
            self.relations.push(LogicalRelation {
                ordinal,
                visible_name,
                kind,
            });
            vec![ordinal]
        }
    }
}

fn classify_barrier(join: &radixdb_sql::ast::JoinTableSource) -> JoinReorderBarrier {
    if !join.using_columns.is_empty() || join.join_type.to_uppercase().contains("NATURAL") {
        return JoinReorderBarrier::NaturalOrUsing;
    }
    match join.join_type.trim().to_uppercase().as_str() {
        "INNER" if join.condition.is_some() => JoinReorderBarrier::ReorderableInner,
        "CROSS" => JoinReorderBarrier::Cross,
        "LEFT" | "LEFT OUTER" => JoinReorderBarrier::Left,
        "RIGHT" | "RIGHT OUTER" => JoinReorderBarrier::Right,
        "FULL" | "FULL OUTER" => JoinReorderBarrier::Full,
        _ => JoinReorderBarrier::Other,
    }
}

fn classify_relation(expression: &Expression) -> (Option<String>, LogicalRelationKind) {
    match expression {
        Expression::TableSource(source) => (
            Some(
                source
                    .alias
                    .as_ref()
                    .unwrap_or(&source.name)
                    .value_lower()
                    .to_string(),
            ),
            LogicalRelationKind::Table,
        ),
        Expression::SubquerySource(source) => (
            source
                .alias
                .as_ref()
                .map(|alias| alias.value_lower().to_string()),
            LogicalRelationKind::Derived,
        ),
        Expression::CteReference(source) => (
            Some(
                source
                    .alias
                    .as_ref()
                    .unwrap_or(&source.name)
                    .value_lower()
                    .to_string(),
            ),
            LogicalRelationKind::Cte,
        ),
        Expression::ValuesSource(source) => (
            source
                .alias
                .as_ref()
                .map(|alias| alias.value_lower().to_string()),
            LogicalRelationKind::Values,
        ),
        Expression::FunctionTableSource(source) => (
            Some(
                source
                    .alias
                    .as_ref()
                    .unwrap_or(&source.function)
                    .value_lower()
                    .to_string(),
            ),
            LogicalRelationKind::Function,
        ),
        Expression::Aliased(source) => (
            Some(source.alias.value_lower().to_string()),
            LogicalRelationKind::Derived,
        ),
        _ => (None, LogicalRelationKind::Opaque),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::Statement;

    fn graph(sql: &str) -> LogicalJoinGraph {
        let mut statements = radixdb_sql::parse_sql(sql).unwrap();
        let Statement::Select(statement) = statements.remove(0) else {
            panic!("expected SELECT")
        };
        LogicalJoinGraph::bind(statement.table_expr.as_deref().unwrap()).unwrap()
    }

    #[test]
    fn relation_sets_and_outer_barriers_are_explicit() {
        let graph = graph(
            "SELECT a.id FROM a \
             INNER JOIN b ON b.a_id = a.id \
             LEFT JOIN (SELECT id FROM c) c1 ON c1.id = b.c_id \
             INNER JOIN d ON d.id = a.d_id",
        );

        assert_eq!(graph.relations.len(), 4);
        assert_eq!(graph.relations[0].visible_name.as_deref(), Some("a"));
        assert_eq!(graph.relations[2].visible_name.as_deref(), Some("c1"));
        assert_eq!(graph.relations[2].kind, LogicalRelationKind::Derived);
        assert_eq!(graph.edges.len(), 3);
        assert_eq!(graph.edges[0].left_relations.as_ref(), &[0]);
        assert_eq!(graph.edges[0].right_relations.as_ref(), &[1]);
        assert_eq!(graph.edges[0].barrier, JoinReorderBarrier::ReorderableInner);
        assert_eq!(graph.edges[1].left_relations.as_ref(), &[0, 1]);
        assert_eq!(graph.edges[1].right_relations.as_ref(), &[2]);
        assert_eq!(graph.edges[1].barrier, JoinReorderBarrier::Left);
        assert_eq!(graph.edges[2].left_relations.as_ref(), &[0, 1, 2]);
        assert_eq!(graph.edges[2].right_relations.as_ref(), &[3]);
        assert_eq!(graph.reorderable_edge_count(), 2);
    }

    #[test]
    fn using_and_cross_edges_are_not_silently_reorderable() {
        let graph = graph("SELECT * FROM a JOIN b USING (id) CROSS JOIN c");
        assert_eq!(graph.edges[0].barrier, JoinReorderBarrier::NaturalOrUsing);
        assert_eq!(graph.edges[1].barrier, JoinReorderBarrier::Cross);
        assert_eq!(graph.reorderable_edge_count(), 0);
    }
}
