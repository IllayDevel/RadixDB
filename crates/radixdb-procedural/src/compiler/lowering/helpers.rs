use std::collections::{BTreeSet, VecDeque};

use radixdb_sql::{ProceduralStatement, SourceRange};

use super::{DraftBlock, DraftInstruction, DraftTerminator, Label};
use crate::{Diagnostic, DiagnosticKind, ProceduralResult, SourceSpan};

use super::super::{CompileIdentity, SemanticResolver};

pub(super) fn reachable_labels(
    blocks: &[DraftBlock],
    entry: Label,
) -> ProceduralResult<Vec<Label>> {
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from([entry]);
    while let Some(label) = queue.pop_front() {
        if !seen.insert(label) {
            continue;
        }
        let block = blocks.get(label.0).ok_or_else(|| {
            bind_error(
                DiagnosticKind::RuntimeInvalidIr,
                "compiler control-flow target is invalid",
                None,
            )
        })?;
        let Some((terminator, _)) = &block.terminator else {
            return Err(bind_error(
                DiagnosticKind::RuntimeInvalidIr,
                "compiler control-flow block is unterminated",
                None,
            ));
        };
        match terminator {
            DraftTerminator::Jump(target) => queue.push_back(*target),
            DraftTerminator::Branch {
                when_true,
                when_false,
                ..
            } => {
                queue.push_back(*when_true);
                queue.push_back(*when_false);
            }
            DraftTerminator::Return(_) | DraftTerminator::Raise(_) | DraftTerminator::Rethrow => {}
        }
        for instruction in &block.instructions {
            if let DraftInstruction::EnterExceptionRegion { routes, .. } = instruction {
                queue.extend(routes.iter().map(|route| route.handler));
            }
        }
    }
    Ok(seen.into_iter().collect())
}

pub(super) fn handler_observes_or_rethrows<R: SemanticResolver>(
    resolver: &mut R,
    statements: &[ProceduralStatement],
) -> bool {
    statements.iter().any(|statement| match statement {
        ProceduralStatement::Raise { kind: None, .. } => true,
        ProceduralStatement::Perform { expression, .. } => {
            resolver.is_observability_expression(expression)
        }
        ProceduralStatement::If {
            branches,
            otherwise,
            ..
        } => {
            branches
                .iter()
                .any(|(_, statements)| handler_observes_or_rethrows(resolver, statements))
                || handler_observes_or_rethrows(resolver, otherwise)
        }
        ProceduralStatement::Case {
            arms, otherwise, ..
        } => {
            arms.iter()
                .any(|arm| handler_observes_or_rethrows(resolver, &arm.statements))
                || handler_observes_or_rethrows(resolver, otherwise)
        }
        ProceduralStatement::Loop { statements, .. }
        | ProceduralStatement::While { statements, .. }
        | ProceduralStatement::For { statements, .. } => {
            handler_observes_or_rethrows(resolver, statements)
        }
        ProceduralStatement::Block(block) => {
            handler_observes_or_rethrows(resolver, &block.statements)
                || block
                    .handlers
                    .iter()
                    .any(|handler| handler_observes_or_rethrows(resolver, &handler.statements))
        }
        _ => false,
    })
}

pub(super) fn statement_span(statement: &ProceduralStatement) -> Option<&SourceRange> {
    match statement {
        ProceduralStatement::Assignment { span, .. }
        | ProceduralStatement::Call { span, .. }
        | ProceduralStatement::Perform { span, .. }
        | ProceduralStatement::If { span, .. }
        | ProceduralStatement::Case { span, .. }
        | ProceduralStatement::Loop { span, .. }
        | ProceduralStatement::While { span, .. }
        | ProceduralStatement::For { span, .. }
        | ProceduralStatement::LoopControl { span, .. }
        | ProceduralStatement::Return { span, .. }
        | ProceduralStatement::DynamicExecute { span, .. }
        | ProceduralStatement::OpenCursor { span, .. }
        | ProceduralStatement::FetchCursor { span, .. }
        | ProceduralStatement::CloseCursor { span, .. }
        | ProceduralStatement::Raise { span, .. } => Some(span),
        ProceduralStatement::Sql(sql) => Some(&sql.span),
        ProceduralStatement::Block(block) => Some(&block.span),
    }
}

pub(super) fn span(
    identity: &CompileIdentity,
    range: &SourceRange,
) -> ProceduralResult<SourceSpan> {
    SourceSpan::new(
        identity.object_id,
        identity.definition_revision,
        u32::try_from(range.start.offset).map_err(|_| {
            bind_error(
                DiagnosticKind::ParseLimitExceeded,
                "source span offset exceeds u32",
                None,
            )
        })?,
        u32::try_from(range.end.offset).map_err(|_| {
            bind_error(
                DiagnosticKind::ParseLimitExceeded,
                "source span offset exceeds u32",
                None,
            )
        })?,
        u32::try_from(range.start.line).unwrap_or(u32::MAX),
        u32::try_from(range.start.column).unwrap_or(u32::MAX),
        u32::try_from(range.end.line).unwrap_or(u32::MAX),
        u32::try_from(range.end.column).unwrap_or(u32::MAX),
    )
    .ok_or_else(|| {
        bind_error(
            DiagnosticKind::RuntimeInvalidIr,
            "parser produced an invalid source span",
            None,
        )
    })
}

pub(super) fn bind_error(
    kind: DiagnosticKind,
    message: impl Into<String>,
    source_span: Option<SourceSpan>,
) -> Diagnostic {
    Diagnostic::new(kind, message).with_primary_span(source_span)
}
