// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::collections::BTreeSet;

use super::*;

impl Parser {
    pub(crate) fn parse_drop_routine_statement(
        &mut self,
        token: Token,
        kind: RoutineKindSyntax,
    ) -> Option<DropRoutineStatement> {
        let if_exists = self.parse_optional_if_exists()?;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let signature = self.parse_routine_signature_current()?;
        let behavior = self.parse_optional_drop_behavior();
        Some(DropRoutineStatement {
            token,
            kind,
            signature,
            if_exists,
            behavior,
        })
    }

    pub(crate) fn parse_drop_trigger_statement(
        &mut self,
        token: Token,
    ) -> Option<DropTriggerStatement> {
        let if_exists = self.parse_optional_if_exists()?;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        if !self.expect_keyword("ON") || !self.expect_peek_procedural_identifier() {
            return None;
        }
        let table = self.parse_object_name_current()?;
        let behavior = self.parse_optional_drop_behavior();
        Some(DropTriggerStatement {
            token,
            name,
            table,
            if_exists,
            behavior,
        })
    }

    pub(crate) fn parse_drop_job_statement(&mut self, token: Token) -> Option<DropJobStatement> {
        let if_exists = self.parse_optional_if_exists()?;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        let behavior = self.parse_optional_drop_behavior();
        Some(DropJobStatement {
            token,
            name,
            if_exists,
            behavior,
        })
    }

    pub(crate) fn parse_alter_job_statement(&mut self, token: Token) -> Option<AlterJobStatement> {
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        let enabled = if self.peek_token_is_keyword("ENABLE") {
            self.next_token();
            true
        } else if self.peek_token_is_keyword("DISABLE") {
            self.next_token();
            false
        } else {
            self.add_error("ALTER JOB requires ENABLE or DISABLE".to_string());
            return None;
        };
        Some(AlterJobStatement {
            token,
            name,
            enabled,
        })
    }

    fn parse_optional_if_exists(&mut self) -> Option<bool> {
        if !self.peek_token_is_keyword("IF") {
            return Some(false);
        }
        self.next_token();
        self.expect_keyword("EXISTS").then_some(true)
    }

    fn parse_optional_drop_behavior(&mut self) -> DropBehaviorSyntax {
        if self.peek_token_is_keyword("CASCADE") {
            self.next_token();
            DropBehaviorSyntax::Cascade
        } else {
            if self.peek_token_is_keyword("RESTRICT") {
                self.next_token();
            }
            DropBehaviorSyntax::Restrict
        }
    }

    pub(crate) fn parse_create_trigger_statement(
        &mut self,
        create_token: Token,
        or_replace: bool,
    ) -> Option<CreateTriggerStatement> {
        let start = create_token.position;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        let timing = if self.peek_token_is_keyword("BEFORE") {
            self.next_token();
            TriggerTimingSyntax::Before
        } else if self.peek_token_is_keyword("AFTER") {
            self.next_token();
            TriggerTimingSyntax::After
        } else {
            self.add_error("expected BEFORE or AFTER in trigger definition".to_string());
            return None;
        };
        let events = self.parse_trigger_events()?;
        if !self.expect_keyword("ON") || !self.expect_peek_procedural_identifier() {
            return None;
        }
        let table = self.parse_object_name_current()?;
        if !self.expect_keyword("FOR") || !self.expect_keyword("EACH") {
            return None;
        }
        let level = if self.peek_token_is_keyword("ROW") {
            self.next_token();
            TriggerLevelSyntax::Row
        } else if self.peek_token_is_keyword("STATEMENT") {
            self.next_token();
            TriggerLevelSyntax::Statement
        } else {
            self.add_error("expected ROW or STATEMENT after FOR EACH".to_string());
            return None;
        };
        let priority = if self.peek_token_is_keyword("PRIORITY") {
            self.next_token();
            self.parse_signed_i32_after_current("trigger priority")?
        } else {
            1000
        };
        let when = if self.peek_token_is_keyword("WHEN") {
            self.next_token();
            if !self.peek_token_is_punctuator("(") {
                self.add_error("expected '(' after trigger WHEN".to_string());
                return None;
            }
            self.next_token();
            self.next_token();
            let condition = self.parse_expression(Precedence::Lowest)?;
            if !self.peek_token_is_punctuator(")") {
                self.add_error("expected ')' after trigger WHEN expression".to_string());
                return None;
            }
            self.next_token();
            Some(condition)
        } else {
            None
        };
        if !self.expect_keyword("EXECUTE")
            || !self.expect_keyword("FUNCTION")
            || !self.expect_peek_procedural_identifier()
        {
            return None;
        }
        let function = self.parse_routine_signature_current()?;
        if !self.peek_token_is_punctuator(";") {
            self.add_error("trigger definition must end with ';'".to_string());
            return None;
        }
        Some(CreateTriggerStatement {
            token: create_token,
            or_replace,
            name,
            timing,
            events,
            table,
            level,
            priority,
            when,
            function,
            span: self.source_range_through_peek_from(start),
        })
    }

    pub(crate) fn parse_create_job_statement(
        &mut self,
        create_token: Token,
    ) -> Option<CreateJobStatement> {
        let start = create_token.position;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        if !self.expect_keyword("SCHEDULE") {
            return None;
        }
        let schedule = if self.peek_token_is_keyword("EVERY") {
            self.next_token();
            if !self.peek_token_is_keyword("INTERVAL") {
                self.add_error("SCHEDULE EVERY requires an INTERVAL literal".to_string());
                return None;
            }
            self.next_token();
            JobScheduleSyntax::Every(self.parse_expression(Precedence::Lowest)?)
        } else if self.peek_token_is_keyword("AT") {
            self.next_token();
            if !self.peek_token_is_keyword("TIMESTAMP")
                && !self.peek_token_is_keyword("TIMESTAMPTZ")
            {
                self.add_error("SCHEDULE AT requires a TIMESTAMP literal".to_string());
                return None;
            }
            self.next_token();
            JobScheduleSyntax::At(self.parse_expression(Precedence::Lowest)?)
        } else {
            self.add_error("expected EVERY or AT after SCHEDULE".to_string());
            return None;
        };
        if !self.expect_keyword("RUN") || !self.expect_keyword("AS") {
            return None;
        }
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let principal = self.parse_object_name_current()?;
        if !self.expect_keyword("CALL") || !self.expect_peek_procedural_identifier() {
            return None;
        }
        let procedure = self.parse_object_name_current()?;
        if !self.peek_token_is_punctuator("(") {
            self.add_error("job CALL requires an argument list".to_string());
            return None;
        }
        self.next_token();
        let arguments = self.parse_call_arguments()?;
        let enabled = if self.peek_token_is_keyword("ENABLE") {
            self.next_token();
            true
        } else if self.peek_token_is_keyword("DISABLE") {
            self.next_token();
            false
        } else {
            self.add_error("job definition requires ENABLE or DISABLE".to_string());
            return None;
        };
        if !self.peek_token_is_punctuator(";") {
            self.add_error("job definition must end with ';'".to_string());
            return None;
        }
        Some(CreateJobStatement {
            token: create_token,
            name,
            schedule,
            principal,
            procedure,
            arguments,
            enabled,
            span: self.source_range_through_peek_from(start),
        })
    }

    fn parse_trigger_events(&mut self) -> Option<Vec<TriggerEventSyntax>> {
        let mut events = Vec::new();
        let mut seen = BTreeSet::new();
        loop {
            self.next_token();
            let (key, event) = if self.cur_token_is_keyword("INSERT") {
                ("INSERT", TriggerEventSyntax::Insert)
            } else if self.cur_token_is_keyword("DELETE") {
                ("DELETE", TriggerEventSyntax::Delete)
            } else if self.cur_token_is_keyword("UPDATE") {
                let mut columns = Vec::new();
                if self.peek_token_is_keyword("OF") {
                    self.next_token();
                    loop {
                        if !self.expect_peek_procedural_identifier() {
                            return None;
                        }
                        columns.push(self.cur_token_as_column_identifier());
                        if !self.peek_token_is_punctuator(",") {
                            break;
                        }
                        self.next_token();
                    }
                }
                ("UPDATE", TriggerEventSyntax::Update { columns })
            } else {
                self.add_error("expected INSERT, UPDATE, or DELETE trigger event".to_string());
                return None;
            };
            if !seen.insert(key) {
                self.add_error(format!("duplicate {key} trigger event"));
                return None;
            }
            events.push(event);
            if !self.peek_token_is_keyword("OR") {
                break;
            }
            self.next_token();
        }
        Some(events)
    }

    fn parse_signed_i32_after_current(&mut self, label: &str) -> Option<i32> {
        let negative = if self.peek_token_is_operator("-") {
            self.next_token();
            true
        } else {
            false
        };
        self.next_token();
        if self.cur_token.token_type != TokenType::Integer {
            self.add_error(format!("{label} must be a signed 32-bit integer"));
            return None;
        }
        let unsigned = self.cur_token.literal.parse::<i64>().ok()?;
        let value = if negative { -unsigned } else { unsigned };
        i32::try_from(value).ok().or_else(|| {
            self.add_error(format!("{label} is outside signed 32-bit range"));
            None
        })
    }

    pub(crate) fn parse_routine_signature_current(&mut self) -> Option<RoutineSignatureSyntax> {
        let name = self.parse_object_name_current()?;
        if !self.peek_token_is_punctuator("(") {
            self.add_error("routine signature requires '('".to_string());
            return None;
        }
        self.next_token();
        let mut argument_types = Vec::new();
        if self.peek_token_is_punctuator(")") {
            self.next_token();
            return Some(RoutineSignatureSyntax {
                name,
                argument_types,
            });
        }
        loop {
            self.next_token();
            argument_types.push(self.parse_procedural_type_current()?);
            if self.peek_token_is_punctuator(")") {
                self.next_token();
                break;
            }
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' or ')' in routine signature".to_string());
                return None;
            }
            self.next_token();
        }
        Some(RoutineSignatureSyntax {
            name,
            argument_types,
        })
    }
}
