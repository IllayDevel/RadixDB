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
    pub(super) fn parse_create_schema_statement(
        &mut self,
        token: Token,
    ) -> Option<CreateSchemaStatement> {
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        Some(CreateSchemaStatement {
            token,
            name: self.parse_object_name_current()?,
        })
    }

    pub(super) fn parse_create_principal_statement(
        &mut self,
        token: Token,
    ) -> Option<CreatePrincipalStatement> {
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.cur_token_as_column_identifier();
        if self.peek_token_is_punctuator(".") {
            self.add_error("principal names are global and cannot be qualified".to_string());
            return None;
        }
        let password = if self.peek_token_is_keyword("PASSWORD") {
            self.next_token();
            if !self.peek_token_is(TokenType::String) {
                self.add_error("PASSWORD requires a string literal".to_string());
                return None;
            }
            self.next_token();
            Some(unquote_security_secret(self.cur_token.literal.as_str()))
        } else {
            None
        };
        Some(CreatePrincipalStatement {
            token,
            name,
            password,
        })
    }

    pub(super) fn parse_create_role_statement(
        &mut self,
        token: Token,
    ) -> Option<CreateRoleStatement> {
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.cur_token_as_column_identifier();
        if self.peek_token_is_punctuator(".") {
            self.add_error("role names are global and cannot be qualified".to_string());
            return None;
        }
        Some(CreateRoleStatement { token, name })
    }

    pub(super) fn parse_alter_security_subject_statement(
        &mut self,
        token: Token,
        kind: SecuritySubjectKindSyntax,
    ) -> Option<AlterSecuritySubjectStatement> {
        self.next_token();
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_global_security_subject("security subject")?;
        self.next_token();
        let action = if self.cur_token_is_keyword("ENABLE") {
            AlterSecuritySubjectActionSyntax::Enable
        } else if self.cur_token_is_keyword("DISABLE") {
            AlterSecuritySubjectActionSyntax::Disable
        } else if self.cur_token_is_keyword("RENAME") {
            if !self.expect_keyword("TO") || !self.expect_peek_procedural_identifier() {
                return None;
            }
            AlterSecuritySubjectActionSyntax::RenameTo(
                self.parse_global_security_subject("new security subject")?,
            )
        } else if self.cur_token_is_keyword("PASSWORD") {
            if kind != SecuritySubjectKindSyntax::Principal {
                self.add_error("PASSWORD is valid only for PRINCIPAL".to_string());
                return None;
            }
            if self.peek_token_is_keyword("NULL") {
                self.next_token();
                AlterSecuritySubjectActionSyntax::ClearPassword
            } else if self.peek_token_is(TokenType::String) {
                self.next_token();
                AlterSecuritySubjectActionSyntax::SetPassword(unquote_security_secret(
                    self.cur_token.literal.as_str(),
                ))
            } else {
                self.add_error("PASSWORD requires a string literal or NULL".to_string());
                return None;
            }
        } else {
            self.add_error(
                "expected ENABLE, DISABLE, RENAME TO, or PASSWORD for security subject".to_string(),
            );
            return None;
        };
        Some(AlterSecuritySubjectStatement {
            token,
            kind,
            name,
            action,
        })
    }

    pub(super) fn parse_drop_security_subject_statement(
        &mut self,
        token: Token,
        kind: SecuritySubjectKindSyntax,
    ) -> Option<DropSecuritySubjectStatement> {
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_global_security_subject("security subject")?;
        let behavior = if self.peek_token_is_keyword("CASCADE") {
            self.next_token();
            DropBehaviorSyntax::Cascade
        } else {
            if self.peek_token_is_keyword("RESTRICT") {
                self.next_token();
            }
            DropBehaviorSyntax::Restrict
        };
        Some(DropSecuritySubjectStatement {
            token,
            kind,
            name,
            behavior,
        })
    }

    pub(super) fn parse_grant_statement(&mut self) -> Option<GrantStatement> {
        let token = self.cur_token.clone();
        self.next_token();
        let grant = if let Some(kind) = privilege_kind(&self.cur_token) {
            let privileges = self.parse_privilege_list(kind)?;
            if !self.expect_keyword("ON") {
                return None;
            }
            let target = self.parse_privilege_target_after_on()?;
            if !self.expect_keyword("TO") || !self.expect_peek_procedural_identifier() {
                return None;
            }
            let grantee = self.parse_global_security_subject("grantee")?;
            let grant_option = if self.peek_token_is_keyword("WITH") {
                self.next_token();
                if !self.expect_keyword("GRANT") || !self.expect_keyword("OPTION") {
                    return None;
                }
                true
            } else {
                false
            };
            GrantSyntax::ObjectPrivileges {
                privileges,
                target,
                grantee,
                grant_option,
            }
        } else {
            let role = self.parse_global_security_subject("role")?;
            if !self.expect_keyword("TO") || !self.expect_peek_procedural_identifier() {
                return None;
            }
            let member = self.parse_global_security_subject("role member")?;
            let admin_option = if self.peek_token_is_keyword("WITH") {
                self.next_token();
                if !self.expect_keyword("ADMIN") || !self.expect_keyword("OPTION") {
                    return None;
                }
                true
            } else {
                false
            };
            GrantSyntax::RoleMembership {
                role,
                member,
                admin_option,
            }
        };
        Some(GrantStatement { token, grant })
    }

    pub(super) fn parse_revoke_statement(&mut self) -> Option<RevokeStatement> {
        let token = self.cur_token.clone();
        self.next_token();
        let grant_option_only = if self.cur_token_is_keyword("GRANT") {
            if !self.expect_keyword("OPTION") || !self.expect_keyword("FOR") {
                return None;
            }
            self.next_token();
            true
        } else {
            false
        };
        let admin_option_only = if self.cur_token_is_keyword("ADMIN") {
            if !self.expect_keyword("OPTION") || !self.expect_keyword("FOR") {
                return None;
            }
            self.next_token();
            true
        } else {
            false
        };
        let revoke = if let Some(kind) = privilege_kind(&self.cur_token) {
            if admin_option_only {
                self.add_error("ADMIN OPTION FOR requires a role membership".to_string());
                return None;
            }
            let privileges = self.parse_privilege_list(kind)?;
            if !self.expect_keyword("ON") {
                return None;
            }
            let target = self.parse_privilege_target_after_on()?;
            if !self.expect_keyword("FROM") || !self.expect_peek_procedural_identifier() {
                return None;
            }
            let grantee = self.parse_global_security_subject("grantee")?;
            RevokeSyntax::ObjectPrivileges {
                privileges,
                target,
                grantee,
                grant_option_only,
            }
        } else {
            if grant_option_only {
                self.add_error("GRANT OPTION FOR requires object privileges".to_string());
                return None;
            }
            let role = self.parse_global_security_subject("role")?;
            if !self.expect_keyword("FROM") || !self.expect_peek_procedural_identifier() {
                return None;
            }
            let member = self.parse_global_security_subject("role member")?;
            RevokeSyntax::RoleMembership {
                role,
                member,
                admin_option_only,
            }
        };
        let behavior = if self.peek_token_is_keyword("CASCADE") {
            self.next_token();
            DropBehaviorSyntax::Cascade
        } else {
            if self.peek_token_is_keyword("RESTRICT") {
                self.next_token();
            }
            DropBehaviorSyntax::Restrict
        };
        Some(RevokeStatement {
            token,
            revoke,
            behavior,
        })
    }

    pub(super) fn parse_alter_routine_owner_statement(
        &mut self,
        token: Token,
        kind: RoutineKindSyntax,
    ) -> Option<AlterOwnerStatement> {
        self.next_token();
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let signature = self.parse_routine_signature_current()?;
        if !self.expect_keyword("OWNER") || !self.expect_keyword("TO") {
            return None;
        }
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let owner = self.parse_global_security_subject("owner")?;
        let target = match kind {
            RoutineKindSyntax::Function => OwnershipTargetSyntax::Function(signature),
            RoutineKindSyntax::Procedure => OwnershipTargetSyntax::Procedure(signature),
        };
        Some(AlterOwnerStatement {
            token,
            target,
            owner,
        })
    }

    pub(super) fn parse_table_owner_after_name(
        &mut self,
        token: Token,
        name: ObjectName,
    ) -> Option<AlterOwnerStatement> {
        if !self.expect_keyword("OWNER") || !self.expect_keyword("TO") {
            return None;
        }
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        Some(AlterOwnerStatement {
            token,
            target: OwnershipTargetSyntax::Table(name),
            owner: self.parse_global_security_subject("owner")?,
        })
    }

    fn parse_privilege_list(
        &mut self,
        first_kind: ObjectPrivilegeSyntax,
    ) -> Option<Vec<PrivilegeSyntax>> {
        let mut privileges = Vec::new();
        let mut seen = BTreeSet::new();
        let mut kind = first_kind;
        loop {
            if !seen.insert(kind) {
                self.add_error(format!("duplicate {kind} privilege"));
                return None;
            }
            let columns = self.parse_optional_privilege_columns(kind)?;
            privileges.push(PrivilegeSyntax { kind, columns });
            if !self.peek_token_is_punctuator(",") {
                break;
            }
            self.next_token();
            self.next_token();
            kind = privilege_kind(&self.cur_token).or_else(|| {
                self.add_error("expected privilege name after ','".to_string());
                None
            })?;
        }
        Some(privileges)
    }

    fn parse_optional_privilege_columns(
        &mut self,
        kind: ObjectPrivilegeSyntax,
    ) -> Option<Vec<Identifier>> {
        if !self.peek_token_is_punctuator("(") {
            return Some(Vec::new());
        }
        if !matches!(
            kind,
            ObjectPrivilegeSyntax::Select
                | ObjectPrivilegeSyntax::Insert
                | ObjectPrivilegeSyntax::Update
        ) {
            self.add_error(format!("{kind} does not accept a column list"));
            return None;
        }
        self.next_token();
        let mut columns = Vec::new();
        let mut seen = BTreeSet::new();
        loop {
            if !self.expect_peek_procedural_identifier() {
                return None;
            }
            let column = self.cur_token_as_column_identifier();
            if !seen.insert(column.value_lower.to_string()) {
                self.add_error(format!("duplicate privilege column '{}'", column.value));
                return None;
            }
            columns.push(column);
            if self.peek_token_is_punctuator(")") {
                self.next_token();
                break;
            }
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' or ')' in privilege column list".to_string());
                return None;
            }
            self.next_token();
        }
        Some(columns)
    }

    fn parse_privilege_target_after_on(&mut self) -> Option<PrivilegeTargetSyntax> {
        self.next_token();
        let keyword = self.cur_token.literal.to_uppercase();
        match keyword.as_str() {
            "DATABASE" | "SCHEMA" | "TABLE" => {
                if !self.expect_peek_procedural_identifier() {
                    return None;
                }
                let name = self.parse_object_name_current()?;
                Some(match keyword.as_str() {
                    "DATABASE" => PrivilegeTargetSyntax::Database(name),
                    "SCHEMA" => PrivilegeTargetSyntax::Schema(name),
                    "TABLE" => PrivilegeTargetSyntax::Table(name),
                    _ => unreachable!(),
                })
            }
            "FUNCTION" | "PROCEDURE" => {
                if !self.expect_peek_procedural_identifier() {
                    return None;
                }
                let signature = self.parse_routine_signature_current()?;
                Some(if keyword == "FUNCTION" {
                    PrivilegeTargetSyntax::Function(signature)
                } else {
                    PrivilegeTargetSyntax::Procedure(signature)
                })
            }
            _ => {
                self.add_error(
                    "expected DATABASE, SCHEMA, TABLE, FUNCTION, or PROCEDURE after ON".to_string(),
                );
                None
            }
        }
    }

    fn parse_global_security_subject(&mut self, label: &str) -> Option<Identifier> {
        if !self.cur_token_is_procedural_identifier() {
            self.add_error(format!("expected {label} name"));
            return None;
        }
        let subject = self.cur_token_as_column_identifier();
        if self.peek_token_is_punctuator(".") {
            self.add_error(format!("{label} name is global and cannot be qualified"));
            return None;
        }
        Some(subject)
    }
}

fn unquote_security_secret(literal: &str) -> String {
    let inner = literal
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(literal);
    inner.replace("''", "'")
}

fn privilege_kind(token: &Token) -> Option<ObjectPrivilegeSyntax> {
    if token.token_type != TokenType::Keyword || token.quoted {
        return None;
    }
    Some(match token.literal.to_ascii_uppercase().as_str() {
        "CONNECT" => ObjectPrivilegeSyntax::Connect,
        "USAGE" => ObjectPrivilegeSyntax::Usage,
        "CREATE" => ObjectPrivilegeSyntax::Create,
        "SELECT" => ObjectPrivilegeSyntax::Select,
        "INSERT" => ObjectPrivilegeSyntax::Insert,
        "UPDATE" => ObjectPrivilegeSyntax::Update,
        "DELETE" => ObjectPrivilegeSyntax::Delete,
        "EXECUTE" => ObjectPrivilegeSyntax::Execute,
        _ => return None,
    })
}
