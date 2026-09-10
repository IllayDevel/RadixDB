// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaStatement {
    pub token: Token,
    pub name: ObjectName,
}

impl fmt::Display for CreateSchemaStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CREATE SCHEMA {}", self.name)
    }
}

#[derive(Clone, PartialEq)]
pub struct CreatePrincipalStatement {
    pub token: Token,
    pub name: Identifier,
    /// Plaintext exists only in the transient parsed statement. Display is
    /// deliberately redacted and the catalog stores only a verifier.
    pub password: Option<String>,
}

impl fmt::Debug for CreatePrincipalStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreatePrincipalStatement")
            .field("token", &self.token)
            .field("name", &self.name)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl fmt::Display for CreatePrincipalStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CREATE PRINCIPAL {}", self.name)?;
        if self.password.is_some() {
            formatter.write_str(" PASSWORD '<redacted>'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRoleStatement {
    pub token: Token,
    pub name: Identifier,
}

impl fmt::Display for CreateRoleStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CREATE ROLE {}", self.name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecuritySubjectKindSyntax {
    Principal,
    Role,
}

impl fmt::Display for SecuritySubjectKindSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Principal => "PRINCIPAL",
            Self::Role => "ROLE",
        })
    }
}

#[derive(Clone, PartialEq)]
pub enum AlterSecuritySubjectActionSyntax {
    Enable,
    Disable,
    RenameTo(Identifier),
    SetPassword(String),
    ClearPassword,
}

impl fmt::Debug for AlterSecuritySubjectActionSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Enable => formatter.write_str("Enable"),
            Self::Disable => formatter.write_str("Disable"),
            Self::RenameTo(name) => formatter.debug_tuple("RenameTo").field(name).finish(),
            Self::SetPassword(_) => formatter
                .debug_tuple("SetPassword")
                .field(&"<redacted>")
                .finish(),
            Self::ClearPassword => formatter.write_str("ClearPassword"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterSecuritySubjectStatement {
    pub token: Token,
    pub kind: SecuritySubjectKindSyntax,
    pub name: Identifier,
    pub action: AlterSecuritySubjectActionSyntax,
}

impl fmt::Display for AlterSecuritySubjectStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ALTER {} {} ", self.kind, self.name)?;
        match &self.action {
            AlterSecuritySubjectActionSyntax::Enable => formatter.write_str("ENABLE"),
            AlterSecuritySubjectActionSyntax::Disable => formatter.write_str("DISABLE"),
            AlterSecuritySubjectActionSyntax::RenameTo(name) => {
                write!(formatter, "RENAME TO {name}")
            }
            AlterSecuritySubjectActionSyntax::SetPassword(_) => {
                formatter.write_str("PASSWORD '<redacted>'")
            }
            AlterSecuritySubjectActionSyntax::ClearPassword => formatter.write_str("PASSWORD NULL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropBehaviorSyntax {
    Restrict,
    Cascade,
}

impl fmt::Display for DropBehaviorSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Restrict => "RESTRICT",
            Self::Cascade => "CASCADE",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropSecuritySubjectStatement {
    pub token: Token,
    pub kind: SecuritySubjectKindSyntax,
    pub name: Identifier,
    pub behavior: DropBehaviorSyntax,
}

impl fmt::Display for DropSecuritySubjectStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DROP {} {} {}",
            self.kind, self.name, self.behavior
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectPrivilegeSyntax {
    Connect,
    Usage,
    Create,
    Select,
    Insert,
    Update,
    Delete,
    Execute,
}

impl fmt::Display for ObjectPrivilegeSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Connect => "CONNECT",
            Self::Usage => "USAGE",
            Self::Create => "CREATE",
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Execute => "EXECUTE",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrivilegeSyntax {
    pub kind: ObjectPrivilegeSyntax,
    pub columns: Vec<Identifier>,
}

impl fmt::Display for PrivilegeSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.kind)?;
        if !self.columns.is_empty() {
            formatter.write_str(" (")?;
            for (index, column) in self.columns.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(", ")?;
                }
                write!(formatter, "{column}")?;
            }
            formatter.write_str(")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrivilegeTargetSyntax {
    Database(ObjectName),
    Schema(ObjectName),
    Table(ObjectName),
    Function(RoutineSignatureSyntax),
    Procedure(RoutineSignatureSyntax),
}

impl fmt::Display for PrivilegeTargetSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(name) => write!(formatter, "DATABASE {name}"),
            Self::Schema(name) => write!(formatter, "SCHEMA {name}"),
            Self::Table(name) => write!(formatter, "TABLE {name}"),
            Self::Function(signature) => write!(formatter, "FUNCTION {signature}"),
            Self::Procedure(signature) => write!(formatter, "PROCEDURE {signature}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GrantSyntax {
    RoleMembership {
        role: Identifier,
        member: Identifier,
        admin_option: bool,
    },
    ObjectPrivileges {
        privileges: Vec<PrivilegeSyntax>,
        target: PrivilegeTargetSyntax,
        grantee: Identifier,
        grant_option: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct GrantStatement {
    pub token: Token,
    pub grant: GrantSyntax,
}

impl fmt::Display for GrantStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GRANT ")?;
        match &self.grant {
            GrantSyntax::RoleMembership {
                role,
                member,
                admin_option,
            } => {
                write!(formatter, "{role} TO {member}")?;
                if *admin_option {
                    formatter.write_str(" WITH ADMIN OPTION")?;
                }
                Ok(())
            }
            GrantSyntax::ObjectPrivileges {
                privileges,
                target,
                grantee,
                grant_option,
            } => {
                write_privileges(formatter, privileges)?;
                write!(formatter, " ON {target} TO {grantee}")?;
                if *grant_option {
                    formatter.write_str(" WITH GRANT OPTION")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RevokeSyntax {
    RoleMembership {
        role: Identifier,
        member: Identifier,
        admin_option_only: bool,
    },
    ObjectPrivileges {
        privileges: Vec<PrivilegeSyntax>,
        target: PrivilegeTargetSyntax,
        grantee: Identifier,
        grant_option_only: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStatement {
    pub token: Token,
    pub revoke: RevokeSyntax,
    pub behavior: DropBehaviorSyntax,
}

impl fmt::Display for RevokeStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("REVOKE ")?;
        match &self.revoke {
            RevokeSyntax::RoleMembership {
                role,
                member,
                admin_option_only,
            } => {
                if *admin_option_only {
                    formatter.write_str("ADMIN OPTION FOR ")?;
                }
                write!(formatter, "{role} FROM {member}")
            }
            RevokeSyntax::ObjectPrivileges {
                privileges,
                target,
                grantee,
                grant_option_only,
            } => {
                if *grant_option_only {
                    formatter.write_str("GRANT OPTION FOR ")?;
                }
                write_privileges(formatter, privileges)?;
                write!(formatter, " ON {target} FROM {grantee}")
            }
        }?;
        write!(formatter, " {}", self.behavior)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnershipTargetSyntax {
    Table(ObjectName),
    Function(RoutineSignatureSyntax),
    Procedure(RoutineSignatureSyntax),
}

impl fmt::Display for OwnershipTargetSyntax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(name) => write!(formatter, "TABLE {name}"),
            Self::Function(signature) => write!(formatter, "FUNCTION {signature}"),
            Self::Procedure(signature) => write!(formatter, "PROCEDURE {signature}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterOwnerStatement {
    pub token: Token,
    pub target: OwnershipTargetSyntax,
    pub owner: Identifier,
}

impl fmt::Display for AlterOwnerStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ALTER {} OWNER TO {}", self.target, self.owner)
    }
}

fn write_privileges(
    formatter: &mut fmt::Formatter<'_>,
    privileges: &[PrivilegeSyntax],
) -> fmt::Result {
    for (index, privilege) in privileges.iter().enumerate() {
        if index > 0 {
            formatter.write_str(", ")?;
        }
        write!(formatter, "{privilege}")?;
    }
    Ok(())
}
