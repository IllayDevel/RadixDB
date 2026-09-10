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

use super::*;

// ============================================================================
// Core Traits
// ============================================================================

/// Node trait - base for all AST nodes
pub trait Node: fmt::Display + fmt::Debug {
    /// Returns the literal string of the first token
    fn token_literal(&self) -> &str;
    /// Returns the position of the node in source code
    fn position(&self) -> Position;
}

// ============================================================================
// Expressions
// ============================================================================

/// Expression enum representing all expression types
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Identifier (column name, table name)
    Identifier(Identifier),
    /// Qualified identifier (table.column)
    QualifiedIdentifier(QualifiedIdentifier),
    /// Integer literal
    IntegerLiteral(IntegerLiteral),
    /// Float literal
    FloatLiteral(FloatLiteral),
    /// String literal
    StringLiteral(StringLiteral),
    /// Boolean literal (TRUE/FALSE)
    BooleanLiteral(BooleanLiteral),
    /// NULL literal
    NullLiteral(NullLiteral),
    /// INTERVAL literal
    IntervalLiteral(IntervalLiteral),
    /// Executor-bound typed value. Never produced by the SQL parser; this
    /// preserves exact subquery/runtime identity during internal rewriting.
    BoundValue(Box<Value>),
    /// Parameter ($1, ?)
    Parameter(Parameter),
    /// Prefix expression (-x, NOT x)
    Prefix(PrefixExpression),
    /// Infix expression (a + b, a = b)
    Infix(InfixExpression),
    /// List of expressions (for IN clause) - Boxed to reduce enum size
    List(Box<ListExpression>),
    /// DISTINCT expression
    Distinct(DistinctExpression),
    /// EXISTS subquery
    Exists(ExistsExpression),
    /// ALL/ANY/SOME subquery comparison (e.g., x > ALL (SELECT ...))
    AllAny(AllAnyExpression),
    /// IN expression
    In(InExpression),
    /// Pre-computed IN expression with HashSet (for semi-join optimization)
    /// Uses Arc for cheap cloning in parallel execution
    InHashSet(InHashSetExpression),
    /// BETWEEN expression
    Between(BetweenExpression),
    /// LIKE expression (with optional ESCAPE clause)
    Like(LikeExpression),
    /// Scalar subquery
    ScalarSubquery(ScalarSubquery),
    /// Expression list (for IN values) - Boxed to reduce enum size
    ExpressionList(Box<ExpressionList>),
    /// CASE expression - Boxed to reduce enum size
    Case(Box<CaseExpression>),
    /// CAST expression
    Cast(CastExpression),
    /// Function call - Boxed to reduce enum size (has 2 Vecs)
    FunctionCall(Box<FunctionCall>),
    /// Aliased expression (expr AS alias)
    Aliased(AliasedExpression),
    /// Window expression - Boxed to reduce enum size (has 2 Vecs)
    Window(Box<WindowExpression>),
    /// Simple table source - Boxed to reduce enum size
    TableSource(Box<SimpleTableSource>),
    /// Join table source
    JoinSource(Box<JoinTableSource>),
    /// Subquery table source - Boxed to reduce enum size (216 bytes unboxed)
    SubquerySource(Box<SubqueryTableSource>),
    /// VALUES table source - Boxed to reduce enum size (256 bytes unboxed)
    ValuesSource(Box<ValuesTableSource>),
    /// CTE reference - Boxed to reduce enum size (336 bytes unboxed)
    CteReference(Box<CteReference>),
    /// Function table source (table-valued function in FROM clause) - Boxed to reduce enum size
    FunctionTableSource(Box<FunctionTableSource>),
    /// Star (*) for SELECT *
    Star(StarExpression),
    /// Qualified star (table.*) for SELECT table.*
    QualifiedStar(QualifiedStarExpression),
    /// DEFAULT keyword (for INSERT VALUES)
    Default(DefaultExpression),
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Identifier(e) => write!(f, "{}", e),
            Expression::QualifiedIdentifier(e) => write!(f, "{}", e),
            Expression::IntegerLiteral(e) => write!(f, "{}", e),
            Expression::FloatLiteral(e) => write!(f, "{}", e),
            Expression::StringLiteral(e) => write!(f, "{}", e),
            Expression::BooleanLiteral(e) => write!(f, "{}", e),
            Expression::NullLiteral(e) => write!(f, "{}", e),
            Expression::IntervalLiteral(e) => write!(f, "{}", e),
            Expression::BoundValue(value) => write!(f, "<bound:{}>", value.data_type()),
            Expression::Parameter(e) => write!(f, "{}", e),
            Expression::Prefix(e) => write!(f, "{}", e),
            Expression::Infix(e) => write!(f, "{}", e),
            Expression::List(e) => write!(f, "{}", e),
            Expression::Distinct(e) => write!(f, "{}", e),
            Expression::Exists(e) => write!(f, "{}", e),
            Expression::AllAny(e) => write!(f, "{}", e),
            Expression::In(e) => write!(f, "{}", e),
            Expression::InHashSet(e) => write!(f, "{}", e),
            Expression::Between(e) => write!(f, "{}", e),
            Expression::Like(e) => write!(f, "{}", e),
            Expression::ScalarSubquery(e) => write!(f, "{}", e),
            Expression::ExpressionList(e) => write!(f, "{}", e),
            Expression::Case(e) => write!(f, "{}", e),
            Expression::Cast(e) => write!(f, "{}", e),
            Expression::FunctionCall(e) => write!(f, "{}", e),
            Expression::Aliased(e) => write!(f, "{}", e),
            Expression::Window(e) => write!(f, "{}", e),
            Expression::TableSource(e) => write!(f, "{}", e),
            Expression::JoinSource(e) => write!(f, "{}", e),
            Expression::SubquerySource(e) => write!(f, "{}", e),
            Expression::ValuesSource(e) => write!(f, "{}", e),
            Expression::CteReference(e) => write!(f, "{}", e),
            Expression::FunctionTableSource(e) => write!(f, "{}", e),
            Expression::Star(e) => write!(f, "{}", e),
            Expression::QualifiedStar(e) => write!(f, "{}", e),
            Expression::Default(e) => write!(f, "{}", e),
        }
    }
}

impl Expression {
    /// Get the position of this expression
    pub fn position(&self) -> Position {
        match self {
            Expression::Identifier(e) => e.token.position,
            Expression::QualifiedIdentifier(e) => e.token.position,
            Expression::IntegerLiteral(e) => e.token.position,
            Expression::FloatLiteral(e) => e.token.position,
            Expression::StringLiteral(e) => e.token.position,
            Expression::BooleanLiteral(e) => e.token.position,
            Expression::NullLiteral(e) => e.token.position,
            Expression::IntervalLiteral(e) => e.token.position,
            Expression::BoundValue(_) => Position::default(),
            Expression::Parameter(e) => e.token.position,
            Expression::Prefix(e) => e.token.position,
            Expression::Infix(e) => e.token.position,
            Expression::List(e) => e.token.position,
            Expression::Distinct(e) => e.token.position,
            Expression::Exists(e) => e.token.position,
            Expression::AllAny(e) => e.token.position,
            Expression::In(e) => e.token.position,
            Expression::InHashSet(e) => e.token.position,
            Expression::Between(e) => e.token.position,
            Expression::Like(e) => e.token.position,
            Expression::ScalarSubquery(e) => e.token.position,
            Expression::ExpressionList(e) => e.token.position,
            Expression::Case(e) => e.token.position,
            Expression::Cast(e) => e.token.position,
            Expression::FunctionCall(e) => e.token.position,
            Expression::Aliased(e) => e.token.position,
            Expression::Window(e) => e.token.position,
            Expression::TableSource(e) => e.token.position,
            Expression::JoinSource(e) => e.token.position,
            Expression::SubquerySource(e) => e.token.position,
            Expression::ValuesSource(e) => e.token.position,
            Expression::CteReference(e) => e.token.position,
            Expression::FunctionTableSource(e) => e.token.position,
            Expression::Star(e) => e.token.position,
            Expression::QualifiedStar(e) => e.token.position,
            Expression::Default(e) => e.token.position,
        }
    }
}

// ============================================================================
// Expression Types
// ============================================================================

/// Identifier (column name, table name, etc.)
#[derive(Debug, Clone, PartialEq)]
pub struct Identifier {
    pub token: Token,
    #[doc(hidden)]
    pub value: SmartString,
    /// Pre-computed lowercase value for fast case-insensitive lookups
    #[doc(hidden)]
    pub value_lower: SmartString,
}

impl Identifier {
    /// Create a new identifier with pre-computed lowercase value.
    /// Keywords are uppercased by the lexer for parsing; when used as identifiers
    /// (column names, aliases, table names), fold to lowercase like PostgreSQL.
    #[inline]
    pub fn new(token: Token, value: impl Into<SmartString>) -> Self {
        let value = value.into();
        if token.token_type == TokenType::Keyword {
            // Keywords are uppercased by the lexer; fold to lowercase like PostgreSQL.
            // value and value_lower are identical, so avoid double lowercasing.
            let lowered = value.to_lowercase();
            Self {
                token,
                value_lower: lowered.clone(),
                value: lowered,
            }
        } else {
            let value_lower = value.to_lowercase();
            Self {
                token,
                value,
                value_lower,
            }
        }
    }

    #[inline]
    pub fn value(&self) -> &str {
        &self.value
    }

    #[inline]
    pub fn value_lower(&self) -> &str {
        &self.value_lower
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.token.quoted {
            write!(f, "\"{}\"", self.value.replace('"', "\"\""))
        } else {
            write!(f, "{}", self.value)
        }
    }
}

/// Qualified identifier or an unresolved multi-part identifier path.
///
/// Two-component identifiers keep the historic `qualifier.name` shape. For
/// longer paths, `intermediate` owns every component between the root
/// qualifier and terminal name. The parser preserves each component as an
/// `Identifier`, including its source token and position; schema meaning is
/// deliberately assigned later by the binder.
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedIdentifier {
    pub token: Token,
    pub qualifier: Box<Identifier>,
    /// Allocated only for paths longer than `qualifier.name`, keeping the
    /// common two-component AST node compact.
    pub intermediate: Option<Box<Vec<Identifier>>>,
    pub name: Box<Identifier>,
}

impl QualifiedIdentifier {
    #[inline]
    pub fn component_count(&self) -> usize {
        self.intermediate.as_ref().map_or(0, |items| items.len()) + 2
    }

    #[inline]
    pub fn is_multi_part_path(&self) -> bool {
        self.intermediate
            .as_ref()
            .is_some_and(|items| !items.is_empty())
    }

    pub fn components(&self) -> impl Iterator<Item = &Identifier> {
        std::iter::once(self.qualifier.as_ref())
            .chain(
                self.intermediate
                    .as_deref()
                    .into_iter()
                    .flat_map(|items| items.iter()),
            )
            .chain(std::iter::once(self.name.as_ref()))
    }
}

impl fmt::Display for QualifiedIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.qualifier)?;
        if let Some(intermediate) = &self.intermediate {
            for component in intermediate.iter() {
                write!(f, ".{component}")?;
            }
        }
        write!(f, ".{}", self.name)
    }
}

/// Integer literal
#[derive(Debug, Clone, PartialEq)]
pub struct IntegerLiteral {
    pub token: Token,
    pub value: i64,
}

impl fmt::Display for IntegerLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// Float literal
#[derive(Debug, Clone, PartialEq)]
pub struct FloatLiteral {
    pub token: Token,
    pub value: f64,
}

impl fmt::Display for FloatLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered = self.value.to_string();
        if self.value.is_finite()
            && !rendered.contains('.')
            && !rendered.contains('e')
            && !rendered.contains('E')
        {
            write!(f, "{}.0", rendered)
        } else {
            write!(f, "{}", rendered)
        }
    }
}

/// String literal
#[derive(Debug, Clone, PartialEq)]
pub struct StringLiteral {
    pub token: Token,
    pub value: SmartString,
    /// Optional type hint (DATE, TIME, JSON, etc.)
    pub type_hint: Option<SmartString>,
}

impl fmt::Display for StringLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(type_hint) = &self.type_hint {
            write!(
                f,
                "{} '{}'",
                type_hint.to_uppercase(),
                self.value.replace('\'', "''")
            )
        } else {
            write!(f, "'{}'", self.value.replace('\'', "''"))
        }
    }
}

/// Boolean literal
#[derive(Debug, Clone, PartialEq)]
pub struct BooleanLiteral {
    pub token: Token,
    pub value: bool,
}

impl fmt::Display for BooleanLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", if self.value { "TRUE" } else { "FALSE" })
    }
}

/// NULL literal
#[derive(Debug, Clone, PartialEq)]
pub struct NullLiteral {
    pub token: Token,
}

impl fmt::Display for NullLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NULL")
    }
}

/// INTERVAL literal
#[derive(Debug, Clone, PartialEq)]
pub struct IntervalLiteral {
    pub token: Token,
    pub value: SmartString,
    pub quantity: i64,
    pub unit: SmartString,
}

impl fmt::Display for IntervalLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INTERVAL '{}'", self.value)
    }
}

/// Parameter ($1, ?)
#[derive(Debug, Clone, PartialEq)]
pub struct Parameter {
    pub token: Token,
    pub name: SmartString,
    pub index: usize,
    /// Optional record field selected from a named procedural parameter, for
    /// example `:NEW.id`. The SQL parser preserves it as a parameter leaf;
    /// only the stored-program binder resolves the record contract.
    pub field: Option<Box<Identifier>>,
}

impl fmt::Display for Parameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.name.is_empty() {
            write!(f, "?")
        } else if let Some(field) = &self.field {
            write!(f, "{}.{}", self.name, field)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Infix operator type (pre-computed at parse time for zero-allocation evaluation)
/// This is a key optimization: instead of string comparison for every row,
/// we match on a small enum which is faster and allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InfixOperator {
    // Comparison operators
    Equal,        // =
    NotEqual,     // <> or !=
    LessThan,     // <
    LessEqual,    // <=
    GreaterThan,  // >
    GreaterEqual, // >=

    // Logical operators
    And,
    Or,
    Xor,

    // Arithmetic operators
    Add,      // +
    Subtract, // -
    Multiply, // *
    Divide,   // /
    Modulo,   // % or MOD

    // String operators
    Concat, // ||

    // Pattern matching
    Like,
    ILike,
    NotLike,
    NotILike,
    Glob,
    NotGlob,
    Regexp,
    NotRegexp,

    // Null check
    Is,                // IS (NULL)
    IsNot,             // IS NOT (NULL)
    IsDistinctFrom,    // IS DISTINCT FROM (NULL-safe not equal)
    IsNotDistinctFrom, // IS NOT DISTINCT FROM (NULL-safe equal)

    // Array index
    Index, // []

    // JSON operators
    JsonAccess,     // -> (returns JSON)
    JsonAccessText, // ->> (returns TEXT)

    // Vector distance operator
    VectorDistance, // <=>

    // Bitwise operators
    BitwiseAnd, // &
    BitwiseOr,  // |
    BitwiseXor, // ^
    LeftShift,  // <<
    RightShift, // >>

    // Unknown/other (fallback for rare operators)
    Other,
}

impl InfixOperator {
    /// Parse operator string to enum (called once at parse time)
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "=" => InfixOperator::Equal,
            "<>" | "!=" => InfixOperator::NotEqual,
            "<" => InfixOperator::LessThan,
            "<=" => InfixOperator::LessEqual,
            ">" => InfixOperator::GreaterThan,
            ">=" => InfixOperator::GreaterEqual,
            "AND" => InfixOperator::And,
            "OR" => InfixOperator::Or,
            "XOR" => InfixOperator::Xor,
            "+" => InfixOperator::Add,
            "-" => InfixOperator::Subtract,
            "*" => InfixOperator::Multiply,
            "/" => InfixOperator::Divide,
            "%" | "MOD" => InfixOperator::Modulo,
            "||" => InfixOperator::Concat,
            "LIKE" => InfixOperator::Like,
            "ILIKE" => InfixOperator::ILike,
            "NOT LIKE" => InfixOperator::NotLike,
            "NOT ILIKE" => InfixOperator::NotILike,
            "GLOB" => InfixOperator::Glob,
            "NOT GLOB" => InfixOperator::NotGlob,
            "REGEXP" | "RLIKE" => InfixOperator::Regexp,
            "NOT REGEXP" | "NOT RLIKE" => InfixOperator::NotRegexp,
            "IS" => InfixOperator::Is,
            "IS NOT" => InfixOperator::IsNot,
            "IS DISTINCT FROM" => InfixOperator::IsDistinctFrom,
            "IS NOT DISTINCT FROM" => InfixOperator::IsNotDistinctFrom,
            "[]" => InfixOperator::Index,
            "->" => InfixOperator::JsonAccess,
            "->>" => InfixOperator::JsonAccessText,
            "<=>" => InfixOperator::VectorDistance,
            "&" => InfixOperator::BitwiseAnd,
            "|" => InfixOperator::BitwiseOr,
            "^" => InfixOperator::BitwiseXor,
            "<<" => InfixOperator::LeftShift,
            ">>" => InfixOperator::RightShift,
            _ => InfixOperator::Other,
        }
    }
}

/// Prefix operator type (pre-computed at parse time)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrefixOperator {
    Negate,     // -
    Not,        // NOT
    Plus,       // + (unary plus, no-op)
    BitwiseNot, // ~ (bitwise NOT)
    Other,
}

impl PrefixOperator {
    /// Parse operator string to enum (called once at parse time)
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "-" => PrefixOperator::Negate,
            "NOT" => PrefixOperator::Not,
            "+" => PrefixOperator::Plus,
            "~" => PrefixOperator::BitwiseNot,
            _ => PrefixOperator::Other,
        }
    }
}

/// Star expression (*)
#[derive(Debug, Clone, PartialEq)]
pub struct StarExpression {
    pub token: Token,
}

impl fmt::Display for StarExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "*")
    }
}

/// Qualified star expression (table.*)
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedStarExpression {
    pub token: Token,
    pub qualifier: SmartString,
}

impl fmt::Display for QualifiedStarExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.*", self.qualifier)
    }
}

/// DEFAULT keyword expression (for INSERT VALUES)
#[derive(Debug, Clone, PartialEq)]
pub struct DefaultExpression {
    pub token: Token,
}

impl fmt::Display for DefaultExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DEFAULT")
    }
}

/// Prefix expression (-x, NOT x)
#[derive(Debug, Clone, PartialEq)]
pub struct PrefixExpression {
    pub token: Token,
    #[doc(hidden)]
    pub operator: SmartString,
    /// Pre-computed operator type for fast evaluation (no string comparison)
    #[doc(hidden)]
    pub op_type: PrefixOperator,
    pub right: Box<Expression>,
}

impl PrefixExpression {
    /// Create a new prefix expression with auto-computed op_type
    #[inline]
    pub fn new(token: Token, operator: impl Into<SmartString>, right: Box<Expression>) -> Self {
        let operator = operator.into();
        let op_type = PrefixOperator::from_str(&operator);
        Self {
            token,
            operator,
            op_type,
            right,
        }
    }

    #[inline]
    pub fn operator(&self) -> &str {
        &self.operator
    }

    #[inline]
    pub fn op_type(&self) -> PrefixOperator {
        self.op_type
    }
}

impl fmt::Display for PrefixExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.operator == "-" || self.operator == "+" {
            write!(f, "({}{})", self.operator, self.right)
        } else {
            write!(f, "({} {})", self.operator, self.right)
        }
    }
}

/// Infix expression (a + b, a = b)
#[derive(Debug, Clone, PartialEq)]
pub struct InfixExpression {
    pub token: Token,
    pub left: Box<Expression>,
    #[doc(hidden)]
    pub operator: SmartString,
    /// Pre-computed operator type for fast evaluation (no string comparison)
    #[doc(hidden)]
    pub op_type: InfixOperator,
    pub right: Box<Expression>,
}

impl InfixExpression {
    /// Create a new infix expression with auto-computed op_type
    #[inline]
    pub fn new(
        token: Token,
        left: Box<Expression>,
        operator: impl Into<SmartString>,
        right: Box<Expression>,
    ) -> Self {
        let operator = operator.into();
        let op_type = InfixOperator::from_str(&operator);
        Self {
            token,
            left,
            operator,
            op_type,
            right,
        }
    }

    #[inline]
    pub fn operator(&self) -> &str {
        &self.operator
    }

    #[inline]
    pub fn op_type(&self) -> InfixOperator {
        self.op_type
    }
}

impl fmt::Display for InfixExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.operator, self.right)
    }
}

/// List expression (for IN clause values)
#[derive(Debug, Clone, PartialEq)]
pub struct ListExpression {
    pub token: Token,
    pub elements: Vec<Expression>,
}

impl fmt::Display for ListExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elements: Vec<String> = self.elements.iter().map(|e| e.to_string()).collect();
        write!(f, "({})", elements.join(", "))
    }
}

/// DISTINCT expression
#[derive(Debug, Clone, PartialEq)]
pub struct DistinctExpression {
    pub token: Token,
    pub expr: Box<Expression>,
}

impl fmt::Display for DistinctExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DISTINCT {}", self.expr)
    }
}

/// EXISTS expression
#[derive(Debug, Clone, PartialEq)]
pub struct ExistsExpression {
    pub token: Token,
    pub subquery: Box<SelectStatement>,
}

impl fmt::Display for ExistsExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EXISTS ({})", self.subquery)
    }
}

/// ALL/ANY comparison type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllAnyType {
    All,
    Any,
}

impl fmt::Display for AllAnyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AllAnyType::All => write!(f, "ALL"),
            AllAnyType::Any => write!(f, "ANY"),
        }
    }
}

/// ALL/ANY/SOME subquery expression (e.g., x > ALL (SELECT ...))
#[derive(Debug, Clone, PartialEq)]
pub struct AllAnyExpression {
    pub token: Token,
    pub left: Box<Expression>,
    pub operator: SmartString,
    pub all_any_type: AllAnyType,
    pub subquery: Box<SelectStatement>,
}

impl fmt::Display for AllAnyExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {} ({})",
            self.left, self.operator, self.all_any_type, self.subquery
        )
    }
}

/// IN expression
#[derive(Debug, Clone, PartialEq)]
pub struct InExpression {
    pub token: Token,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub not: bool,
}

impl fmt::Display for InExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.not {
            write!(f, "{} NOT IN {}", self.left, self.right)
        } else {
            write!(f, "{} IN {}", self.left, self.right)
        }
    }
}

/// Pre-computed IN expression with HashSet for O(1) lookup
///
/// This is used by the semi-join optimization to avoid rebuilding
/// the HashSet on every row during parallel filtering.
/// Arc enables cheap cloning when the expression is cloned for parallel execution.
#[derive(Debug, Clone)]
pub struct InHashSetExpression {
    pub token: Token,
    /// The column/expression to check
    pub column: Box<Expression>,
    /// Pre-computed ValueSet for O(1) lookup - Arc for cheap parallel cloning
    pub values: CompactArc<ValueSet>,
    /// Whether this is NOT IN
    pub not: bool,
}

impl PartialEq for InHashSetExpression {
    fn eq(&self, other: &Self) -> bool {
        // Compare by CompactArc pointer for efficiency (same HashSet = same CompactArc)
        self.not == other.not
            && CompactArc::ptr_eq(&self.values, &other.values)
            && self.column == other.column
    }
}

impl fmt::Display for InHashSetExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.not {
            write!(f, "{} NOT IN (<{} values>)", self.column, self.values.len())
        } else {
            write!(f, "{} IN (<{} values>)", self.column, self.values.len())
        }
    }
}

/// BETWEEN expression
#[derive(Debug, Clone, PartialEq)]
pub struct BetweenExpression {
    pub token: Token,
    pub expr: Box<Expression>,
    pub lower: Box<Expression>,
    pub upper: Box<Expression>,
    pub not: bool,
}

impl fmt::Display for BetweenExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.not {
            write!(
                f,
                "{} NOT BETWEEN {} AND {}",
                self.expr, self.lower, self.upper
            )
        } else {
            write!(f, "{} BETWEEN {} AND {}", self.expr, self.lower, self.upper)
        }
    }
}

/// LIKE expression with optional ESCAPE clause
#[derive(Debug, Clone, PartialEq)]
pub struct LikeExpression {
    pub token: Token,
    pub left: Box<Expression>,
    pub pattern: Box<Expression>,
    /// The operator: LIKE, ILIKE, NOT LIKE, NOT ILIKE, GLOB, NOT GLOB, REGEXP, RLIKE, NOT REGEXP, NOT RLIKE
    pub operator: SmartString,
    /// Optional escape character
    pub escape: Option<Box<Expression>>,
}

impl fmt::Display for LikeExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.pattern)?;
        if let Some(ref escape) = self.escape {
            write!(f, " ESCAPE {}", escape)?;
        }
        Ok(())
    }
}

/// Scalar subquery
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarSubquery {
    pub token: Token,
    pub subquery: Box<SelectStatement>,
}

impl fmt::Display for ScalarSubquery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.subquery)
    }
}

/// Expression list (for IN values)
#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionList {
    pub token: Token,
    pub expressions: Vec<Expression>,
}

impl fmt::Display for ExpressionList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exprs: Vec<String> = self.expressions.iter().map(|e| e.to_string()).collect();
        write!(f, "({})", exprs.join(", "))
    }
}

/// CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpression {
    pub token: Token,
    pub value: Option<Box<Expression>>,
    pub when_clauses: Vec<WhenClause>,
    pub else_value: Option<Box<Expression>>,
}

impl fmt::Display for CaseExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("CASE");
        if let Some(ref val) = self.value {
            result.push_str(&format!(" {}", val));
        }
        for when in &self.when_clauses {
            result.push_str(&format!(" {}", when));
        }
        if let Some(ref else_val) = self.else_value {
            result.push_str(&format!(" ELSE {}", else_val));
        }
        result.push_str(" END");
        write!(f, "{}", result)
    }
}

/// WHEN clause in CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub token: Token,
    pub condition: Expression,
    pub then_result: Expression,
}

impl fmt::Display for WhenClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {}", self.condition, self.then_result)
    }
}

/// CAST expression
#[derive(Debug, Clone, PartialEq)]
pub struct CastExpression {
    pub token: Token,
    pub expr: Box<Expression>,
    pub type_name: SmartString,
}

impl fmt::Display for CastExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.type_name)
    }
}

/// Function call
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub token: Token,
    pub function: SmartString,
    pub arguments: Vec<Expression>,
    pub is_distinct: bool,
    pub order_by: Vec<OrderByExpression>,
    /// FILTER clause for aggregate functions (e.g., COUNT(*) FILTER (WHERE condition))
    pub filter: Option<Box<Expression>>,
}

impl fmt::Display for FunctionCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut args = String::new();
        if self.is_distinct && !self.arguments.is_empty() {
            args.push_str("DISTINCT ");
            args.push_str(&self.arguments[0].to_string());
            for arg in &self.arguments[1..] {
                args.push_str(", ");
                args.push_str(&arg.to_string());
            }
        } else {
            let arg_strs: Vec<String> = self
                .arguments
                .iter()
                .map(|a| {
                    if matches!(a, Expression::Star(_)) {
                        "*".to_string()
                    } else {
                        a.to_string()
                    }
                })
                .collect();
            args = arg_strs.join(", ");
        }
        if !self.order_by.is_empty() {
            args.push_str(" ORDER BY ");
            let order_strs: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
            args.push_str(&order_strs.join(", "));
        }
        write!(f, "{}({})", self.function, args)?;
        if let Some(filter) = &self.filter {
            write!(f, " FILTER (WHERE {})", filter)?;
        }
        Ok(())
    }
}

/// Aliased expression (expr AS alias)
#[derive(Debug, Clone, PartialEq)]
pub struct AliasedExpression {
    pub token: Token,
    pub expression: Box<Expression>,
    pub alias: Identifier,
}

impl fmt::Display for AliasedExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.expression, self.alias)
    }
}

/// Window expression
#[derive(Debug, Clone, PartialEq)]
pub struct WindowExpression {
    pub token: Token,
    pub function: Box<FunctionCall>,
    /// Named window reference (e.g., OVER w)
    pub window_ref: Option<SmartString>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByExpression>,
    pub frame: Option<WindowFrame>,
}

impl fmt::Display for WindowExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = self.function.to_string();
        if let Some(ref win_ref) = self.window_ref {
            result.push_str(" OVER ");
            result.push_str(win_ref);
        } else {
            result.push_str(" OVER (");
            if !self.partition_by.is_empty() {
                result.push_str("PARTITION BY ");
                let parts: Vec<String> = self.partition_by.iter().map(|e| e.to_string()).collect();
                result.push_str(&parts.join(", "));
            }
            if !self.order_by.is_empty() {
                if !self.partition_by.is_empty() {
                    result.push(' ');
                }
                result.push_str("ORDER BY ");
                let orders: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
                result.push_str(&orders.join(", "));
            }
            if let Some(ref frame) = self.frame {
                result.push(' ');
                result.push_str(&frame.to_string());
            }
            result.push(')');
        }
        write!(f, "{}", result)
    }
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub unit: WindowFrameUnit,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

impl fmt::Display for WindowFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let unit = match self.unit {
            WindowFrameUnit::Rows => "ROWS",
            WindowFrameUnit::Range => "RANGE",
        };
        if let Some(ref end) = self.end {
            write!(f, "{} BETWEEN {} AND {}", unit, self.start, end)
        } else {
            write!(f, "{} {}", unit, self.start)
        }
    }
}

/// Window frame unit
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnit {
    Rows,
    Range,
}

/// Window frame bound
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(Box<Expression>),
    Following(Box<Expression>),
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => write!(f, "CURRENT ROW"),
            WindowFrameBound::UnboundedPreceding => write!(f, "UNBOUNDED PRECEDING"),
            WindowFrameBound::UnboundedFollowing => write!(f, "UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(e) => write!(f, "{} PRECEDING", e),
            WindowFrameBound::Following(e) => write!(f, "{} FOLLOWING", e),
        }
    }
}

/// Named window definition (WINDOW w AS (...))
#[derive(Debug, Clone, PartialEq)]
pub struct WindowDefinition {
    pub name: SmartString,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByExpression>,
    pub frame: Option<WindowFrame>,
}

impl fmt::Display for WindowDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!("{} AS (", self.name);
        if !self.partition_by.is_empty() {
            result.push_str("PARTITION BY ");
            let parts: Vec<String> = self.partition_by.iter().map(|e| e.to_string()).collect();
            result.push_str(&parts.join(", "));
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                result.push(' ');
            }
            result.push_str("ORDER BY ");
            let orders: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
            result.push_str(&orders.join(", "));
        }
        if let Some(ref frame) = self.frame {
            result.push(' ');
            result.push_str(&frame.to_string());
        }
        result.push(')');
        write!(f, "{}", result)
    }
}
