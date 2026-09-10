use radixdb_core::{DataType, Error, Result, Value};

/// Function type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionType {
    Aggregate,
    Scalar,
    Window,
    TableValued,
}

/// Data type used by function signatures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionDataType {
    Any,
    Integer,
    Float,
    String,
    Boolean,
    Timestamp,
    Date,
    Time,
    DateTime,
    Json,
    Vector,
    Unknown,
}

/// Rule used to derive a polymorphic return type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionReturnRule {
    Declared,
    AllArguments,
    Arguments(Vec<usize>),
}

/// Fully resolved ORDER BY contract for aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateOrderBySpec {
    pub ascending: bool,
    pub nulls_first: bool,
}

impl AggregateOrderBySpec {
    #[inline]
    pub fn new(ascending: bool, nulls_first: Option<bool>) -> Self {
        Self {
            ascending,
            nulls_first: nulls_first.unwrap_or(!ascending),
        }
    }
}

/// Function signature information.
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    pub return_type: FunctionDataType,
    pub argument_types: Vec<FunctionDataType>,
    pub min_args: usize,
    pub max_args: usize,
    pub is_variadic: bool,
    pub return_rule: FunctionReturnRule,
}

impl FunctionSignature {
    pub fn new(
        return_type: FunctionDataType,
        argument_types: Vec<FunctionDataType>,
        min_args: usize,
        max_args: usize,
    ) -> Self {
        Self {
            return_type,
            argument_types,
            min_args,
            max_args,
            is_variadic: false,
            return_rule: FunctionReturnRule::Declared,
        }
    }

    pub fn variadic(return_type: FunctionDataType, arg_type: FunctionDataType) -> Self {
        Self {
            return_type,
            argument_types: vec![arg_type],
            min_args: 1,
            max_args: usize::MAX,
            is_variadic: true,
            return_rule: FunctionReturnRule::Declared,
        }
    }

    pub fn returns_all_arguments(mut self) -> Self {
        self.return_rule = FunctionReturnRule::AllArguments;
        self
    }

    pub fn returns_arguments(mut self, arguments: &[usize]) -> Self {
        self.return_rule = FunctionReturnRule::Arguments(arguments.to_vec());
        self
    }

    pub fn validate_arg_count(&self, count: usize) -> Result<()> {
        if count < self.min_args {
            return Err(Error::invalid_argument(format!(
                "expected at least {} arguments, got {}",
                self.min_args, count
            )));
        }
        if count > self.max_args {
            return Err(Error::invalid_argument(format!(
                "expected at most {} arguments, got {}",
                self.max_args, count
            )));
        }
        Ok(())
    }

    pub fn validate_values(&self, values: &[Value]) -> Result<()> {
        self.validate_arg_count(values.len())?;
        for (index, value) in values.iter().enumerate() {
            let expected = if self.is_variadic {
                self.argument_types.first()
            } else {
                self.argument_types.get(index)
            };
            let Some(expected) = expected else {
                continue;
            };
            if value.is_null() || expected.accepts_value(value) {
                continue;
            }
            return Err(Error::invalid_argument(format!(
                "argument {} expected {:?}, got {}",
                index + 1,
                expected,
                value.data_type()
            )));
        }
        Ok(())
    }
}

impl FunctionDataType {
    fn accepts_value(self, value: &Value) -> bool {
        match self {
            Self::Any | Self::Unknown => true,
            Self::Integer => matches!(value, Value::Integer(_)),
            Self::Float => matches!(value, Value::Integer(_) | Value::Float(_)),
            Self::String => matches!(value, Value::Text(_)),
            Self::Boolean => matches!(value, Value::Boolean(_)),
            Self::Timestamp | Self::DateTime | Self::Time => {
                matches!(value, Value::Timestamp(_))
            }
            Self::Date => value.data_type() == DataType::Date,
            Self::Json => value.data_type() == DataType::Json,
            Self::Vector => value.data_type() == DataType::Vector,
        }
    }
}

/// Function metadata published by the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub function_type: FunctionType,
    pub description: String,
    pub signature: FunctionSignature,
    pub deterministic: bool,
    pub volatility: FunctionVolatility,
}

impl FunctionInfo {
    pub fn new(
        name: impl Into<String>,
        function_type: FunctionType,
        description: impl Into<String>,
        signature: FunctionSignature,
    ) -> Self {
        Self {
            name: name.into(),
            function_type,
            description: description.into(),
            signature,
            deterministic: true,
            volatility: FunctionVolatility::Immutable,
        }
    }

    pub fn stable(mut self) -> Self {
        self.deterministic = false;
        self.volatility = FunctionVolatility::Stable;
        self
    }

    pub fn non_deterministic(mut self) -> Self {
        self.deterministic = false;
        self.volatility = FunctionVolatility::Volatile;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn function_type(&self) -> FunctionType {
        self.function_type
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn signature(&self) -> &FunctionSignature {
        &self.signature
    }
}
