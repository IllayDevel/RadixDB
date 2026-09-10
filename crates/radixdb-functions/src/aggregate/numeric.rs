// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0

//! Shared checked numeric state for dynamic and compiled SUM/AVG.

use radixdb_core::{Error, Result, Value};

#[derive(Debug, Clone, Default)]
pub struct NumericAccumulator {
    state: NumericState,
    count: u64,
    error: Option<String>,
}

#[derive(Debug, Clone, Default)]
enum NumericState {
    #[default]
    Empty,
    Exact {
        unscaled: i128,
        scale: u8,
        decimal_input: bool,
    },
    Float(f64),
}

impl NumericAccumulator {
    #[doc(hidden)]
    pub fn accumulate(&mut self, value: &Value) {
        if self.error.is_some() || value.is_null() {
            return;
        }

        let result = match value {
            Value::Integer(integer) => self.add_exact(i128::from(*integer), 0, false),
            Value::Float(float) => self.add_float(*float),
            value => match value.as_decimal_parts() {
                Some((unscaled, _, scale)) => self.add_exact(unscaled, scale, true),
                None => return,
            },
        };

        match result {
            Ok(()) => {
                self.count = match self.count.checked_add(1) {
                    Some(count) => count,
                    None => {
                        self.error = Some("numeric aggregate row count overflow".to_string());
                        self.count
                    }
                };
            }
            Err(error) => self.error = Some(error),
        }
    }

    fn add_exact(
        &mut self,
        value: i128,
        scale: u8,
        decimal_input: bool,
    ) -> std::result::Result<(), String> {
        match self.state {
            NumericState::Empty => {
                self.state = NumericState::Exact {
                    unscaled: value,
                    scale,
                    decimal_input,
                };
                Ok(())
            }
            NumericState::Float(ref mut sum) => {
                *sum += exact_to_f64(value, scale);
                Ok(())
            }
            NumericState::Exact {
                ref mut unscaled,
                scale: ref mut current_scale,
                decimal_input: ref mut had_decimal,
            } => {
                let target_scale = (*current_scale).max(scale);
                let left = scale_exact(*unscaled, *current_scale, target_scale)?;
                let right = scale_exact(value, scale, target_scale)?;
                *unscaled = left.checked_add(right).ok_or_else(|| {
                    "SUM exact accumulator overflowed the supported DECIMAL range".to_string()
                })?;
                *current_scale = target_scale;
                *had_decimal |= decimal_input;
                Ok(())
            }
        }
    }

    fn add_float(&mut self, value: f64) -> std::result::Result<(), String> {
        match self.state {
            NumericState::Empty => self.state = NumericState::Float(value),
            NumericState::Float(ref mut sum) => *sum += value,
            NumericState::Exact {
                unscaled, scale, ..
            } => self.state = NumericState::Float(exact_to_f64(unscaled, scale) + value),
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn sum_result(&self) -> Result<Value> {
        self.check_error()?;
        match self.state {
            NumericState::Empty => Ok(Value::null_unknown()),
            NumericState::Float(sum) => Ok(Value::Float(sum)),
            NumericState::Exact {
                unscaled,
                scale,
                decimal_input,
            } if scale == 0 && !decimal_input && i64::try_from(unscaled).is_ok() => {
                Ok(Value::Integer(unscaled as i64))
            }
            NumericState::Exact {
                unscaled, scale, ..
            } => decimal_value(unscaled, scale),
        }
    }

    #[doc(hidden)]
    pub fn average_result(&self) -> Result<Value> {
        self.check_error()?;
        if self.count == 0 {
            return Ok(Value::null_unknown());
        }
        let sum = match self.state {
            NumericState::Empty => return Ok(Value::null_unknown()),
            NumericState::Float(sum) => sum,
            NumericState::Exact {
                unscaled, scale, ..
            } => exact_to_f64(unscaled, scale),
        };
        Ok(Value::Float(sum / self.count as f64))
    }

    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    fn check_error(&self) -> Result<()> {
        self.error.as_ref().map_or(Ok(()), |message| {
            Err(Error::invalid_argument(message.clone()))
        })
    }
}

fn scale_exact(value: i128, from: u8, to: u8) -> std::result::Result<i128, String> {
    let factor = 10_i128
        .checked_pow(u32::from(to - from))
        .ok_or_else(|| "SUM decimal scale exceeds the supported exact range".to_string())?;
    value.checked_mul(factor).ok_or_else(|| {
        "SUM decimal scale conversion overflowed the supported exact range".to_string()
    })
}

fn exact_to_f64(unscaled: i128, scale: u8) -> f64 {
    unscaled as f64 / 10_f64.powi(i32::from(scale))
}

fn decimal_value(unscaled: i128, scale: u8) -> Result<Value> {
    let digits = unscaled
        .to_string()
        .trim_start_matches('-')
        .len()
        .max(usize::from(scale))
        .max(1);
    let precision = u8::try_from(digits)
        .ok()
        .filter(|precision| *precision <= 38)
        .ok_or_else(|| Error::invalid_argument("SUM result exceeds DECIMAL(38) precision"))?;
    Value::try_decimal(unscaled, precision, scale)
}
