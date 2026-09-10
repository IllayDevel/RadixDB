use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DerivedRates {
    pub interval_millis: u64,
    pub per_second: BTreeMap<String, f64>,
    pub resets: Vec<String>,
}

#[derive(Default)]
pub struct RateDeriver {
    previous_monotonic_millis: Option<u64>,
    previous: BTreeMap<String, i64>,
}

impl RateDeriver {
    pub fn observe(
        &mut self,
        monotonic_millis: u64,
        counters: &BTreeMap<String, i64>,
    ) -> Result<DerivedRates, String> {
        let Some(previous_millis) = self.previous_monotonic_millis else {
            self.previous_monotonic_millis = Some(monotonic_millis);
            self.previous = counters.clone();
            return Ok(DerivedRates::default());
        };
        if monotonic_millis <= previous_millis {
            return Err("rate samples must have increasing monotonic timestamps".into());
        }
        let interval_millis = monotonic_millis - previous_millis;
        let seconds = interval_millis as f64 / 1_000.0;
        let mut derived = DerivedRates {
            interval_millis,
            ..DerivedRates::default()
        };
        for (name, current) in counters {
            let Some(previous) = self.previous.get(name) else {
                continue;
            };
            if current < previous {
                derived.resets.push(name.clone());
                continue;
            }
            derived
                .per_second
                .insert(name.clone(), (*current - *previous) as f64 / seconds);
        }
        self.previous_monotonic_millis = Some(monotonic_millis);
        self.previous = counters.clone();
        Ok(derived)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_rates_and_marks_counter_reset_without_underflow() {
        let mut deriver = RateDeriver::default();
        let first = BTreeMap::from([("reads".into(), 100), ("writes".into(), 50)]);
        assert_eq!(
            deriver.observe(1_000, &first).unwrap(),
            DerivedRates::default()
        );

        let second = BTreeMap::from([("reads".into(), 120), ("writes".into(), 2)]);
        let rates = deriver.observe(2_000, &second).unwrap();

        assert_eq!(rates.per_second.get("reads"), Some(&20.0));
        assert!(!rates.per_second.contains_key("writes"));
        assert_eq!(rates.resets, ["writes"]);
    }
}
