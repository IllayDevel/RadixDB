//! Bounded preparation of independent accelerator outputs.

#[cfg(not(feature = "test-hooks"))]
use super::super::FormatError;
use super::super::FormatResult;
#[cfg(not(feature = "test-hooks"))]
use super::model::invalid_index;
use super::model::{FanoutBuildLimits, MAX_ACCELERATOR_PREPARATION_WORKERS};
use super::runs::{IndexRunBuilder, PreparedIndexRuns};

pub(super) fn prepare_accelerators(
    builders: Vec<IndexRunBuilder>,
    source_row_count: u64,
    limits: FanoutBuildLimits,
) -> FormatResult<Vec<PreparedIndexRuns>> {
    let worker_count = preparation_worker_count(builders.len(), limits);
    if worker_count <= 1 {
        return prepare_serial(builders, source_row_count);
    }

    // Publication diagnostics are deliberately thread-local. Instrumented
    // builds stay serial so the structural gate observes one complete event
    // stream; ordinary builds exercise the bounded parallel path below.
    #[cfg(feature = "test-hooks")]
    {
        prepare_serial(builders, source_row_count)
    }

    #[cfg(not(feature = "test-hooks"))]
    {
        prepare_parallel(builders, source_row_count, worker_count)
    }
}

fn prepare_serial(
    builders: Vec<IndexRunBuilder>,
    source_row_count: u64,
) -> FormatResult<Vec<PreparedIndexRuns>> {
    builders
        .into_iter()
        .map(|builder| builder.prepare(source_row_count))
        .collect()
}

#[cfg(not(feature = "test-hooks"))]
fn prepare_parallel(
    builders: Vec<IndexRunBuilder>,
    source_row_count: u64,
    worker_count: usize,
) -> FormatResult<Vec<PreparedIndexRuns>> {
    let job_count = builders.len();
    let mut lanes = (0..worker_count)
        .map(|_| Vec::new())
        .collect::<Vec<Vec<(usize, IndexRunBuilder)>>>();
    for (ordinal, builder) in builders.into_iter().enumerate() {
        lanes[ordinal % worker_count].push((ordinal, builder));
    }

    let prepared = std::thread::scope(|scope| {
        let handles = lanes
            .into_iter()
            .map(|lane| {
                scope.spawn(move || {
                    lane.into_iter()
                        .map(|(ordinal, builder)| (ordinal, builder.prepare(source_row_count)))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();

        let mut prepared = Vec::with_capacity(job_count);
        for handle in handles {
            prepared.extend(
                handle
                    .join()
                    .map_err(|_| invalid_index("accelerator preparation worker panicked"))?,
            );
        }
        Ok::<_, FormatError>(prepared)
    })?;

    let mut ordered = (0..job_count)
        .map(|_| None)
        .collect::<Vec<Option<PreparedIndexRuns>>>();
    for (ordinal, result) in prepared {
        ordered[ordinal] = Some(result?);
    }
    ordered
        .into_iter()
        .map(|prepared| {
            prepared.ok_or_else(|| invalid_index("accelerator preparation result is missing"))
        })
        .collect()
}

fn preparation_worker_count(job_count: usize, limits: FanoutBuildLimits) -> usize {
    if job_count <= 1 {
        return job_count;
    }
    let configured = limits.accelerator_preparation_workers();
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1);
    let requested = if configured == 0 {
        available
    } else {
        configured as usize
    };
    job_count
        .min(available)
        .min(requested)
        .min(MAX_ACCELERATOR_PREPARATION_WORKERS as usize)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_count_is_bounded_by_jobs_host_and_configuration() {
        let available = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .max(1);
        assert_eq!(preparation_worker_count(0, FanoutBuildLimits::default()), 0);
        assert_eq!(preparation_worker_count(1, FanoutBuildLimits::default()), 1);
        assert_eq!(
            preparation_worker_count(usize::MAX, FanoutBuildLimits::default()),
            available.min(MAX_ACCELERATOR_PREPARATION_WORKERS as usize)
        );
        assert_eq!(
            preparation_worker_count(
                usize::MAX,
                FanoutBuildLimits::default()
                    .with_accelerator_preparation_workers(1)
                    .unwrap(),
            ),
            1
        );
    }
}
