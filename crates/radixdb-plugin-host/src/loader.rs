use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CStr,
    fs::{self, File},
    io::{BufReader, Read},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    slice, str,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use libloading::Library;
use radixdb_plugin_abi::*;
use semver::Version;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    manifest::{
        decode_lower_hex, parse_canonical_uuid, parse_glibc_version, MAX_LIBRARY_BYTES,
        MAX_MANIFEST_BYTES, PLUGIN_MANIFEST_FILE,
    },
    ObjectId, PluginHostConfig, PluginPackageManifest, PluginRegistry, RegisteredBinding,
    RegisteredExternalType, RegisteredFunction, RegisteredOperator, RegisteredOperatorClass,
    RegisteredPackage, RegisteredPlannerSupport, RegisteredTypeRef,
};

static NEXT_REGISTRY_GENERATION: AtomicU64 = AtomicU64::new(1);
static STARTUP_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static STARTUP_FAILURES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginLoaderMetricsSnapshot {
    pub startup_attempts: u64,
    pub startup_failures: u64,
}

pub fn plugin_loader_metrics() -> PluginLoaderMetricsSnapshot {
    PluginLoaderMetricsSnapshot {
        startup_attempts: STARTUP_ATTEMPTS.load(Ordering::Relaxed),
        startup_failures: STARTUP_FAILURES.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Error)]
pub enum PluginHostError {
    #[error("plugin host configuration: {0}")]
    Configuration(String),
    #[error("plugin package {path}: {reason}")]
    Package { path: PathBuf, reason: String },
    #[error("plugin registry: {0}")]
    Registry(String),
    #[error("plugin registry generation space exhausted")]
    GenerationExhausted,
}

struct Candidate {
    directory: PathBuf,
    library: PathBuf,
    library_bytes: u64,
    package_id: ObjectId,
    descriptor_fingerprint: [u8; 32],
    manifest: PluginPackageManifest,
}

struct LoadedObjects {
    package: RegisteredPackage,
    capabilities: u64,
    external_types: Vec<RegisteredExternalType>,
    functions: Vec<RegisteredFunction>,
    operators: Vec<RegisteredOperator>,
    operator_classes: Vec<RegisteredOperatorClass>,
    planner_support: Vec<RegisteredPlannerSupport>,
}

/// Discover, validate, load, and atomically construct one immutable registry.
/// No registry is returned if any configured package or descriptor is invalid.
pub fn load_plugin_registry(
    config: &PluginHostConfig,
) -> Result<Arc<PluginRegistry>, PluginHostError> {
    STARTUP_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    let result = load_plugin_registry_inner(config);
    if result.is_err() {
        STARTUP_FAILURES.fetch_add(1, Ordering::Relaxed);
    }
    result
}

fn load_plugin_registry_inner(
    config: &PluginHostConfig,
) -> Result<Arc<PluginRegistry>, PluginHostError> {
    if config.package_directories.is_empty() {
        return Ok(Arc::new(PluginRegistry::empty()));
    }
    let mut candidates = Vec::with_capacity(config.package_directories.len());
    let mut configured_paths = BTreeSet::new();
    for directory in &config.package_directories {
        let normalized = validate_allowlisted_directory(directory)?;
        if !configured_paths.insert(normalized.clone()) {
            return Err(PluginHostError::Configuration(format!(
                "duplicate package directory {}",
                normalized.display()
            )));
        }
        candidates.push(read_candidate(normalized)?);
    }

    let (active, shadowed_versions) = select_active_versions(candidates)?;
    let mut loaded = Vec::with_capacity(active.len());
    let mut loaded_library_bytes = 0_u64;
    for candidate in active {
        loaded_library_bytes = loaded_library_bytes
            .checked_add(candidate.library_bytes)
            .ok_or_else(|| PluginHostError::Registry("library byte total overflow".into()))?;
        loaded.push(load_candidate(candidate)?);
    }
    validate_loaded_graph(&loaded)?;

    let generation = NEXT_REGISTRY_GENERATION
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| PluginHostError::GenerationExhausted)?;
    Ok(Arc::new(build_registry(
        generation,
        loaded,
        shadowed_versions,
        loaded_library_bytes,
    )))
}

fn validate_allowlisted_directory(path: &Path) -> Result<PathBuf, PluginHostError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(PluginHostError::Configuration(format!(
            "package path must be normalized and absolute: {}",
            path.display()
        )));
    }
    validate_secure_path(path, PathKind::Directory).map_err(|reason| {
        PluginHostError::Configuration(format!("{}: {reason}", path.display()))
    })?;
    let canonical = fs::canonicalize(path)
        .map_err(|error| PluginHostError::Configuration(format!("{}: {error}", path.display())))?;
    if canonical != path {
        return Err(PluginHostError::Configuration(format!(
            "package path must not traverse aliases or symlinks: {}",
            path.display()
        )));
    }
    Ok(canonical)
}

fn read_candidate(directory: PathBuf) -> Result<Candidate, PluginHostError> {
    let manifest_path = directory.join(PLUGIN_MANIFEST_FILE);
    validate_secure_path(&manifest_path, PathKind::File)
        .map_err(|reason| package_error(&directory, reason))?;
    let manifest_metadata = fs::metadata(&manifest_path)
        .map_err(|error| package_error(&directory, format!("manifest metadata: {error}")))?;
    if manifest_metadata.len() > MAX_MANIFEST_BYTES {
        return Err(package_error(
            &directory,
            format!("manifest exceeds {MAX_MANIFEST_BYTES} bytes"),
        ));
    }
    let mut source = String::new();
    BufReader::new(
        File::open(&manifest_path)
            .map_err(|error| package_error(&directory, format!("open manifest: {error}")))?,
    )
    .take(MAX_MANIFEST_BYTES + 1)
    .read_to_string(&mut source)
    .map_err(|error| package_error(&directory, format!("read manifest: {error}")))?;
    let manifest: PluginPackageManifest = toml::from_str(&source)
        .map_err(|error| package_error(&directory, format!("parse manifest: {error}")))?;
    manifest
        .validate_static_fields()
        .map_err(|reason| package_error(&directory, reason))?;
    validate_host_glibc(&manifest.maximum_required_glibc)?;
    let raw_manifest: toml::Value = toml::from_str(&source)
        .map_err(|error| package_error(&directory, format!("parse manifest: {error}")))?;
    let raw_version = raw_manifest
        .get("version")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| package_error(&directory, "version must be a string"))?;
    if raw_version != manifest.version.to_string() {
        return Err(package_error(
            &directory,
            "version must use canonical SemVer spelling",
        ));
    }
    let package_id = parse_canonical_uuid(&manifest.package_id)
        .map_err(|reason| package_error(&directory, reason))?;
    let descriptor_fingerprint =
        decode_lower_hex::<32>(&manifest.descriptor_fingerprint, "descriptor_fingerprint")
            .map_err(|reason| package_error(&directory, reason))?;
    let expected_library_hash = decode_lower_hex::<32>(&manifest.library_sha256, "library_sha256")
        .map_err(|reason| package_error(&directory, reason))?;

    let library_directory = directory.join("lib");
    validate_secure_path(&library_directory, PathKind::Directory)
        .map_err(|reason| package_error(&directory, format!("library directory: {reason}")))?;
    let library = directory.join(&manifest.library);
    validate_secure_path(&library, PathKind::File)
        .map_err(|reason| package_error(&directory, format!("library: {reason}")))?;
    let canonical_library = fs::canonicalize(&library)
        .map_err(|error| package_error(&directory, format!("canonicalize library: {error}")))?;
    if canonical_library != library {
        return Err(package_error(
            &directory,
            "library path must not traverse aliases or symlinks",
        ));
    }
    let library_bytes = fs::metadata(&library)
        .map_err(|error| package_error(&directory, format!("library metadata: {error}")))?
        .len();
    if library_bytes == 0 || library_bytes > MAX_LIBRARY_BYTES {
        return Err(package_error(
            &directory,
            format!("library size must be 1..={MAX_LIBRARY_BYTES} bytes"),
        ));
    }
    validate_elf(&library).map_err(|reason| package_error(&directory, reason))?;
    let actual_library_hash =
        sha256_file(&library).map_err(|reason| package_error(&directory, reason))?;
    if actual_library_hash != expected_library_hash {
        return Err(package_error(&directory, "library SHA-256 mismatch"));
    }

    Ok(Candidate {
        directory,
        library,
        library_bytes,
        package_id,
        descriptor_fingerprint,
        manifest,
    })
}

#[derive(Clone, Copy)]
enum PathKind {
    File,
    Directory,
}

fn validate_secure_path(path: &Path, kind: PathKind) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Err("symbolic links are forbidden".into());
    }
    match kind {
        PathKind::File if !metadata.is_file() => return Err("not a regular file".into()),
        PathKind::Directory if !metadata.is_dir() => return Err("not a directory".into()),
        _ => {}
    }
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != 0 && metadata.uid() != effective_uid {
        return Err(format!(
            "owner uid {} is neither root nor server uid {effective_uid}",
            metadata.uid()
        ));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o022 != 0 {
        return Err(format!("permissions {mode:04o} allow group/world writes"));
    }
    Ok(())
}

fn package_error(directory: &Path, reason: impl Into<String>) -> PluginHostError {
    PluginHostError::Package {
        path: directory.to_path_buf(),
        reason: reason.into(),
    }
}

fn validate_elf(path: &Path) -> Result<(), String> {
    let mut header = [0_u8; 20];
    File::open(path)
        .map_err(|error| format!("open library: {error}"))?
        .read_exact(&mut header)
        .map_err(|error| format!("read ELF header: {error}"))?;
    if &header[..4] != b"\x7fELF"
        || header[4] != 2
        || header[5] != 1
        || u16::from_le_bytes([header[16], header[17]]) != 3
        || u16::from_le_bytes([header[18], header[19]]) != 62
    {
        return Err("library is not an x86_64 little-endian ELF shared object".into());
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<[u8; 32], String> {
    let file = File::open(path).map_err(|error| format!("open library for hashing: {error}"))?;
    let mut reader = BufReader::new(file);
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| format!("hash library: {error}"))?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hash.finalize().into())
}

fn select_active_versions(
    candidates: Vec<Candidate>,
) -> Result<(Vec<Candidate>, usize), PluginHostError> {
    let mut grouped: BTreeMap<ObjectId, Vec<Candidate>> = BTreeMap::new();
    for candidate in candidates {
        grouped
            .entry(candidate.package_id)
            .or_default()
            .push(candidate);
    }
    let mut active = Vec::with_capacity(grouped.len());
    let mut shadowed = 0_usize;
    for (package_id, mut versions) in grouped {
        let names: BTreeSet<_> = versions
            .iter()
            .map(|candidate| candidate.manifest.name.as_str())
            .collect();
        if names.len() != 1 {
            return Err(PluginHostError::Registry(format!(
                "package identity {} is reused by different names",
                format_id(package_id)
            )));
        }
        versions.sort_by(|left, right| left.manifest.version.cmp(&right.manifest.version));
        for pair in versions.windows(2) {
            if pair[0].manifest.version == pair[1].manifest.version {
                return Err(PluginHostError::Registry(format!(
                    "duplicate package identity {} version {}",
                    format_id(package_id),
                    pair[0].manifest.version
                )));
            }
        }
        shadowed = shadowed
            .checked_add(versions.len().saturating_sub(1))
            .ok_or_else(|| PluginHostError::Registry("shadowed version count overflow".into()))?;
        active.push(versions.pop().expect("group is non-empty"));
    }
    let mut names = BTreeMap::<String, ObjectId>::new();
    for candidate in &active {
        if let Some(previous) = names.insert(candidate.manifest.name.clone(), candidate.package_id)
        {
            if previous != candidate.package_id {
                return Err(PluginHostError::Registry(format!(
                    "package name {} is claimed by identities {} and {}",
                    candidate.manifest.name,
                    format_id(previous),
                    format_id(candidate.package_id)
                )));
            }
        }
    }
    Ok((active, shadowed))
}

fn load_candidate(candidate: Candidate) -> Result<LoadedObjects, PluginHostError> {
    let library = unsafe { Library::new(&candidate.library) }.map_err(|error| {
        package_error(
            &candidate.directory,
            format!("load {}: {error}", candidate.library.display()),
        )
    })?;
    // Deliberately leak immediately. Even a rejected descriptor may have run
    // constructors or returned pointers; PLUG-20 never invokes `dlclose`.
    let library: &'static Library = Box::leak(Box::new(library));
    let entrypoint = unsafe { library.get::<RadixPluginEntrypointV1>(ENTRYPOINT_SYMBOL_V1) }
        .map_err(|error| package_error(&candidate.directory, format!("entrypoint: {error}")))?;
    let host = RadixHostApiV1 {
        header: RadixAbiHeaderV1::new::<RadixHostApiV1>(0),
        handle: 1,
        max_external_value_bytes: RADIX_MAX_EXTERNAL_VALUE_BYTES,
        max_batch_rows: 65_535,
        max_planner_spans: RADIX_MAX_PLANNER_SPANS,
        reserved: 0,
        log: Some(host_log),
    };
    let mut status = RADIX_STATUS_INTERNAL_ERROR;
    let descriptor = unsafe { entrypoint(&host, &mut status) };
    if status != RADIX_STATUS_OK {
        return Err(package_error(
            &candidate.directory,
            format!("entrypoint returned status {status}"),
        ));
    }
    let descriptor = unsafe { descriptor.as_ref() }
        .ok_or_else(|| package_error(&candidate.directory, "entrypoint returned null"))?;
    validate_package_descriptor_shallow(descriptor).map_err(|error| {
        package_error(
            &candidate.directory,
            format!("invalid package descriptor: {error:?}"),
        )
    })?;
    let package_name = unsafe { copy_string(descriptor.package_name) }
        .map_err(|reason| package_error(&candidate.directory, reason))?;
    let package_version = unsafe { copy_string(descriptor.package_version) }
        .map_err(|reason| package_error(&candidate.directory, reason))?;
    let parsed_version = Version::parse(&package_version).map_err(|error| {
        package_error(
            &candidate.directory,
            format!("descriptor package version: {error}"),
        )
    })?;
    if descriptor.package_id != candidate.package_id
        || package_name != candidate.manifest.name
        || parsed_version != candidate.manifest.version
        || package_version != parsed_version.to_string()
        || descriptor.descriptor_fingerprint != candidate.descriptor_fingerprint
        || descriptor.abi_min_minor != candidate.manifest.abi_min_minor
        || descriptor.abi_max_minor != candidate.manifest.abi_max_minor
    {
        return Err(package_error(
            &candidate.directory,
            "manifest and embedded package descriptor differ",
        ));
    }
    let abi_minor = negotiate_minor(
        RADIX_ABI_MINOR,
        RADIX_ABI_MINOR,
        descriptor.abi_min_minor,
        descriptor.abi_max_minor,
    )
    .map_err(|error| package_error(&candidate.directory, format!("ABI negotiation: {error:?}")))?;

    let raw_types = unsafe { copy_table(descriptor.types, descriptor.type_count) };
    let raw_functions = unsafe { copy_table(descriptor.functions, descriptor.function_count) };
    let raw_operators = unsafe { copy_table(descriptor.operators, descriptor.operator_count) };
    let raw_operator_classes =
        unsafe { copy_table(descriptor.operator_classes, descriptor.operator_class_count) };
    let raw_planner =
        unsafe { copy_table(descriptor.planner_support, descriptor.planner_support_count) };

    let external_types = raw_types
        .iter()
        .map(|raw| copy_external_type(candidate.package_id, raw))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|reason| package_error(&candidate.directory, reason))?;
    let functions = raw_functions
        .iter()
        .map(|raw| copy_function(candidate.package_id, raw))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|reason| package_error(&candidate.directory, reason))?;
    let operators = raw_operators
        .iter()
        .map(|raw| copy_operator(candidate.package_id, raw))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|reason| package_error(&candidate.directory, reason))?;
    let operator_classes = raw_operator_classes
        .iter()
        .map(|raw| copy_operator_class(candidate.package_id, raw))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|reason| package_error(&candidate.directory, reason))?;
    let planner_support = raw_planner
        .iter()
        .map(|raw| copy_planner_support(candidate.package_id, raw))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|reason| package_error(&candidate.directory, reason))?;

    Ok(LoadedObjects {
        package: RegisteredPackage {
            package_id: candidate.package_id,
            name: package_name,
            version: parsed_version,
            abi_major: candidate.manifest.abi_major,
            abi_min_minor: descriptor.abi_min_minor,
            abi_max_minor: descriptor.abi_max_minor,
            abi_minor,
            descriptor_fingerprint: descriptor.descriptor_fingerprint,
        },
        capabilities: descriptor.header.flags,
        external_types,
        functions,
        operators,
        operator_classes,
        planner_support,
    })
}

/// Admit a statically linked descriptor through the same copy and graph gates
/// as the production loader. The caller keeps all callback code loaded for the
/// registry lifetime; this helper is available only to tests.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub unsafe fn registry_from_test_descriptor(
    descriptor: &'static RadixPluginDescriptorV1,
) -> Result<Arc<PluginRegistry>, PluginHostError> {
    validate_package_descriptor_shallow(descriptor).map_err(|error| {
        PluginHostError::Registry(format!("invalid package descriptor: {error:?}"))
    })?;
    let package_id = descriptor.package_id;
    let package_name =
        unsafe { copy_string(descriptor.package_name) }.map_err(PluginHostError::Registry)?;
    let package_version =
        unsafe { copy_string(descriptor.package_version) }.map_err(PluginHostError::Registry)?;
    let parsed_version = Version::parse(&package_version)
        .map_err(|error| PluginHostError::Registry(format!("package version: {error}")))?;
    if package_version != parsed_version.to_string() {
        return Err(PluginHostError::Registry(
            "package version is not canonical SemVer".into(),
        ));
    }
    let abi_minor = negotiate_minor(
        RADIX_ABI_MINOR,
        RADIX_ABI_MINOR,
        descriptor.abi_min_minor,
        descriptor.abi_max_minor,
    )
    .map_err(|error| PluginHostError::Registry(format!("ABI negotiation: {error:?}")))?;

    let raw_types = unsafe { copy_table(descriptor.types, descriptor.type_count) };
    let raw_functions = unsafe { copy_table(descriptor.functions, descriptor.function_count) };
    let raw_operators = unsafe { copy_table(descriptor.operators, descriptor.operator_count) };
    let raw_operator_classes =
        unsafe { copy_table(descriptor.operator_classes, descriptor.operator_class_count) };
    let raw_planner =
        unsafe { copy_table(descriptor.planner_support, descriptor.planner_support_count) };
    let loaded = LoadedObjects {
        package: RegisteredPackage {
            package_id,
            name: package_name,
            version: parsed_version,
            abi_major: descriptor.header.abi_major,
            abi_min_minor: descriptor.abi_min_minor,
            abi_max_minor: descriptor.abi_max_minor,
            abi_minor,
            descriptor_fingerprint: descriptor.descriptor_fingerprint,
        },
        capabilities: descriptor.header.flags,
        external_types: raw_types
            .iter()
            .map(|raw| copy_external_type(package_id, raw))
            .collect::<Result<_, _>>()
            .map_err(PluginHostError::Registry)?,
        functions: raw_functions
            .iter()
            .map(|raw| copy_function(package_id, raw))
            .collect::<Result<_, _>>()
            .map_err(PluginHostError::Registry)?,
        operators: raw_operators
            .iter()
            .map(|raw| copy_operator(package_id, raw))
            .collect::<Result<_, _>>()
            .map_err(PluginHostError::Registry)?,
        operator_classes: raw_operator_classes
            .iter()
            .map(|raw| copy_operator_class(package_id, raw))
            .collect::<Result<_, _>>()
            .map_err(PluginHostError::Registry)?,
        planner_support: raw_planner
            .iter()
            .map(|raw| copy_planner_support(package_id, raw))
            .collect::<Result<_, _>>()
            .map_err(PluginHostError::Registry)?,
    };
    validate_loaded_graph(std::slice::from_ref(&loaded))?;
    Ok(Arc::new(build_registry(1, vec![loaded], 0, 0)))
}

unsafe extern "C" fn host_log(
    _handle: u64,
    level: u16,
    reserved: u16,
    message: RadixAbiStringV1,
) -> RadixAbiStatusV1 {
    if validate_log_record(level, reserved, message).is_err() {
        return RADIX_STATUS_CONTRACT_VIOLATION;
    }
    let bytes = if message.len == 0 {
        &[][..]
    } else {
        unsafe { slice::from_raw_parts(message.ptr, message.len as usize) }
    };
    let Ok(message) = str::from_utf8(bytes) else {
        return RADIX_STATUS_CONTRACT_VIOLATION;
    };
    let level = match level {
        RADIX_LOG_INFO => "INFO",
        RADIX_LOG_WARN => "WARN",
        RADIX_LOG_ERROR => "ERROR",
        RADIX_LOG_DEBUG => "DEBUG",
        _ => return RADIX_STATUS_CONTRACT_VIOLATION,
    };
    eprintln!("radixdb plugin [{level}] {message}");
    RADIX_STATUS_OK
}

unsafe fn copy_table<T: Copy>(pointer: *const T, count: u32) -> Vec<T> {
    if count == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(pointer, count as usize) }.to_vec()
    }
}

unsafe fn copy_string(value: RadixAbiStringV1) -> Result<String, String> {
    validate_slice(value, RADIX_MAX_LOCAL_ID_BYTES, 1)
        .map_err(|error| format!("invalid string slice: {error:?}"))?;
    let bytes = if value.len == 0 {
        &[][..]
    } else {
        unsafe { slice::from_raw_parts(value.ptr, value.len as usize) }
    };
    let value = str::from_utf8(bytes).map_err(|_| "descriptor string is not UTF-8")?;
    if value.as_bytes().contains(&0) {
        return Err("descriptor string contains NUL".into());
    }
    Ok(value.to_owned())
}

fn copy_external_type(
    package_id: ObjectId,
    raw: &RadixAbiExternalTypeDescriptorV1,
) -> Result<RegisteredExternalType, String> {
    validate_external_type_descriptor(raw)
        .map_err(|error| format!("invalid external type descriptor: {error:?}"))?;
    let local_id = unsafe { copy_string(raw.local_id) }?;
    validate_identity(package_id, &local_id, raw.object_id)?;
    Ok(RegisteredExternalType {
        package_id,
        object_id: raw.object_id,
        local_id,
        display_name: unsafe { copy_string(raw.display_name) }?,
        codec_version: raw.codec_version,
        semantic_revision: raw.semantic_revision,
        storage_kind: raw.storage_kind,
        fixed_bytes: raw.fixed_bytes,
        max_bytes: raw.max_bytes,
        capabilities: raw.capabilities,
        codec_fingerprint: raw.codec_fingerprint,
        encode: raw.encode.expect("validated encode callback"),
        decode: raw.decode.expect("validated decode callback"),
        equality: raw.equality,
        hash: raw.hash,
        ordering: raw.ordering,
        text_input: raw.text_input,
        text_output: raw.text_output,
        binary_input: raw.binary_input,
        binary_output: raw.binary_output,
    })
}

fn copy_function(
    package_id: ObjectId,
    raw: &RadixAbiScalarFunctionDescriptorV1,
) -> Result<RegisteredFunction, String> {
    validate_scalar_function_descriptor(raw)
        .map_err(|error| format!("invalid function descriptor: {error:?}"))?;
    let local_id = unsafe { copy_string(raw.local_id) }?;
    validate_identity(package_id, &local_id, raw.object_id)?;
    let arguments = unsafe { copy_table(raw.arguments, raw.argument_count) }
        .iter()
        .map(convert_type_ref)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RegisteredFunction {
        package_id,
        object_id: raw.object_id,
        local_id,
        display_name: unsafe { copy_string(raw.display_name) }?,
        semantic_revision: raw.semantic_revision,
        arguments,
        result: convert_type_ref(&raw.result)?,
        volatility: raw.volatility,
        cancellation: raw.cancellation,
        strict: raw.strict != 0,
        parallel_safe: raw.parallel_safe != 0,
        cost: raw.cost,
        max_output_bytes: raw.max_output_bytes,
        scalar: raw.scalar.expect("validated scalar callback"),
        batch: raw.batch,
    })
}

fn copy_operator(
    package_id: ObjectId,
    raw: &RadixAbiOperatorDescriptorV1,
) -> Result<RegisteredOperator, String> {
    validate_operator_descriptor(raw)
        .map_err(|error| format!("invalid operator descriptor: {error:?}"))?;
    let local_id = unsafe { copy_string(raw.local_id) }?;
    validate_identity(package_id, &local_id, raw.object_id)?;
    Ok(RegisteredOperator {
        package_id,
        object_id: raw.object_id,
        local_id,
        symbol: unsafe { copy_string(raw.symbol) }?,
        semantic_revision: raw.semantic_revision,
        left: (raw.left != RadixAbiTypeRefV1::ABSENT)
            .then(|| convert_type_ref(&raw.left))
            .transpose()?,
        right: convert_type_ref(&raw.right)?,
        result: convert_type_ref(&raw.result)?,
        function_id: raw.function_id,
    })
}

fn copy_operator_class(
    package_id: ObjectId,
    raw: &RadixAbiOperatorClassDescriptorV1,
) -> Result<RegisteredOperatorClass, String> {
    validate_operator_class_descriptor(raw)
        .map_err(|error| format!("invalid operator class descriptor: {error:?}"))?;
    let local_id = unsafe { copy_string(raw.local_id) }?;
    validate_identity(package_id, &local_id, raw.object_id)?;
    let strategies = unsafe { copy_table(raw.strategies, raw.strategy_count) };
    let supports = unsafe { copy_table(raw.supports, raw.support_count) };
    Ok(RegisteredOperatorClass {
        package_id,
        object_id: raw.object_id,
        local_id,
        semantic_revision: raw.semantic_revision,
        access_method: raw.access_method,
        input_type: convert_type_ref(&raw.input_type)?,
        key_type: convert_type_ref(&raw.key_type)?,
        key_codec_revision: raw.key_codec_revision,
        strategies: convert_bindings(&strategies)?,
        supports: convert_bindings(&supports)?,
        fingerprint: raw.fingerprint,
        encode_key: raw.encode_key.expect("validated key encoder"),
    })
}

fn copy_planner_support(
    package_id: ObjectId,
    raw: &RadixAbiPlannerSupportDescriptorV1,
) -> Result<RegisteredPlannerSupport, String> {
    validate_planner_support_descriptor(raw)
        .map_err(|error| format!("invalid planner support descriptor: {error:?}"))?;
    let local_id = unsafe { copy_string(raw.local_id) }?;
    validate_identity(package_id, &local_id, raw.object_id)?;
    Ok(RegisteredPlannerSupport {
        package_id,
        object_id: raw.object_id,
        local_id,
        semantic_revision: raw.semantic_revision,
        max_spans: raw.max_spans,
        max_output_bytes: raw.max_output_bytes,
        recheck_policy: raw.recheck_policy,
        target_function_id: (raw.target_function_id != [0; 16]).then_some(raw.target_function_id),
        target_operator_class_id: (raw.target_operator_class_id != [0; 16])
            .then_some(raw.target_operator_class_id),
        fingerprint: raw.fingerprint,
        callback: raw.callback.expect("validated planner callback"),
    })
}

fn convert_type_ref(raw: &RadixAbiTypeRefV1) -> Result<RegisteredTypeRef, String> {
    validate_type_ref(raw).map_err(|error| format!("invalid type reference: {error:?}"))?;
    match raw.kind {
        RADIX_TYPE_REF_BUILTIN => Ok(RegisteredTypeRef::Builtin(raw.builtin_tag)),
        RADIX_TYPE_REF_EXTERNAL => Ok(RegisteredTypeRef::External {
            object_id: raw.object_id,
            codec_version: raw.codec_version,
        }),
        _ => Err("invalid type reference kind".into()),
    }
}

fn convert_bindings(raw: &[RadixAbiBindingEntryV1]) -> Result<Vec<RegisteredBinding>, String> {
    let mut previous = 0_u16;
    let mut output = Vec::with_capacity(raw.len());
    for entry in raw {
        if entry.slot == 0
            || entry.slot <= previous
            || entry.flags != 0
            || entry.object_id == [0; 16]
        {
            return Err(
                "binding table must have sorted unique nonzero slots and zero flags".into(),
            );
        }
        previous = entry.slot;
        output.push(RegisteredBinding {
            slot: entry.slot,
            object_id: entry.object_id,
        });
    }
    Ok(output)
}

fn validate_identity(package_id: ObjectId, local_id: &str, actual: ObjectId) -> Result<(), String> {
    let expected = crate::derive_object_id(package_id, local_id)?;
    if actual != expected {
        return Err(format!(
            "object {} does not match derived identity {} for local id {local_id}",
            format_id(actual),
            format_id(expected)
        ));
    }
    Ok(())
}

fn validate_loaded_graph(packages: &[LoadedObjects]) -> Result<(), PluginHostError> {
    let mut objects = BTreeMap::<ObjectId, (&str, &'static str)>::new();
    let mut types = BTreeMap::<ObjectId, &RegisteredExternalType>::new();
    let mut functions = BTreeMap::<ObjectId, &RegisteredFunction>::new();
    let mut operators = BTreeMap::<ObjectId, &RegisteredOperator>::new();
    let mut operator_classes = BTreeMap::<ObjectId, &RegisteredOperatorClass>::new();
    let mut planner_support = BTreeMap::<ObjectId, &RegisteredPlannerSupport>::new();
    let mut function_overloads = BTreeSet::new();
    let mut operator_overloads = BTreeSet::new();

    for package in packages {
        validate_capability_parity(package)?;
        for value in &package.external_types {
            insert_object(&mut objects, value.object_id, &value.local_id, "type")?;
            types.insert(value.object_id, value);
        }
        for value in &package.functions {
            insert_object(&mut objects, value.object_id, &value.local_id, "function")?;
            if !function_overloads.insert((value.display_name.clone(), value.arguments.clone())) {
                return Err(PluginHostError::Registry(format!(
                    "ambiguous function overload {}({:?})",
                    value.display_name, value.arguments
                )));
            }
            functions.insert(value.object_id, value);
        }
        for value in &package.operators {
            insert_object(&mut objects, value.object_id, &value.local_id, "operator")?;
            if !operator_overloads.insert((value.symbol.clone(), value.left, value.right)) {
                return Err(PluginHostError::Registry(format!(
                    "ambiguous operator overload {}",
                    value.symbol
                )));
            }
            operators.insert(value.object_id, value);
        }
        for value in &package.operator_classes {
            insert_object(
                &mut objects,
                value.object_id,
                &value.local_id,
                "operator class",
            )?;
            operator_classes.insert(value.object_id, value);
        }
        for value in &package.planner_support {
            insert_object(
                &mut objects,
                value.object_id,
                &value.local_id,
                "planner support",
            )?;
            planner_support.insert(value.object_id, value);
        }
    }

    for function in functions.values() {
        for reference in function.arguments.iter().chain([&function.result]) {
            validate_type_resolution(reference, &types)?;
        }
    }
    for operator in operators.values() {
        if let Some(left) = operator.left {
            validate_type_resolution(&left, &types)?;
        }
        validate_type_resolution(&operator.right, &types)?;
        validate_type_resolution(&operator.result, &types)?;
        let function = functions.get(&operator.function_id).ok_or_else(|| {
            PluginHostError::Registry(format!(
                "operator {} references missing function {}",
                operator.local_id,
                format_id(operator.function_id)
            ))
        })?;
        let expected_arguments: Vec<_> =
            operator.left.into_iter().chain([operator.right]).collect();
        if function.arguments != expected_arguments || function.result != operator.result {
            return Err(PluginHostError::Registry(format!(
                "operator {} and backing function signature differ",
                operator.local_id
            )));
        }
    }
    for class in operator_classes.values() {
        validate_type_resolution(&class.input_type, &types)?;
        if !matches!(class.key_type, RegisteredTypeRef::Builtin(_)) {
            return Err(PluginHostError::Registry(format!(
                "operator class {} key type must be core-owned",
                class.local_id
            )));
        }
        let RegisteredTypeRef::External { object_id, .. } = class.input_type else {
            return Err(PluginHostError::Registry(format!(
                "operator class {} input type must be package-defined",
                class.local_id
            )));
        };
        let input = types.get(&object_id).expect("type resolution was checked");
        match class.access_method {
            radixdb_plugin_abi::RADIX_ACCESS_METHOD_BTREE if input.ordering.is_none() => {
                return Err(PluginHostError::Registry(format!(
                    "B-tree operator class {} requires input ordering",
                    class.local_id
                )));
            }
            radixdb_plugin_abi::RADIX_ACCESS_METHOD_HASH => {
                if input.equality.is_none() || input.hash.is_none() {
                    return Err(PluginHostError::Registry(format!(
                        "hash operator class {} requires input equality and hash",
                        class.local_id
                    )));
                }
                if class.key_type
                    != RegisteredTypeRef::Builtin(radixdb_plugin_abi::RADIX_BUILTIN_BYTES)
                {
                    return Err(PluginHostError::Registry(format!(
                        "hash operator class {} physical key must be BYTES",
                        class.local_id
                    )));
                }
            }
            _ => {}
        }
        let expected_strategies: &[(u16, &str)] = match class.access_method {
            radixdb_plugin_abi::RADIX_ACCESS_METHOD_BTREE => {
                &[(1, "<"), (2, "<="), (3, "="), (4, ">="), (5, ">")]
            }
            radixdb_plugin_abi::RADIX_ACCESS_METHOD_HASH
            | radixdb_plugin_abi::RADIX_ACCESS_METHOD_BITMAP => &[(1, "=")],
            radixdb_plugin_abi::RADIX_ACCESS_METHOD_HNSW => {
                return Err(PluginHostError::Registry(format!(
                    "external HNSW operator class {} requires planner support outside the v1.2 boundary",
                    class.local_id
                )));
            }
            _ => unreachable!("access method was validated"),
        };
        if class.strategies.len() != expected_strategies.len() {
            return Err(PluginHostError::Registry(format!(
                "operator class {} has an incomplete strategy table",
                class.local_id
            )));
        }
        for ((expected_slot, expected_symbol), binding) in
            expected_strategies.iter().zip(&class.strategies)
        {
            let Some(operator) = operators.get(&binding.object_id) else {
                return Err(PluginHostError::Registry(format!(
                    "operator class {} references missing strategy {}",
                    class.local_id,
                    format_id(binding.object_id)
                )));
            };
            if binding.slot != *expected_slot
                || operator.symbol != *expected_symbol
                || operator.left != Some(class.input_type)
                || operator.right != class.input_type
                || operator.result
                    != RegisteredTypeRef::Builtin(radixdb_plugin_abi::RADIX_BUILTIN_BOOLEAN)
                || operator.package_id != class.package_id
            {
                return Err(PluginHostError::Registry(format!(
                    "operator class {} strategy slot {} is semantically incompatible",
                    class.local_id, expected_slot
                )));
            }
        }
        for binding in &class.supports {
            if !planner_support.contains_key(&binding.object_id) {
                return Err(PluginHostError::Registry(format!(
                    "operator class {} references missing planner support {}",
                    class.local_id,
                    format_id(binding.object_id)
                )));
            }
        }
    }
    for support in planner_support.values() {
        if support
            .target_function_id
            .is_some_and(|id| !functions.contains_key(&id))
            || support
                .target_operator_class_id
                .is_some_and(|id| !operator_classes.contains_key(&id))
        {
            return Err(PluginHostError::Registry(format!(
                "planner support {} has an unresolved target",
                support.local_id
            )));
        }
    }
    Ok(())
}

fn validate_capability_parity(package: &LoadedObjects) -> Result<(), PluginHostError> {
    let flags = package.capabilities;
    let expected = [
        (
            RADIX_PACKAGE_CAP_EXTERNAL_TYPES,
            !package.external_types.is_empty(),
        ),
        (
            RADIX_PACKAGE_CAP_SCALAR_FUNCTIONS,
            !package.functions.is_empty(),
        ),
        (RADIX_PACKAGE_CAP_OPERATORS, !package.operators.is_empty()),
        (
            RADIX_PACKAGE_CAP_OPERATOR_CLASSES,
            !package.operator_classes.is_empty(),
        ),
        (
            RADIX_PACKAGE_CAP_PLANNER_SUPPORT,
            !package.planner_support.is_empty(),
        ),
        (
            RADIX_PACKAGE_CAP_BATCH_FUNCTIONS,
            package
                .functions
                .iter()
                .any(|function| function.batch.is_some()),
        ),
    ];
    if expected
        .iter()
        .any(|(flag, present)| (flags & *flag != 0) != *present)
    {
        return Err(PluginHostError::Registry(format!(
            "package {} capability flags do not match descriptor graph",
            package.package.name
        )));
    }
    Ok(())
}

fn insert_object<'a>(
    objects: &mut BTreeMap<ObjectId, (&'a str, &'static str)>,
    id: ObjectId,
    local_id: &'a str,
    kind: &'static str,
) -> Result<(), PluginHostError> {
    if let Some((previous_id, previous_kind)) = objects.insert(id, (local_id, kind)) {
        return Err(PluginHostError::Registry(format!(
            "object identity collision: {previous_kind} {previous_id} and {kind} {local_id}"
        )));
    }
    Ok(())
}

fn validate_type_resolution(
    reference: &RegisteredTypeRef,
    types: &BTreeMap<ObjectId, &RegisteredExternalType>,
) -> Result<(), PluginHostError> {
    let RegisteredTypeRef::External {
        object_id,
        codec_version,
    } = reference
    else {
        return Ok(());
    };
    let value = types.get(object_id).ok_or_else(|| {
        PluginHostError::Registry(format!(
            "unresolved external type {}",
            format_id(*object_id)
        ))
    })?;
    if value.codec_version != *codec_version {
        return Err(PluginHostError::Registry(format!(
            "external type {} requires codec {}, registry has {}",
            format_id(*object_id),
            codec_version,
            value.codec_version
        )));
    }
    Ok(())
}

fn build_registry(
    generation: u64,
    loaded: Vec<LoadedObjects>,
    shadowed_versions: usize,
    loaded_library_bytes: u64,
) -> PluginRegistry {
    let mut registry = PluginRegistry {
        generation,
        packages: BTreeMap::new(),
        external_types: BTreeMap::new(),
        functions: BTreeMap::new(),
        operators: BTreeMap::new(),
        operator_classes: BTreeMap::new(),
        planner_support: BTreeMap::new(),
        shadowed_versions,
        loaded_library_bytes,
    };
    for package in loaded {
        let package_id = package.package.package_id;
        registry
            .packages
            .insert(package_id, Arc::new(package.package));
        for value in package.external_types {
            registry
                .external_types
                .insert(value.object_id, Arc::new(value));
        }
        for value in package.functions {
            registry.functions.insert(value.object_id, Arc::new(value));
        }
        for value in package.operators {
            registry.operators.insert(value.object_id, Arc::new(value));
        }
        for value in package.operator_classes {
            registry
                .operator_classes
                .insert(value.object_id, Arc::new(value));
        }
        for value in package.planner_support {
            registry
                .planner_support
                .insert(value.object_id, Arc::new(value));
        }
    }
    registry
}

fn validate_host_glibc(required: &str) -> Result<(), PluginHostError> {
    let version = unsafe { CStr::from_ptr(libc::gnu_get_libc_version()) }
        .to_str()
        .map_err(|_| PluginHostError::Configuration("host glibc version is not UTF-8".into()))?;
    let mut components = version.split('.');
    let major = components
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| PluginHostError::Configuration(format!("invalid host glibc {version}")))?;
    let minor = components
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| PluginHostError::Configuration(format!("invalid host glibc {version}")))?;
    let required = parse_glibc_version(required).ok_or_else(|| {
        PluginHostError::Configuration("invalid package glibc requirement".into())
    })?;
    if (major, minor) < required {
        return Err(PluginHostError::Configuration(format!(
            "host glibc {version} is older than required {}.{}",
            required.0, required.1
        )));
    }
    Ok(())
}

fn format_id(id: ObjectId) -> String {
    let mut output = String::with_capacity(32);
    for byte in id {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C" fn codec(
        _context: *const RadixAbiCallContextV1,
        _input: *const RadixAbiValueV1,
        _output: *const RadixAbiResultBuilderV1,
    ) -> RadixAbiStatusV1 {
        RADIX_STATUS_OK
    }

    unsafe extern "C" fn parse(
        _context: *const RadixAbiCallContextV1,
        _input: RadixAbiSliceV1,
        _output: *const RadixAbiResultBuilderV1,
    ) -> RadixAbiStatusV1 {
        RADIX_STATUS_OK
    }

    unsafe extern "C" fn scalar(
        _context: *const RadixAbiCallContextV1,
        _arguments: *const RadixAbiValueV1,
        _argument_count: u32,
        _output: *const RadixAbiResultBuilderV1,
    ) -> RadixAbiStatusV1 {
        RADIX_STATUS_OK
    }

    unsafe extern "C" fn key(
        _context: *const RadixAbiCallContextV1,
        _value: *const RadixAbiValueV1,
        _output: *const RadixAbiResultBuilderV1,
    ) -> RadixAbiStatusV1 {
        RADIX_STATUS_OK
    }

    unsafe extern "C" fn equal(
        _context: *const RadixAbiCallContextV1,
        _left: *const RadixAbiValueV1,
        _right: *const RadixAbiValueV1,
        output: *mut u8,
    ) -> RadixAbiStatusV1 {
        if let Some(output) = unsafe { output.as_mut() } {
            *output = 1;
            RADIX_STATUS_OK
        } else {
            RADIX_STATUS_INVALID_ARGUMENT
        }
    }

    unsafe extern "C" fn hash(
        _context: *const RadixAbiCallContextV1,
        _value: *const RadixAbiValueV1,
        _sink: *const RadixAbiHashSinkV1,
    ) -> RadixAbiStatusV1 {
        RADIX_STATUS_OK
    }

    unsafe extern "C" fn support(
        _context: *const RadixAbiCallContextV1,
        _predicate: RadixAbiSliceV1,
        _output: *const RadixAbiResultBuilderV1,
    ) -> RadixAbiStatusV1 {
        RADIX_STATUS_OK
    }

    fn complete_graph() -> LoadedObjects {
        let package_id = [7; 16];
        let type_id = crate::derive_object_id(package_id, "point").unwrap();
        let function_id = crate::derive_object_id(package_id, "point_equal").unwrap();
        let operator_id = crate::derive_object_id(package_id, "point_eq_operator").unwrap();
        let class_id = crate::derive_object_id(package_id, "point_btree").unwrap();
        let support_id = crate::derive_object_id(package_id, "point_ranges").unwrap();
        let point = RegisteredTypeRef::External {
            object_id: type_id,
            codec_version: 1,
        };
        LoadedObjects {
            package: RegisteredPackage {
                package_id,
                name: "spatial".into(),
                version: Version::new(1, 0, 0),
                abi_major: RADIX_ABI_MAJOR,
                abi_min_minor: RADIX_ABI_MINOR,
                abi_max_minor: RADIX_ABI_MINOR,
                abi_minor: 0,
                descriptor_fingerprint: [1; 32],
            },
            capabilities: RADIX_PACKAGE_CAP_EXTERNAL_TYPES
                | RADIX_PACKAGE_CAP_SCALAR_FUNCTIONS
                | RADIX_PACKAGE_CAP_OPERATORS
                | RADIX_PACKAGE_CAP_OPERATOR_CLASSES
                | RADIX_PACKAGE_CAP_PLANNER_SUPPORT,
            external_types: vec![RegisteredExternalType {
                package_id,
                object_id: type_id,
                local_id: "point".into(),
                display_name: "point".into(),
                codec_version: 1,
                semantic_revision: 1,
                storage_kind: RADIX_EXTERNAL_STORAGE_FIXED,
                fixed_bytes: 16,
                max_bytes: 16,
                capabilities: RADIX_TYPE_CAP_EQUALITY | RADIX_TYPE_CAP_HASH,
                codec_fingerprint: [2; 32],
                encode: codec,
                decode: parse,
                equality: Some(equal),
                hash: Some(hash),
                ordering: None,
                text_input: None,
                text_output: None,
                binary_input: None,
                binary_output: None,
            }],
            functions: vec![RegisteredFunction {
                package_id,
                object_id: function_id,
                local_id: "point_equal".into(),
                display_name: "point_equal".into(),
                semantic_revision: 1,
                arguments: vec![point, point],
                result: RegisteredTypeRef::Builtin(RADIX_BUILTIN_BOOLEAN),
                volatility: RADIX_VOLATILITY_IMMUTABLE,
                cancellation: RADIX_CANCELLATION_BOUNDED,
                strict: true,
                parallel_safe: true,
                cost: 1,
                max_output_bytes: 1,
                scalar,
                batch: None,
            }],
            operators: vec![RegisteredOperator {
                package_id,
                object_id: operator_id,
                local_id: "point_eq_operator".into(),
                symbol: "=".into(),
                semantic_revision: 1,
                left: Some(point),
                right: point,
                result: RegisteredTypeRef::Builtin(RADIX_BUILTIN_BOOLEAN),
                function_id,
            }],
            operator_classes: vec![RegisteredOperatorClass {
                package_id,
                object_id: class_id,
                local_id: "point_btree".into(),
                semantic_revision: 1,
                access_method: RADIX_ACCESS_METHOD_HASH,
                input_type: point,
                key_type: RegisteredTypeRef::Builtin(RADIX_BUILTIN_BYTES),
                key_codec_revision: 1,
                strategies: vec![RegisteredBinding {
                    slot: 1,
                    object_id: operator_id,
                }],
                supports: vec![RegisteredBinding {
                    slot: 1,
                    object_id: support_id,
                }],
                fingerprint: [3; 32],
                encode_key: key,
            }],
            planner_support: vec![RegisteredPlannerSupport {
                package_id,
                object_id: support_id,
                local_id: "point_ranges".into(),
                semantic_revision: 1,
                max_spans: 8,
                max_output_bytes: 4096,
                recheck_policy: RADIX_RECHECK_ALWAYS,
                target_function_id: None,
                target_operator_class_id: Some(class_id),
                fingerprint: [4; 32],
                callback: support,
            }],
        }
    }

    #[test]
    fn complete_descriptor_graph_resolves_every_identity_and_signature() {
        validate_loaded_graph(&[complete_graph()]).unwrap();
    }

    #[test]
    fn unresolved_reference_and_batch_capability_fail_closed() {
        let mut unresolved = complete_graph();
        unresolved.functions[0].arguments[0] = RegisteredTypeRef::External {
            object_id: [99; 16],
            codec_version: 1,
        };
        assert!(validate_loaded_graph(&[unresolved])
            .unwrap_err()
            .to_string()
            .contains("unresolved external type"));

        let mut false_batch = complete_graph();
        false_batch.capabilities |= RADIX_PACKAGE_CAP_BATCH_FUNCTIONS;
        assert!(validate_loaded_graph(&[false_batch])
            .unwrap_err()
            .to_string()
            .contains("capability flags"));
    }
}
