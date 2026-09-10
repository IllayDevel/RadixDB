use std::{
    fs,
    mem::{size_of, MaybeUninit},
    path::PathBuf,
    process::Command,
};

use radixdb_plugin_abi::*;

#[repr(C)]
struct OracleRecord {
    header: RadixAbiHeaderV1,
    type_ref: RadixAbiTypeRefV1,
    descriptor: RadixAbiExternalTypeDescriptorV1,
    value: RadixAbiValueV1,
}

#[test]
fn c11_fixture_and_rust_exchange_descriptor_and_value_bytes() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let temporary = std::env::temp_dir().join(format!(
        "radixdb-plugin-abi-c-oracle-{}",
        std::process::id()
    ));
    if temporary.exists() {
        fs::remove_dir_all(&temporary).unwrap();
    }
    fs::create_dir(&temporary).unwrap();
    let executable = temporary.join("abi-oracle");
    let output = temporary.join("oracle.bin");

    let compile = Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror"])
        .arg("-I")
        .arg(manifest.join("include"))
        .arg(manifest.join("tests/fixtures/abi_oracle.c"))
        .arg("-o")
        .arg(&executable)
        .output()
        .expect("execute C compiler");
    assert!(
        compile.status.success(),
        "C ABI oracle did not compile:\n{}\n{}",
        String::from_utf8_lossy(&compile.stdout),
        String::from_utf8_lossy(&compile.stderr)
    );
    assert!(Command::new(&executable)
        .arg(&output)
        .status()
        .unwrap()
        .success());

    let bytes = fs::read(&output).unwrap();
    assert_eq!(bytes.len(), size_of::<OracleRecord>());
    let mut record = MaybeUninit::<OracleRecord>::uninit();
    // SAFETY: the C oracle emitted exactly one repr(C) record after compile-time
    // size assertions; copy avoids alignment assumptions about Vec storage.
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr(),
            record.as_mut_ptr().cast::<u8>(),
            bytes.len(),
        );
    }
    // SAFETY: every byte was initialized by the preceding copy.
    let record = unsafe { record.assume_init() };

    assert_eq!(
        validate_header(&record.header, size_of::<RadixAbiHeaderV1>() as u32, 0),
        Ok(())
    );
    assert_eq!(record.header.struct_size, 48, "compatible C tail retained");
    assert_eq!(validate_type_ref(&record.type_ref), Ok(()));
    assert_eq!(record.type_ref.object_id[0], 0x42);
    assert_eq!(
        record.descriptor.header.struct_size as usize,
        size_of::<RadixAbiExternalTypeDescriptorV1>()
    );
    assert_eq!(record.descriptor.codec_version, 7);
    assert_eq!(record.descriptor.semantic_revision, 3);
    assert_eq!(record.descriptor.codec_fingerprint[0], 0xa5);
    assert_eq!(validate_value(&record.value), Ok(()));
    assert_eq!(record.value.inline_bytes[0], 0x7b);

    fs::remove_dir_all(&temporary).unwrap();
}
