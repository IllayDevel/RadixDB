//! Isolated Linux filesystem process used by ignored storage-crash evidence.

use std::fs::{self, OpenOptions};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const IMAGE_BYTES: u64 = 256 * 1024 * 1024;
const MOUNT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct CrashableFilesystem {
    _workspace: tempfile::TempDir,
    image: PathBuf,
    mount: PathBuf,
    host_device: u64,
    child: Option<Child>,
}

impl CrashableFilesystem {
    pub fn new() -> Self {
        let workspace = tempfile::Builder::new()
            .prefix("radixdb-crashable-fs-")
            .tempdir_in("/var/tmp")
            .expect("create isolated filesystem workspace");
        let image = workspace.path().join("filesystem.img");
        let mount = workspace.path().join("mount");
        fs::create_dir(&mount).expect("create isolated mount point");
        let host_device = fs::metadata(&mount).expect("stat mount point").dev();
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&image)
            .expect("create sparse filesystem image")
            .set_len(IMAGE_BYTES)
            .expect("size sparse filesystem image");
        require_status(
            Command::new("mkfs.ext4")
                .args(["-q", "-F", "-O", "^has_journal"])
                .arg(&image)
                .status()
                .expect("run mkfs.ext4"),
            &[0],
            "mkfs.ext4",
        );
        let mut filesystem = Self {
            _workspace: workspace,
            image,
            mount,
            host_device,
            child: None,
        };
        filesystem.start();
        filesystem
    }

    pub fn mount_path(&self) -> &Path {
        &self.mount
    }

    pub fn sync_baseline(&self, path: &Path) {
        require_status(
            Command::new("sync")
                .arg("-f")
                .arg(path)
                .status()
                .expect("run sync -f"),
            &[0],
            "sync -f",
        );
    }

    pub fn crash_repair_and_remount(&mut self) {
        let mut child = self.child.take().expect("mounted filesystem process");
        child.kill().expect("kill filesystem process");
        let _ = child.wait();
        let _ = Command::new("fusermount3")
            .args(["-u", "-z"])
            .arg(&self.mount)
            .status();
        self.wait_for_device(self.host_device, "detach crashed filesystem");
        let status = Command::new("e2fsck")
            .args(["-f", "-y"])
            .arg(&self.image)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run e2fsck after storage crash");
        // e2fsck bit 0 means errors were corrected. Bit 1 would request a
        // reboot for a mounted block device and is not expected for an image.
        require_status(status, &[0, 1], "e2fsck");
        self.start();
    }

    pub fn shutdown(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let status = Command::new("fusermount3")
            .arg("-u")
            .arg(&self.mount)
            .status()
            .expect("run fusermount3");
        if !status.success() {
            let _ = child.kill();
        }
        let _ = child.wait();
        self.wait_for_device(self.host_device, "unmount isolated filesystem");
    }

    fn start(&mut self) {
        assert!(self.child.is_none(), "filesystem process already running");
        let child = Command::new("fuse2fs")
            .args(["-f", "-o", "fakeroot"])
            .arg(&self.image)
            .arg(&self.mount)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start fuse2fs");
        self.child = Some(child);
        let started = Instant::now();
        loop {
            let device = fs::metadata(&self.mount).map(|metadata| metadata.dev());
            if device.is_ok_and(|device| device != self.host_device) {
                return;
            }
            if let Some(status) = self
                .child
                .as_mut()
                .expect("filesystem process")
                .try_wait()
                .expect("poll fuse2fs")
            {
                panic!("fuse2fs exited before mount: {status}");
            }
            assert!(started.elapsed() < MOUNT_TIMEOUT, "fuse2fs mount timeout");
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_device(&self, expected: u64, operation: &str) {
        let started = Instant::now();
        loop {
            if fs::metadata(&self.mount).is_ok_and(|metadata| metadata.dev() == expected) {
                return;
            }
            assert!(started.elapsed() < MOUNT_TIMEOUT, "{operation} timeout");
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for CrashableFilesystem {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn require_status(status: ExitStatus, accepted: &[i32], operation: &str) {
    assert!(
        status.code().is_some_and(|code| accepted.contains(&code)),
        "{operation} failed with {status}"
    );
}
