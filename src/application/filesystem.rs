use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CROSS_DEVICE_LINK_ERROR: i32 = 18;

pub fn ensure_managed_root(root: &Path) -> io::Result<()> {
    if root.exists() {
        ensure_directory_component(root, "managed root")?;
        return Ok(());
    }

    let parent = root.parent().ok_or_else(|| {
        io::Error::other(format!(
            "managed root {} has no parent directory",
            root.display()
        ))
    })?;
    ensure_directory_component(parent, "managed root parent")?;
    fs::create_dir(root)?;
    ensure_directory_component(root, "managed root")?;
    Ok(())
}

pub fn assert_safe_source_file(path: &Path, label: &str) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to inspect {label} {}: {error}", path.display()),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::other(format!(
            "{label} {} was a symlink",
            path.display()
        )));
    }
    if !metadata.is_file() {
        return Err(io::Error::other(format!(
            "{label} {} was not a regular file",
            path.display()
        )));
    }
    Ok(())
}

pub fn ensure_safe_target_parent(root: &Path, target: &Path) -> io::Result<()> {
    ensure_managed_root(root)?;
    if !target.starts_with(root) {
        return Err(io::Error::other(format!(
            "managed target {} escaped root {}",
            target.display(),
            root.display()
        )));
    }

    let relative = target.strip_prefix(root).map_err(|error| {
        io::Error::other(format!(
            "managed target {} could not be validated under {}: {error}",
            target.display(),
            root.display()
        ))
    })?;
    let mut current = root.to_path_buf();
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    for component in parent.components() {
        match component {
            Component::Normal(value) => {
                current.push(value);
                if current.exists() {
                    ensure_directory_component(&current, "managed path component")?;
                } else {
                    fs::create_dir(&current)?;
                }
            }
            _ => {
                return Err(io::Error::other(format!(
                    "managed target {} contained an invalid path component",
                    target.display()
                )));
            }
        }
    }

    let canonical_root = fs::canonicalize(root).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to canonicalize managed root {}: {error}",
                root.display()
            ),
        )
    })?;
    let canonical_parent = fs::canonicalize(target.parent().unwrap_or(root)).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to canonicalize managed parent for {}: {error}",
                target.display()
            ),
        )
    })?;
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(io::Error::other(format!(
            "managed target parent {} escaped root {}",
            canonical_parent.display(),
            canonical_root.display()
        )));
    }

    Ok(())
}

pub fn atomic_copy_into_place(source: &Path, target: &Path, root: &Path) -> io::Result<()> {
    assert_safe_source_file(source, "source file")?;
    ensure_safe_target_parent(root, target)?;
    let temp_path = temporary_target_path(target);
    cleanup_stale_temp_path(&temp_path);
    fs::copy(source, &temp_path)?;
    if let Err(error) = fs::rename(&temp_path, target) {
        cleanup_stale_temp_path(&temp_path);
        return Err(error);
    }
    Ok(())
}

pub fn atomic_hard_link_into_place(source: &Path, target: &Path, root: &Path) -> io::Result<()> {
    assert_safe_source_file(source, "source file")?;
    ensure_safe_target_parent(root, target)?;
    let temp_path = temporary_target_path(target);
    cleanup_stale_temp_path(&temp_path);
    fs::hard_link(source, &temp_path)?;
    if let Err(error) = fs::rename(&temp_path, target) {
        cleanup_stale_temp_path(&temp_path);
        return Err(error);
    }
    Ok(())
}

pub fn move_into_place(source: &Path, target: &Path, root: &Path) -> io::Result<()> {
    assert_safe_source_file(source, "source file")?;
    ensure_safe_target_parent(root, target)?;
    match fs::rename(source, target) {
        Ok(()) => Ok(()),
        Err(error) if error.raw_os_error() == Some(CROSS_DEVICE_LINK_ERROR) => {
            atomic_copy_into_place(source, target, root)?;
            fs::remove_file(source)
        }
        Err(error) => Err(error),
    }
}

fn ensure_directory_component(path: &Path, label: &str) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to inspect {label} {}: {error}", path.display()),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::other(format!(
            "{label} {} was a symlink",
            path.display()
        )));
    }
    if !metadata.is_dir() {
        return Err(io::Error::other(format!(
            "{label} {} was not a directory",
            path.display()
        )));
    }
    Ok(())
}

fn temporary_target_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("discern");
    target.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        unix_timestamp_nanos()
    ))
}

fn cleanup_stale_temp_path(path: &Path) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}

fn unix_timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos()
}
