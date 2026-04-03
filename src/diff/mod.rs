use crate::util::error::{HarDataError, Result};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestEntry {
    path: String,
    line: String,
}

pub async fn run_diff(dir: String) -> Result<()> {
    let output = tokio::task::spawn_blocking(move || build_manifest_output(PathBuf::from(dir)))
        .await
        .map_err(|e| HarDataError::Unknown(format!("Diff task join error: {}", e)))??;

    let mut stdout = std::io::stdout().lock();
    stdout.write_all(output.as_bytes())?;
    Ok(())
}

/// 收集目录中所有条目的路径和元数据（不读文件内容）
struct CollectedPath {
    abs_path: PathBuf,
    relative_path: String,
    kind: PathKind,
}

enum PathKind {
    Dir,
    File { size: u64 },
    Symlink { target: String },
}

fn collect_paths(root: &Path, current: &Path, out: &mut Vec<CollectedPath>) -> Result<()> {
    let mut children = Vec::new();
    for entry in fs::read_dir(current)? {
        children.push(entry?.path());
    }
    children.sort();

    for path in children {
        let metadata = fs::symlink_metadata(&path)?;
        let relative_path = normalize_relative_path(root, &path)?;

        if metadata.file_type().is_symlink() {
            let target = normalize_path_string(&fs::read_link(&path)?)?;
            out.push(CollectedPath {
                abs_path: path,
                relative_path,
                kind: PathKind::Symlink { target },
            });
            continue;
        }

        if metadata.is_dir() {
            out.push(CollectedPath {
                abs_path: path.clone(),
                relative_path,
                kind: PathKind::Dir,
            });
            collect_paths(root, &path, out)?;
            continue;
        }

        if metadata.is_file() {
            out.push(CollectedPath {
                abs_path: path,
                relative_path,
                kind: PathKind::File {
                    size: metadata.len(),
                },
            });
            continue;
        }

        return Err(HarDataError::FileOperation(format!(
            "Unsupported file type: {}",
            path.display()
        )));
    }

    Ok(())
}

fn format_bytes_human(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.0} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn build_manifest_output(root: PathBuf) -> Result<String> {
    validate_root(&root)?;

    // 阶段1：收集所有路径
    let mut paths = Vec::new();
    eprintln!("[diff] Scanning directory: {}", root.display());
    collect_paths(&root, &root, &mut paths)?;

    let file_count = paths.iter().filter(|p| matches!(p.kind, PathKind::File { .. })).count();
    let total_bytes: u64 = paths.iter().filter_map(|p| match p.kind {
        PathKind::File { size } => Some(size),
        _ => None,
    }).sum();
    eprintln!(
        "[diff] Found {} entries ({} files, {})",
        paths.len(),
        file_count,
        format_bytes_human(total_bytes)
    );

    // 阶段2：逐个处理，计算哈希
    let mut entries = Vec::with_capacity(paths.len());
    let mut processed_files: usize = 0;
    let mut processed_bytes: u64 = 0;
    let progress_interval = (file_count / 20).max(1);

    for item in &paths {
        match &item.kind {
            PathKind::Symlink { target } => {
                entries.push(ManifestEntry {
                    path: item.relative_path.clone(),
                    line: format!(
                        "L\t{}\t{}",
                        serde_json::to_string(&item.relative_path)?,
                        serde_json::to_string(target)?
                    ),
                });
            }
            PathKind::Dir => {
                entries.push(ManifestEntry {
                    path: item.relative_path.clone(),
                    line: format!("D\t{}", serde_json::to_string(&item.relative_path)?),
                });
            }
            PathKind::File { size } => {
                let hash = hash_file(&item.abs_path)?;
                entries.push(ManifestEntry {
                    path: item.relative_path.clone(),
                    line: format!(
                        "F\t{}\t{}\t{}",
                        serde_json::to_string(&item.relative_path)?,
                        size,
                        hash
                    ),
                });
                processed_files += 1;
                processed_bytes += size;
                if processed_files % progress_interval == 0 || processed_files == file_count {
                    eprintln!(
                        "[diff] Hashing: {}/{} files ({}/{})",
                        processed_files,
                        file_count,
                        format_bytes_human(processed_bytes),
                        format_bytes_human(total_bytes)
                    );
                }
            }
        }
    }

    entries.sort_by(|left, right| left.path.cmp(&right.path));

    let mut output = String::new();
    let mut summary_hasher = blake3::Hasher::new();

    for (index, entry) in entries.iter().enumerate() {
        if index > 0 {
            summary_hasher.update(b"\n");
        }
        summary_hasher.update(entry.line.as_bytes());
        output.push_str(&entry.line);
        output.push('\n');
    }

    let summary_line = format!(
        "SUMMARY\t{}\t{}",
        entries.len(),
        summary_hasher.finalize().to_hex()
    );
    output.push_str(&summary_line);
    output.push('\n');

    eprintln!(
        "[diff] Done: {} entries, {}",
        entries.len(),
        format_bytes_human(processed_bytes)
    );

    Ok(output)
}

fn validate_root(root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            HarDataError::FileOperation(format!("Directory not found: {}", root.display()))
        } else {
            HarDataError::Io(e)
        }
    })?;

    if metadata.file_type().is_symlink() {
        return Err(HarDataError::FileOperation(format!(
            "Root path must be a directory, got symlink: {}",
            root.display()
        )));
    }

    if !metadata.is_dir() {
        return Err(HarDataError::FileOperation(format!(
            "Root path must be a directory: {}",
            root.display()
        )));
    }

    Ok(())
}

fn normalize_relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root).map_err(|e| {
        HarDataError::FileOperation(format!(
            "Failed to resolve relative path for '{}': {}",
            path.display(),
            e
        ))
    })?;

    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| {
                        HarDataError::FileOperation(format!(
                            "Path is not valid UTF-8: {}",
                            path.display()
                        ))
                    })?
                    .to_string(),
            ),
            other => {
                return Err(HarDataError::FileOperation(format!(
                    "Unsupported path component {:?} in {}",
                    other,
                    path.display()
                )));
            }
        }
    }

    Ok(parts.join("/"))
}

fn normalize_path_string(path: &Path) -> Result<String> {
    path.to_str().map(|value| value.to_string()).ok_or_else(|| {
        HarDataError::FileOperation(format!("Path is not valid UTF-8: {}", path.display()))
    })
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::build_manifest_output;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-diff-{label}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn manifest_output_for_empty_directory_contains_only_summary() {
        let root = temp_dir("empty");

        let output = build_manifest_output(root.clone()).unwrap();

        assert_eq!(
            output,
            format!(
                "SUMMARY\t0\t{}\n",
                blake3::Hasher::new().finalize().to_hex()
            )
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manifest_output_records_empty_directory_entries() {
        let root = temp_dir("empty-child");
        std::fs::create_dir_all(root.join("empty")).unwrap();

        let output = build_manifest_output(root.clone()).unwrap();

        assert!(output.contains("D\t\"empty\""));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manifest_output_is_stable_and_changes_with_file_content() {
        let root = temp_dir("stable");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/data.txt"), b"alpha").unwrap();
        std::fs::write(root.join("root.txt"), b"beta").unwrap();

        let first = build_manifest_output(root.clone()).unwrap();
        let second = build_manifest_output(root.clone()).unwrap();
        assert_eq!(first, second);

        std::fs::write(root.join("root.txt"), b"gamma").unwrap();
        let changed = build_manifest_output(root.clone()).unwrap();
        assert_ne!(first, changed);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manifest_output_accepts_relative_and_absolute_paths() {
        let cwd = std::env::current_dir().unwrap();
        let relative = PathBuf::from(format!(
            ".tmp-hardata-diff-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let absolute = cwd.join(&relative);

        std::fs::create_dir_all(absolute.join("nested")).unwrap();
        std::fs::write(absolute.join("nested/data.txt"), b"payload").unwrap();

        let relative_output = build_manifest_output(relative.clone()).unwrap();
        let absolute_output = build_manifest_output(absolute.clone()).unwrap();
        assert_eq!(relative_output, absolute_output);

        std::fs::remove_dir_all(absolute).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn manifest_output_records_symlink_without_following_it() {
        let root = temp_dir("symlink");
        let outside = temp_dir("symlink-outside");
        std::fs::write(outside.join("secret.txt"), b"secret").unwrap();
        std::os::unix::fs::symlink("../symlink-outside", root.join("linked")).unwrap();
        std::os::unix::fs::symlink("target.txt", root.join("file-link")).unwrap();
        std::fs::write(root.join("target.txt"), b"payload").unwrap();

        let output = build_manifest_output(root.clone()).unwrap();

        assert!(output.contains("L\t\"file-link\"\t\"target.txt\""));
        assert!(!output.contains("secret.txt"));

        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn manifest_output_rejects_non_utf8_paths() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let root = temp_dir("non-utf8");
        let non_utf8 = OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]);
        std::fs::write(root.join(non_utf8), b"payload").unwrap();

        let error = build_manifest_output(root.clone()).unwrap_err();
        assert!(error.to_string().contains("not valid UTF-8"));

        std::fs::remove_dir_all(root).unwrap();
    }
}
