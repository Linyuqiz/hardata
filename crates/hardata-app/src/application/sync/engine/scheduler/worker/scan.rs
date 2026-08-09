impl SyncScheduler {
    async fn list_directory_recursive(
        &self,
        root_path: &str,
        region: &str,
        filter: &ScanFilter,
    ) -> Result<RemoteScanResult> {
        if filter.excludes(root_path) {
            info!(operation = "scan.root_skipped", root_path = %root_path, region = %region, reason = "root_excluded", "remote scan skipped");
            return Ok(RemoteScanResult {
                files: Vec::new(),
                source_is_single_file: false,
                root_excluded: true,
            });
        }

        let mut all_files = Vec::new();
        let mut dirs_to_scan = vec![root_path.to_string()];
        let mut is_single_file = false;

        while let Some(current_dir) = dirs_to_scan.pop() {
            debug!(
                operation = "scan.remote_path_started",
                path = %current_dir,
                region = %region,
                "remote path scan started"
            );

            let list_response = self.list_directory_once(&current_dir, region).await?;

            if let Some(file_info) =
                single_file_root_candidate(root_path, &current_dir, &list_response.files)
            {
                if self.root_path_is_single_file(root_path, region).await? {
                    is_single_file = true;
                    if filter.should_include_file(root_path) {
                        all_files.push(ScannedFile {
                            path: PathBuf::from(root_path),
                            size: file_info.size,
                            modified: file_info.modified,
                            change_time: file_info.change_time,
                            inode: file_info.inode,
                            is_dir: false,
                            mode: file_info.mode,
                            is_symlink: file_info.is_symlink,
                            symlink_target: file_info.symlink_target.clone(),
                        });
                    }
                    continue;
                }
            }

            for file_info in list_response.files {
                let full_path = format!("{}/{}", current_dir.trim_end_matches('/'), file_info.path);

                if file_info.is_directory {
                    if filter.should_include_dir(&full_path) {
                        all_files.push(ScannedFile {
                            path: PathBuf::from(&full_path),
                            size: 0,
                            modified: file_info.modified,
                            change_time: file_info.change_time,
                            inode: file_info.inode,
                            is_dir: true,
                            mode: file_info.mode,
                            is_symlink: false,
                            symlink_target: None,
                        });
                    }
                    if filter.should_scan_dir(&full_path) {
                        dirs_to_scan.push(full_path);
                    }
                } else if filter.should_include_file(&full_path) {
                    all_files.push(ScannedFile {
                        path: PathBuf::from(&full_path),
                        size: file_info.size,
                        modified: file_info.modified,
                        change_time: file_info.change_time,
                        inode: file_info.inode,
                        is_dir: false,
                        mode: file_info.mode,
                        is_symlink: file_info.is_symlink,
                        symlink_target: file_info.symlink_target.clone(),
                    });
                }
            }
        }

        info!(
            operation = "scan.remote_completed",
            region = %region,
            root_path = %root_path,
            file_count = all_files.len(),
            source_is_single_file = is_single_file,
            "recursive remote scan completed"
        );
        Ok(RemoteScanResult {
            files: all_files,
            source_is_single_file: is_single_file,
            root_excluded: false,
        })
    }
}

fn single_file_root_candidate<'a>(
    root_path: &str,
    current_dir: &str,
    files: &'a [crate::domain::FileInfo],
) -> Option<&'a crate::domain::FileInfo> {
    if current_dir != root_path || files.len() != 1 {
        return None;
    }

    let file_info = &files[0];
    if file_info.is_directory {
        return None;
    }

    let root_file_name = std::path::Path::new(root_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    (file_info.path == root_file_name).then_some(file_info)
}

fn parent_lookup_path(root_path: &str) -> Option<String> {
    let normalized_root = root_path.trim_end_matches('/');
    let root = Path::new(normalized_root);
    let parent = root.parent()?;
    if parent.as_os_str().is_empty() {
        Some(".".to_string())
    } else {
        Some(parent.to_string_lossy().to_string())
    }
}

fn parent_listing_confirms_single_file(
    root_path: &str,
    parent_files: &[crate::domain::FileInfo],
) -> bool {
    let root_name = Path::new(root_path.trim_end_matches('/'))
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    parent_files
        .iter()
        .any(|entry| entry.path == root_name && !entry.is_directory)
}

fn compile_patterns(patterns: &[String], field: &str) -> Result<Vec<Regex>> {
    patterns
        .iter()
        .map(|pattern| {
            Regex::new(pattern).map_err(|e| {
                HarDataError::InvalidConfig(format!(
                    "Invalid {} pattern '{}': {}",
                    field, pattern, e
                ))
            })
        })
        .collect()
}
