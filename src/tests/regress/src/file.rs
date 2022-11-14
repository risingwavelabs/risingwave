// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::OsStr;
use std::fs::{create_dir_all, read_dir, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use tracing::info;

use crate::Opts;

pub(crate) struct FileManager {
    opts: Opts,
}

impl FileManager {
    pub(crate) fn new(opts: Opts) -> Self {
        Self { opts }
    }

    /// Initialize file related stuff.
    ///
    /// - Create necessary directories.
    /// - Convert source files.
    pub(crate) fn init(&self) -> anyhow::Result<()> {
        ensure_dir(self.opts.absolutized_output_dir()?)?;
        ensure_dir(self.opts.absolutized_output_dir()?.join("results"))?;
        ensure_dir(self.opts.absolutized_output_dir()?.join("sql"))?;
        ensure_dir(self.test_table_space_dir()?)?;
        ensure_dir(self.result_dir()?)?;

        self.convert_source_files()?;
        Ok(())
    }

    /// Try to find the input file of `test_name`.
    pub(crate) fn source_of(&self, test_name: &str) -> anyhow::Result<PathBuf> {
        let mut path = self
            .opts
            .absolutized_input_dir()?
            .join("sql")
            .join(format!("{}.sql", test_name));

        if path.exists() {
            return Ok(path);
        }

        path = self
            .opts
            .absolutized_output_dir()?
            .join("sql")
            .join(format!("{}.sql", test_name));

        if path.exists() {
            return Ok(path);
        }

        bail!("Can't find source of test case: {}", test_name)
    }

    /// Try to find the output file of `test_name`.
    pub(crate) fn output_of(&self, test_name: &str) -> anyhow::Result<PathBuf> {
        Ok(self
            .opts
            .absolutized_output_dir()?
            .join("results")
            .join(format!("{}.out", test_name)))
    }

    /// Try to find the diff file of `test_name`.
    pub(crate) fn diff_of(&self, test_name: &str) -> anyhow::Result<PathBuf> {
        Ok(self
            .opts
            .absolutized_output_dir()?
            .join("results")
            .join(format!("{}.diff", test_name)))
    }

    /// Try to find the expected output file of `test_name`.
    pub(crate) fn expected_output_of(&self, test_name: &str) -> anyhow::Result<PathBuf> {
        let mut path = self
            .opts
            .absolutized_input_dir()?
            .join("expected")
            .join(format!("{}.out", test_name));

        if path.exists() {
            return Ok(path);
        }

        path = self
            .opts
            .absolutized_output_dir()?
            .join("expected")
            .join(format!("{}.sql", test_name));

        if path.exists() {
            return Ok(path);
        }

        bail!("Can't find expected output of test case: {}", test_name)
    }

    /// Convert source files in input dir, use [`Self::replace_placeholder`].
    pub(crate) fn convert_source_files(&self) -> anyhow::Result<()> {
        self.convert_source_files_internal("input", "sql", "sql")?;
        self.convert_source_files_internal("output", "expected", "out")?;
        Ok(())
    }

    /// Converts files ends with ".source" suffix in `input_subdir` and output them to
    /// `dest_subdir` with filename ends with `suffix`
    ///
    /// The `input_subdir` is relative to [`crate::Opts::input_dir`], and `output_subdir` is
    /// relative to [`crate::Opts::output_dir`].
    fn convert_source_files_internal(
        &self,
        input_subdir: &str,
        dest_subdir: &str,
        suffix: &str,
    ) -> anyhow::Result<()> {
        let output_subdir_path = self.opts.absolutized_output_dir()?.join(dest_subdir);
        ensure_dir(&output_subdir_path)?;

        let input_subdir_path: PathBuf = self.opts.absolutized_input_dir()?.join(input_subdir);

        let dir_entries = read_dir(&input_subdir_path)
            .with_context(|| format!("Failed to read dir {:?}", input_subdir_path))?;

        for entry in dir_entries {
            let path = entry?.path();
            let extension = path.extension().and_then(OsStr::to_str);

            if path.is_file() && extension == Some("source") {
                let filename = path.file_prefix().unwrap();
                let output_filename = format!("{}.{}", filename.to_str().unwrap(), suffix);
                let output_path = output_subdir_path.join(output_filename);
                info!("Converting {:?} to {:?}", path, output_path);
                self.replace_placeholder(path, output_path)?;
            } else {
                info!("Skip converting {:?}", path);
            }
        }

        Ok(())
    }

    /// Replace predefined placeholders in `input` with correct values and output them to
    /// `output`.
    ///
    /// ## Placeholders
    /// * `@abs_srcdir@`: Absolute path of input directory.
    /// * `@abs_builddir@`: Absolute path of output directory.
    /// * `@testtablespace@`: Absolute path of tablespace for test.
    fn replace_placeholder<P: AsRef<Path>>(&self, input: P, output: P) -> anyhow::Result<()> {
        let abs_input_dir = self.opts.absolutized_input_dir()?;
        let abs_output_dir = self.opts.absolutized_output_dir()?;
        let test_tablespace = self.test_table_space_dir()?;

        let reader = BufReader::new(
            File::options()
                .read(true)
                .open(&input)
                .with_context(|| format!("Failed to open input file: {:?}", input.as_ref()))?,
        );

        let mut writer = BufWriter::new(
            File::options()
                .write(true)
                .create_new(true)
                .open(&output)
                .with_context(|| format!("Failed to create output file: {:?}", output.as_ref()))?,
        );

        for line in reader.lines() {
            let mut new_line = line?;
            new_line = new_line.replace("@abs_srcdir@", abs_input_dir.to_str().unwrap());
            new_line = new_line.replace("@abs_builddir@", abs_output_dir.to_str().unwrap());
            new_line = new_line.replace("@testtablespace@", test_tablespace.to_str().unwrap());
            writer.write_all(new_line.as_bytes())?;
            writer.write_all("\n".as_bytes())?;
        }

        Ok(writer.flush()?)
    }

    fn test_table_space_dir(&self) -> anyhow::Result<PathBuf> {
        self.opts
            .absolutized_output_dir()
            .map(|p| p.join("testtablespace"))
    }

    fn result_dir(&self) -> anyhow::Result<PathBuf> {
        self.opts.absolutized_output_dir().map(|p| p.join("result"))
    }
}

/// Check `dir` not exists or is empty.
///
/// # Return
///
/// * If `dir` doesn't exist, create it and all its parents.
/// * If `dir` exits, return error if not empty.
fn ensure_dir<P: AsRef<Path>>(dir: P) -> anyhow::Result<()> {
    let dir = dir.as_ref();
    if !dir.exists() {
        create_dir_all(dir).with_context(|| format!("Failed to create dir {:?}", dir))?;
        return Ok(());
    }

    if !dir.is_dir() {
        bail!("{:?} already exists and is not a directory!", dir);
    }

    let dir_entry = read_dir(dir).with_context(|| format!("Failed to read dir {:?}", dir))?;
    if dir_entry.count() != 0 {
        bail!("{:?} is not empty!", dir);
    }

    Ok(())
}
