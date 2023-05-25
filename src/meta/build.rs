use std::error::Error;

use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    EmitBuilder::builder()
        .git_branch()
        .git_sha(true)
        // date when it is built
        .build_date()
        .quiet()
        .emit()?;
    Ok(())
}
