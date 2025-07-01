use clippy_utils::paths::{PathLookup, PathNS};
use rustc_span::Symbol;

pub(crate) fn path(ns: PathNS, path: &str) -> PathLookup {
    let paths = path.split("::").map(Symbol::intern).collect::<Vec<_>>();
    let paths = Box::leak(paths.into_boxed_slice());
    PathLookup::new(ns, paths)
}

/// Define paths that will be used for lookups.
///
/// For debugging, you can print out `path.get(cx)` to see if it's correctly resolved.
macro_rules! def_path_lookup {
    ($name:ident, $ns:ident, $path:literal) => {
        pub static $name: LazyLock<PathLookup> =
            LazyLock::new(|| $crate::utils::path::path(PathNS::$ns, $path));
    };
}
pub(crate) use def_path_lookup;
