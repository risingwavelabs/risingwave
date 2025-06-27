use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinEncodingType {
    #[default]
    MemoryOptimized = 1,
    CPUOptimized = 2,
}

impl FromStr for JoinEncodingType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("memoryoptimized") {
            Ok(Self::MemoryOptimized)
        } else if s.eq_ignore_ascii_case("cpuoptimized") {
            Ok(Self::CPUOptimized)
        } else {
            Err("expect one of [MemoryOptimized, CPUOptimized]")
        }
    }
}

impl std::fmt::Display for JoinEncodingType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryOptimized => write!(f, "MemoryOptimized"),
            Self::CPUOptimized => write!(f, "CPUOptimized"),
        }
    }
}
