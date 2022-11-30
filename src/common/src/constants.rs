pub mod hummock {
    use bitflags::bitflags;
    bitflags! {

        #[derive(Default)]
        pub struct CompactionFilterFlag: u32 {
            const NONE = 0b00000000;
            const STATE_CLEAN = 0b00000010;
            const TTL = 0b00000100;
        }
    }

    impl From<CompactionFilterFlag> for u32 {
        fn from(flag: CompactionFilterFlag) -> Self {
            flag.bits()
        }
    }

    pub const TABLE_OPTION_DUMMY_RETENTION_SECOND: u32 = 0;
    pub const PROPERTIES_RETENTION_SECOND_KEY: &str = "retention_seconds";
}
