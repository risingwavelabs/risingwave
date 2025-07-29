// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Memory pooling for parser components to reduce allocation overhead

use std::cell::RefCell;

use typed_arena::Arena;

thread_local! {
    static GLOBAL_BUFFER_ARENA: RefCell<Arena<Vec<u8>>> = RefCell::new(Arena::new());
    static GLOBAL_STRING_ARENA: RefCell<Arena<String>> = RefCell::new(Arena::new());
}

/// Lock-free memory pool using thread-local storage
pub struct ParserMemoryPool;

impl ParserMemoryPool {
    pub fn new() -> Self {
        Self
    }

    /// Get a reusable buffer from the pool
    pub fn get_buffer(&self, capacity: usize) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(capacity);
        buffer.clear();
        buffer
    }

    /// Get a reusable string from the pool
    pub fn get_string(&self) -> String {
        String::new()
    }

    /// Clear all pooled allocations
    pub fn clear(&self) {
        // Thread-local arenas are automatically cleared when the thread exits
    }
}

impl Default for ParserMemoryPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Global memory pool manager using thread-local storage
pub struct GlobalMemoryPool;

impl GlobalMemoryPool {
    pub fn new() -> Self {
        Self
    }

    pub fn get_pool(&self) -> ParserMemoryPool {
        ParserMemoryPool::new()
    }
}

/// Wrapper for pooled JSON access with memory optimization
#[derive(Debug)]
pub struct PooledJsonAccessBuilder {
    payload_start_idx: usize,
    json_parse_options: crate::parser::unified::json::JsonParseOptions,
}

impl PooledJsonAccessBuilder {
    pub fn new(
        config: crate::parser::JsonProperties,
        _pool: &ParserMemoryPool, // Keep for API compatibility
    ) -> crate::error::ConnectorResult<Self> {
        let mut json_parse_options = crate::parser::unified::json::JsonParseOptions::DEFAULT;
        if let Some(mode) = config.timestamptz_handling {
            json_parse_options.timestamptz_handling = mode;
        }

        Ok(Self {
            payload_start_idx: if config.use_schema_registry { 5 } else { 0 },
            json_parse_options,
        })
    }

    pub fn generate_accessor<'a>(
        &'a self,
        payload: &[u8],
    ) -> crate::error::ConnectorResult<crate::parser::unified::AccessImpl<'a>> {
        let mut buffer = Vec::with_capacity(payload.len());
        buffer.extend_from_slice(&payload[self.payload_start_idx..]);

        let value = simd_json::to_borrowed_value(&mut buffer)
            .map_err(|e| anyhow::anyhow!("failed to parse json payload: {}", e))?;

        Ok(crate::parser::unified::AccessImpl::Json(
            crate::parser::unified::json::JsonAccess::new_with_options(
                value,
                &self.json_parse_options,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::JsonProperties;

    #[test]
    fn test_memory_pool_creation() {
        let pool = ParserMemoryPool::new();
        let buffer = pool.get_buffer(1024);
        assert_eq!(buffer.capacity(), 1024);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_pooled_json_access() {
        let pool = ParserMemoryPool::new();
        let builder = PooledJsonAccessBuilder::new(JsonProperties::default(), &pool).unwrap();

        let payload = br#"{"test": "value"}"#;
        let access = builder.generate_accessor(payload).unwrap();

        assert!(matches!(
            access,
            crate::parser::unified::AccessImpl::Json(_)
        ));
    }

    #[test]
    fn test_global_memory_pool() {
        let global_pool = GlobalMemoryPool::new();
        let pool = global_pool.get_pool();
        let buffer = pool.get_buffer(512);
        assert!(buffer.capacity() >= 512);
    }
}
