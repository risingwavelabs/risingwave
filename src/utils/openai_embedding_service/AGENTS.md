# AGENTS.md - OpenAI Embedding Service Crate

## 1. Scope

Policies for the `openai_embedding_service` crate providing an OpenAI-compatible embedding HTTP service.

## 2. Purpose

The OpenAI embedding service crate provides:
- **HTTP API**: OpenAI-compatible `/v1/embeddings` endpoint
- **Model Management**: Load and serve embedding models
- **Batch Processing**: Efficient batch embedding computation
- **Test Client**: Binary for testing and benchmarking

This enables RisingWave to host embedding models for vector operations.

## 3. Structure

```
src/utils/openai_embedding_service/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Library exports
    └── bin/
        └── test_client.rs        # Test client binary
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | HTTP server, embedding handler, model registry |
| `src/bin/test_client.rs` | CLI tool for testing embeddings |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Follow OpenAI API specification for request/response formats
- Implement proper error handling with correct HTTP status codes
- Add request validation and rate limiting
- Test with actual OpenAI client libraries
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Break OpenAI API compatibility without versioning
- Hardcode model paths or configurations
- Skip input validation (protect against abuse)
- Block threads during model inference (use async)
- Log sensitive request content

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p openai_embedding_service` |
| API tests | `cargo test -p openai_embedding_service api` |
| Test client | `cargo run -p openai_embedding_service --bin test_client` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **axum**: HTTP server framework
- **async-openai**: OpenAI API types and client
- **serde**: Request/response serialization
- **serde_json**: JSON handling
- **tokio**: Async runtime
- **tracing**: Structured logging

Contracts:
- API is compatible with OpenAI `/v1/embeddings` specification
- Supports batch requests up to configured limit
- Returns proper error codes (400, 401, 429, 500)
- Model loading is lazy and cached

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- OpenAI API version changes
- New embedding model support added
- Request/response format changes
- Authentication mechanism modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md
