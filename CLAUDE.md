# VeloDB — Agent Instructions

Read this file first every session.

## Attribution — hard rule

All commits in this repo are authored by **Ashish Swain only**. Never add Claude,
Anthropic, or any AI-tool co-authorship, `Co-Authored-By` trailer, or mention of
Claude Code/AI assistance to:

- Git commit messages
- CHANGELOG.md entries
- README.md, code comments, or any other file in this repo

If an existing file already contains such a mention (e.g. a "Tooling: Claude Code"
line), remove it before committing — do not carry it forward.

## Project context

VeloDB is a Redis-protocol-compatible, high-performance in-memory database server
written in Rust (Tokio async, RESP2/RESP3, lock-free store via dashmap). See
`README.md` for architecture and the roadmap table for phase status — note the
roadmap table can lag actual implementation state; verify against `git log` and
`CHANGELOG.md` before trusting it.

## Working conventions

- Run `cargo test` (not just `cargo build`) before considering any change done —
  this repo has a large integration suite (`tests/integration.rs`) that catches
  real concurrency/replication bugs.
- The vendored LuaJIT build (`mlua` with `luajit` + `vendored` features) is fragile
  on a fully clean Windows build (missing build cache) — see CHANGELOG/session notes
  before assuming a build failure is a code regression.
- Check `git status` for uncommitted work before starting new work — this repo has
  a history of multi-phase work sitting uncommitted for a while before being reviewed
  and committed.
