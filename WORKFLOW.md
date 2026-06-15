# VeloDB Development Workflow

## Git Commit Rules (STRICT)

1. **Every commit author metadata must be the developer only.** The author and committer name must always be set to the developer's identity. No commit shall ever include "command-code", "CommandCodeBot", "co-authored-by", or any tool/assistant attribution in any form — not in author name, committer name, commit messages, trailers, or co-author lines.

2. **Auto-commit after every completed phase.** When a phase is complete (all files for that phase written and verified), stage all changes (`git add`) and create a single commit. Do not batch multiple phases into one commit.

3. **Commit message format:** Start with the phase number, followed by a concise description of what was implemented.

   ```
   Phase 1: Core VeloDB server with RESP2 protocol, in-memory store, and 30+ commands
   ```

## Copyright

Every source file created or modified must include a copyright header:

```
// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT
```

For markdown documentation files:

```
<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->
```

## CHANGELOG.md

All changes must be tracked in `CHANGELOG.md` at the repository root. Format:

```
## [Unreleased]
### Added
- Description of what was added

### Changed
- Description of what changed

### Fixed
- Description of what was fixed
```

Each phase completion updates the changelog with all items from that phase, then moves `[Unreleased]` content under a versioned heading.

## Reference Documents

All implementation work must reference and follow the project's design documents:

| Document | Purpose |
|----------|---------|
| `docs/PRD.md` | Product requirements, user stories, functional/non-functional requirements |
| `docs/TRD.md` | Technical requirements, architecture, component specs, data flow |
| `docs/implementation-plan.md` | 7-phase plan with component breakdown and timeline |
| `docs/app-flow.md` | Application flow diagrams for all major workflows |
| `docs/backend-schema.md` | Data structures, persistence schemas, cluster schemas |
| `docs/ui-ux-design.md` | CLI design, metrics, logging, configuration UX |

Before implementing any feature, review the relevant sections in these documents. Implementation must align with the designs specified there.

## Review Points

At decision points (architecture choices, protocol changes, significant refactors), pause and ask for review before proceeding. Key decision points include:

- Adding new dependencies
- Changing the wire protocol or command semantics
- Modifying the storage trait or data model
- Security-sensitive changes
- Cross-phase interactions
