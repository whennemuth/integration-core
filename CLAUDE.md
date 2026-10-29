# integration-core: Shared Abstract Patterns

## Project Purpose
Foundation library exporting abstract base patterns and utilities used by all integration projects. Provides the common layer for delta synchronization, configuration management, environment variable isolation, and data transformation.

## Repository Relationship Model

This project is an independently versioned npm package with its own source repository.

In this workspace, each top-level project has its own repository and package boundary. The packages are composed through dependency relationships rather than workspace-level source control.

`integration-core` is the shared foundation package consumed by other repositories such as `integration-huron-person` and `integration-huron-person-fargate`.

## Shared Skills Repository

Cross-repository agent skills are maintained in a separate repository (https://github.com/whennemuth/integration-skills) that is cloned as the `.claude` directory of the workspace root, so skills live at `<workspace root>/.claude/skills/`. This is a native skill location that both VS Code (GitHub Copilot) and Claude Code discover without any custom settings.

- **Install**: from the workspace root, run `git clone https://github.com/whennemuth/integration-skills.git .claude`.
- **VS Code**: include the workspace root itself (`"path": "."`) as a root folder in your `.code-workspace` file. VS Code discovers `.claude/skills/` in every workspace root folder and makes those skills available across the whole workspace.
- **Claude Code**: start Claude Code in the workspace root. When starting in a package repository instead, add the workspace root with `--add-dir` (e.g. `claude --add-dir ..` from the repository root, or `/add-dir` in a running session).

See `<workspace root>/.claude/README.md` for full setup details.

Core-only and core+person+fargate workspace examples are documented in this repository's `README.md`.

## Workspace Instructions

Workspace-wide agent instructions live in `<workspace root>/.claude/CLAUDE.md`, versioned in the integration-skills repository alongside the shared skills. That file holds the workspace architecture overview, pointers to the shared-pattern skills and project CLAUDE.md files, change and commit preferences, and the task verification protocol (build, test, and completion reporting requirements for all code implementation tasks) that apply to all integration projects (core, huron-person, fargate, dashboard, file-drop, etc.).

**Discovery**: Claude Code and GitHub Copilot CLI load it automatically when started in the workspace root. VS Code Copilot Chat loads it when the workspace root (`"path": "."`) is a root folder of the `.code-workspace` file and the `chat.useClaudeMdFile` setting is enabled. When starting Claude Code in a package repository with `--add-dir` pointing at the workspace root, also set `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` so the workspace instructions are loaded.

## Implementation Verification Protocol

**CRITICAL**: When implementing code that depends on unfamiliar abstractions, control flow directives, or domain-specific patterns, you MUST verify their actual behavior before proceeding.

### High-Risk Abstractions Requiring Verification

- **Control flow directives**: `__arrayFieldOperations`, `__metadata`, behavioral flags
- **Update semantics**: append vs replace, merge vs overwrite patterns
- **Authentication patterns**: token refresh mechanisms, credential chains
- **Data transformation pipelines**: mapping strategies, field filters
- **Sync coordination**: delta strategies, hash comparison logic

### Mandatory Verification Steps

Before implementing code that uses an unfamiliar abstraction:

1. **Search for definition**: Use `grep_search` to find where it's defined
2. **Find consumers**: Search for where it's processed/interpreted
3. **Read usage examples**: Look at tests and similar patterns
4. **State your understanding**: Explicitly describe what you think it does
5. **Think through interactions**: Consider edge cases and combinations
6. **Only then implement**: Proceed with verified understanding

### When You're Uncertain

If you cannot fully verify an abstraction's behavior:

- **State explicitly what you don't know**
- **Ask whether to search for implementation first**
- **Do NOT proceed on "educated guesses"**

### User Override

You can skip verification by saying:
- "Skip verification and proceed"
- "Use inference for this"

**See Also**: `verify-abstractions-before-implementation` skill in workspace skills repository

## Key Exports

### TestEnvironment (Environment Variable Isolation Utility)
**Purpose**: Prefix-based environment variable loading with fallback chain.

**Export Location**: `src/TestEnvironment.ts`

**Usage Pattern**:
```typescript
import { TestEnvironment } from 'integration-core';

const env = new TestEnvironment('MY_PREFIX');
const value = env.getVar('VARIABLE_NAME'); // Loads MY_PREFIX_VARIABLE_NAME, falls back to VARIABLE_NAME
```

**Configuration** (via .env):
```
# Shared variables (no prefix)
DATASOURCE_BASE_URL=https://...
DATASOURCE_API_KEY=key

# Prefixed for specific harnesses
MY_PREFIX_TIMEOUT=30000
MY_PREFIX_BATCH_SIZE=200
```

**Design Principle**: Exemption rule for DATASOURCE_* and DATATARGET_* (always unprefixed, shared across harnesses)

**See Also**: Workspace `.claude/CLAUDE.md` "TestEnvironment Pattern" section

### Delta Engines
**Location**: `src/Delta*.ts` files

Provide abstract synchronization logic for incremental data updates using the three-phase pipeline.

### DataSource / DataTarget Base Classes
**Location**: `src/DataSource.ts`, `src/DataTarget.ts`

Abstract interfaces for:
- Data source consumption (API, file, database queries)
- Data target operations (CRUD, bulk updates, deactivation)
- Authentication credential management (API Key vs JWT Token)

### Utilities
- **Timer**: Performance instrumentation
- **InputParser**: Configuration and command-line argument parsing
- **Hash**: Data consistency verification

## Test Harnesses (3 total)

Located in `bin/` directory. Use TestEnvironment for harness-specific configuration:

1. **Progress**: Progress tracking utility validation
2. **Timer**: Performance measurement validation
3. **SqlLiteDb**: SQLite connectivity validation

**Execution**:
- VS Code: F5 with "Debug current file" configuration
- Command line: `npx ts-node bin/module-name.ts`

## Dependencies
- None outside Node ecosystem
- Basis for huron-person and huron-person-fargate dependencies

## Key Patterns to Avoid
1. **Scattered configuration**: All harnesses must use TestEnvironment pattern, no direct `process.env` access in harness blocks
2. **Mixed prefixing**: Never prefix DATASOURCE_* or DATATARGET_* variables
3. **Key duplication**: Use exemption rule to maintain single source of truth

