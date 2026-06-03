# integration-core: Shared Abstract Patterns

## Project Purpose
Foundation library exporting abstract base patterns and utilities used by all integration projects. Provides the common layer for delta synchronization, configuration management, environment variable isolation, and data transformation.

## Repository Relationship Model

This project is an independently versioned npm package with its own source repository.

In this workspace, each top-level project has its own repository and package boundary. The packages are composed through dependency relationships rather than workspace-level source control.

`integration-core` is the shared foundation package consumed by other repositories such as `integration-huron-person` and `integration-huron-person-fargate`.

## Shared Skills Repository

Cross-repository Copilot skills are maintained in a separate repository at `integration-workspace-skills/skills/`.

VS Code discovers these skills using the `chat.agentSkillsLocations` setting in your `.code-workspace` file. In multi-root `.code-workspace` configurations, `chat.agentSkillsLocations` paths are resolved relative to each workspace root folder (not from the `.code-workspace` file location).

Canonical settings entry:

```json
"chat.agentSkillsLocations": {
	"../integration-workspace-skills/skills": true
}
```

Core-only and core+person+fargate workspace examples are documented in this repository's `README.md`.

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

**See Also**: Workspace CLAUDE.md "TestEnvironment Pattern" section

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

