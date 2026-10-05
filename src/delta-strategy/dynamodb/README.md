# DynamoDB Delta Strategy

## Overview

This directory contains the DynamoDB-based delta storage implementation, which is the third delta strategy in integration-core alongside file-based and database (PostgreSQL) strategies.

## Architecture

### Two-Table Design

#### 1. PersonCurrentState (Single Record Per Person)
- **Purpose**: Track current hash state for each person
- **Access Pattern**: Batch fetch by personId for delta computation
- **Storage Mode**: Overwrite (one record per person)
- **Table Structure**:
  - PK: `personId`
  - Attributes: `hash`, `syncRunId`
  - GSI: Query by `syncRunId` for deletion detection

#### 2. PersonHistory (Append-Only Audit Trail)
- **Purpose**: Complete history of person state changes
- **Access Pattern**: Query by personId for audit trail
- **Storage Mode**: Append-only (never overwrite)
- **Table Structure**:
  - PK: `personId`, SK: `syncRunId`
  - Attributes: `hash`, `changeType`, `previousHash` (UPDATED only)
  - GSI1: Query by syncRunId (all changes in sync run)
  - GSI2: Query by changeType (specific change types across runs)

### Write Policy

- **NEW**: Write to both tables (first appearance in source)
- **UPDATED**: Write to both tables with previousHash in history
- **UNCHANGED**: Skip entirely (no writes)
- **DELETED**: Write to PersonHistory only (detected by merger)

## Key Features

### Eliminates "Maintain the Illusion" Problem

**Current File-Based Strategy:**
- Processors write chunk-specific mini-deltas to S3
- Each processor pretends to write global state but actually writes partial state
- Merger consolidates all mini-deltas into final delta
- Complex coordination with marker files

**DynamoDB Strategy:**
- Processors write directly to shared DynamoDB tables
- No mini-deltas, no marker files, no consolidation needed
- Each processor batch operation is atomic and consistent
- Merger simplified to deletion detection only

### Batch Operations

**Processors:**
1. BatchGetItem: Fetch previous hashes for chunk (up to 100 per request)
2. In-memory comparison: Compute NEW/UPDATED/UNCHANGED
3. BatchWriteItem: Update both tables (up to 25 per request)

**Merger:**
1. Query GSI: Get all personIds seen in current sync run
2. Compare with full population cache
3. Detect deletions (in cache but not in sync run)
4. Write DELETED records to PersonHistory

### Chunk-Scoped Efficiency

Processors only fetch data for personIds in their current chunk:
- 1000 persons in chunk → 10 BatchGetItem requests (100 per batch)
- Much more efficient than full table scan
- No "entire table" fetches needed

## Implementation

### DynamoDBDeltaStorage Class

Located in: `src/delta-strategy/dynamodb/storage/DynamoDBDeltaStorage.ts`

**Key Methods:**
- `fetchPreviousData()`: BatchGetItem for chunk-scoped personIds
- `updatePreviousData()`: BatchWriteItem to both tables
- `getPersonIdsForSyncRun()`: Query GSI for deletion detection

**Configuration:**
Table names passed as constructor parameters (defined in fargate project):
```typescript
const storage = new DynamoDBDeltaStorage({
  region: 'us-east-1',
  personCurrentStateTableName: '${STACK_ID}-person-current-state-${landscape}',
  personHistoryTableName: '${STACK_ID}-person-history-${landscape}',
  currentStateGSIName: 'syncRunId-personId-index'
});
```

## Integration with Fargate Project

### Table Naming (Fargate Responsibility)

Table names defined in **integration-huron-person-fargate**:
- `src/PersonCurrentStateTable.ts`: Table name generator
- `src/PersonHistoryTable.ts`: Table name generator
- `lib/DynamoDB.ts`: CDK table creation

### Entry Points (Fargate Responsibility)

Docker entry points that use DynamoDB storage:
- `docker/processor-dynamodb.ts`: Processor using DynamoDB strategy
- `docker/merger-dynamodb.ts`: Merger using DynamoDB strategy (simplified)

## Comparison with Other Strategies

| Feature | File-Based | Database (PostgreSQL) | DynamoDB |
|---------|------------|----------------------|----------|
| Previous data storage | S3 NDJSON | PostgreSQL tables | PersonCurrentState |
| History storage | None | DeltaHistory table | PersonHistory |
| Delta computation | In-memory comparison | SQL outer joins | In-memory comparison |
| Batch efficiency | Stream entire file | SQL batch operations | BatchGetItem/WriteItem |
| Merger complexity | High (file consolidation) | Medium (SQL queries) | Low (deletion detection only) |
| Coordination | Marker files | Database transactions | Atomic batch operations |
| Scalability | Lambda-friendly | Requires connection pooling | Lambda-friendly |

## Usage Example

```typescript
import { DynamoDBDeltaStorage } from 'integration-core';

// Create storage instance with table names from fargate config
const storage = new DynamoDBDeltaStorage({
  region: process.env.AWS_REGION || 'us-east-1',
  personCurrentStateTableName: getPersonCurrentStateTableName(context),
  personHistoryTableName: getPersonHistoryTableName(context),
  currentStateGSIName: 'syncRunId-personId-index'
});

// Fetch previous hashes for current chunk
const previousData = await storage.fetchPreviousData({
  clientId: 'unused',
  limitTo: currentChunkData // FieldSets with personIds
});

// Compute deltas (in-memory)
const delta = computeDelta(previousData, currentChunkData);

// Update both tables
await storage.updatePreviousData({
  clientId: 'unused',
  newPreviousData: delta.added.concat(delta.updated)
});

// Merger: Get all persons seen in sync run
const seenPersonIds = await storage.getPersonIdsForSyncRun(syncRunId);
const deletedPersons = detectDeletions(populationCache, seenPersonIds);
```

## Testing

Test harnesses should be created in **integration-huron-person-fargate** project:
- `src/DynamoDBDeltaStorageHarness.ts`: Test storage operations
- Uses TestEnvironment pattern for configuration
- Tests BatchGetItem, BatchWriteItem, Query operations

## Migration Path

1. Create DynamoDB tables in CDK (conditional on `PREVIOUS_STORAGE_TYPE === 'dynamodb'`, which is the default)
2. Implement processor-dynamodb.ts entry point
3. Test with subset of data
4. Compare results with file-based strategy
5. Gradually roll out to production
