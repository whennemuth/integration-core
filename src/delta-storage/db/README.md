# Database Delta Storage

This directory contains the database-centric implementation of delta storage using TypeORM with support for both PostgreSQL (production) and SQLite (testing).

## Architecture

The database delta storage follows a different paradigm from file-based storage:

- **File-based storage**: Current data comes from live DataSource, only previous data is stored
- **Database storage**: Both current and previous data are stored, deltas computed via SQL operations

## Key Components

### Entities

- **FieldSetEntity**: Stores individual field sets with hash indexing for efficient delta queries
- **DeltaHistoryEntity**: Maintains audit trail of delta computations with metadata

### Storage Implementation  

- **PostgreSQLDeltaStorage**: Main implementation of `DatabaseDeltaStorage` interface
- **DatabaseProvider**: Abstraction layer for database connections and configuration

## Features

### High Performance
- Indexed hash-based queries for fast delta computation
- SQL outer joins for efficient set operations
- Transactional operations for consistency

### Audit & Monitoring
- Complete history of delta computations
- Processing time tracking
- Record count metadata

### Flexible Configuration
- PostgreSQL for production workloads
- SQLite for development and testing
- In-memory databases for unit tests

## Usage

### Basic Setup

```typescript
import { PostgreSQLDeltaStorage, DatabaseProvider } from './db';

// PostgreSQL configuration
const config = DatabaseProvider.createPostgreSQLConfig({
  host: 'localhost',
  database: 'myapp',
  username: 'user',
  password: 'pass'
});

const storage = new PostgreSQLDeltaStorage(config);
await storage.initialize();
```

### Testing Setup

```typescript
// In-memory SQLite for fast unit tests
const config = DatabaseProvider.createInMemorySQLiteConfig();
const storage = new PostgreSQLDeltaStorage(config);
await storage.initialize();
```

### Delta Workflow

```typescript
// Store current data (automatically promotes existing current to previous)
await storage.storeCurrentData(clientId, currentFieldSets);

// Compute delta using SQL operations
const delta = await storage.fetchDelta(clientId);

// Process delta with your target system...

// Update baseline for next iteration
await storage.updatePreviousData(clientId);
```

## Database Schema

### field_sets table
- Stores both current and previous FieldSet data
- Composite indexes on (clientId, hash) and (clientId, dataType, createdAt)
- JSONB storage for flexible field structures

### delta_history table  
- Audit trail of all delta computations
- Metadata including processing times and record counts
- Useful for monitoring and troubleshooting

## SQL Delta Computation

The implementation uses efficient SQL queries:

```sql
-- Added records (in current but not in previous)
SELECT c.* FROM field_sets c
LEFT JOIN field_sets p ON c.hash = p.hash 
  AND p.client_id = ? AND p.data_type = 'previous'
WHERE c.client_id = ? AND c.data_type = 'current' 
  AND p.hash IS NULL

-- Removed records (in previous but not in current)  
SELECT p.* FROM field_sets p
LEFT JOIN field_sets c ON p.hash = c.hash 
  AND c.client_id = ? AND c.data_type = 'current'
WHERE p.client_id = ? AND p.data_type = 'previous' 
  AND c.hash IS NULL
```

## Testing

Comprehensive test suite using in-memory SQLite:
- Entity conversion and validation
- Delta computation accuracy
- Concurrent operation safety
- Error handling and edge cases

```bash
npm test -- --testNamePattern="PostgreSQLDeltaStorage"
```

## Migration to Production

1. Set up PostgreSQL database
2. Configure connection parameters
3. Enable migrations: `runMigrations: true`
4. Monitor delta history for performance insights

The database approach provides superior performance for large datasets and complex delta scenarios compared to file-based storage.