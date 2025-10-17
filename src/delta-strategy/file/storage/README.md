# Delta Storage Architecture

This document describes the new streaming-based delta storage architecture that supports both file system and S3 storage backends through a unified interface.

## Architecture Overview

The delta storage system uses the Strategy pattern with dependency injection to provide storage-agnostic streaming operations. This enables efficient handling of large datasets (200K+ records) while maintaining compatibility with AWS Lambda environments.

### Key Components

1. **StreamProvider Interface** - Abstract interface for storage operations
2. **NDJSONStreamProcessor** - Generic NDJSON streaming logic
3. **FileSystemStreamProvider** - File system implementation
4. **S3StreamProvider** - S3 bucket implementation
5. **DeltaStorage Classes** - High-level storage management

## StreamProvider Interface

The `StreamProvider` interface defines storage-agnostic streaming operations:

```typescript
interface StreamProvider {
  createReadStream(resourcePath: string): Promise<Readable | null>;
  createWriteStream(resourcePath: string): Promise<Writable>;
  moveResource(sourcePath: string, destinationPath: string): Promise<void>;
  deleteResource(resourcePath: string): Promise<void>;
  resourceExists(resourcePath: string): Promise<boolean>;
  ensureParent(resourcePath: string): Promise<void>;
}
```

## Usage Examples

### File System Storage

```typescript
import { FileSystemDeltaStorage } from 'integration-core/delta-strategy/file';

// Using default FileSystemStreamProvider
const storage = new FileSystemDeltaStorage('/data/storage');

// Using custom StreamProvider
const customProvider = new FileSystemStreamProvider('/custom/path');
const storage = new FileSystemDeltaStorage('/data/storage', customProvider);

// Store current input data
await storage.storeCurrentInput('client-123', fieldSets);

// Fetch previous input data
const previousData = await storage.fetchPreviousData('client-123');

// Purge (move current to previous)
await storage.purgePreviousInput('client-123');
```

### S3 Storage (AWS Lambda)

```typescript
import { S3BucketDeltaStorage } from 'integration-core/delta-strategy/file';

// Using default S3StreamProvider
const storage = new S3BucketDeltaStorage('my-bucket', 'data-prefix/');

// Using custom S3 configuration (SDK v3)
const s3Config = {
  region: 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
};
const storage = new S3BucketDeltaStorage('my-bucket', 'data-prefix/', s3Config);

// Same API as FileSystemDeltaStorage
await storage.storeCurrentInput('client-123', fieldSets);
const previousData = await storage.fetchPreviousData('client-123');
await storage.purgePreviousInput('client-123');
```

## Performance Optimizations

### Streaming NDJSON Format

Data is stored using Newline Delimited JSON (NDJSON) for efficient streaming:

```json
{"fieldValues":[{"id":"1","name":"John"}],"hash":"abc123"}
{"fieldValues":[{"id":"2","name":"Jane"}],"hash":"def456"}
```

Benefits:
- **Memory Efficient**: Processes records one at a time
- **Streaming Compatible**: Can start processing before download completes
- **Backpressure Handling**: Automatic flow control prevents memory overflow

### Large Dataset Handling

The system efficiently handles large datasets through:

1. **Streaming I/O**: No full dataset loading into memory
2. **Backpressure**: Automatic pause/resume based on downstream capacity
3. **Incremental Processing**: Record-by-record processing
4. **Async Operations**: Non-blocking I/O operations

## AWS Lambda Compatibility

### Dependencies

```json
{
  "dependencies": {
    "integration-core": "^1.0.0"
  },
  "peerDependencies": {
    "@aws-sdk/client-s3": "^3.0.0",
    "@aws-sdk/lib-storage": "^3.0.0"
  }
}
```

Note: AWS SDK v3 requires explicit installation in Lambda environments. The modular SDK packages need to be included in your deployment package.

### Example Lambda Function

```typescript
import { S3BucketDeltaStorage } from 'integration-core/delta-strategy/file';

export const handler = async (event: any) => {
  const storage = new S3BucketDeltaStorage(
    process.env.BUCKET_NAME!,
    'integration-data/'
  );
  
  // Process delta data
  const currentData = await fetchDataFromSource();
  const previousData = await storage.fetchPreviousData(event.clientId);
  
  // Compute and apply delta
  const delta = computeDelta(currentData, previousData);
  await applyDelta(delta);
  
  // Store current data for next run
  await storage.storeCurrentInput(event.clientId, currentData);
  await storage.purgePreviousInput(event.clientId);
  
  return { statusCode: 200, body: 'Success' };
};
```

## Error Handling

All storage operations include comprehensive error handling:

```typescript
try {
  await storage.storeCurrentInput('client-123', data);
} catch (error) {
  if (error.message.includes('Failed to write NDJSON stream')) {
    // Handle write errors (disk full, permissions, etc.)
  } else if (error.message.includes('Failed to store current input')) {
    // Handle higher-level storage errors
  }
}
```

## Testing

### Mock StreamProvider

For testing, use the provided mock StreamProvider:

```typescript
import { FileSystemDeltaStorage } from 'integration-core/delta-strategy/file';

class MockStreamProvider implements StreamProvider {
  private storage = new Map<string, string>();
  
  async createReadStream(path: string): Promise<Readable | null> {
    // Mock implementation
  }
  
  // ... other methods
}

const mockProvider = new MockStreamProvider();
const storage = new FileSystemDeltaStorage('/test', mockProvider);
```

### Test Examples

```typescript
describe('DeltaStorage', () => {
  it('should handle large datasets', async () => {
    const largeData: FieldSet[] = [];
    for (let i = 0; i < 100000; i++) {
      largeData.push(createFieldSet(i));
    }
    
    await storage.storeCurrentInput('client', largeData);
    const retrieved = await storage.fetchPreviousData('client');
    
    expect(retrieved).toHaveLength(100000);
  });
});
```

## Migration Guide

### From Legacy FileStorage

Old approach:
```typescript
const storage = new FileSystemDeltaStorage('/path');
```

New approach (no changes needed):
```typescript
const storage = new FileSystemDeltaStorage('/path');
// API remains the same, implementation is now streaming-based
```

### Adding Custom Storage Backend

Implement the `StreamProvider` interface:

```typescript
class CustomStreamProvider implements StreamProvider {
  async createReadStream(resourcePath: string): Promise<Readable | null> {
    // Your implementation
  }
  
  async createWriteStream(resourcePath: string): Promise<Writable> {
    // Your implementation
  }
  
  // ... implement other methods
}

const customStorage = new FileSystemDeltaStorage(
  '/path', 
  new CustomStreamProvider()
);
```

## Best Practices

1. **Use appropriate storage for environment**:
   - File system for local development
   - S3 for AWS Lambda production

2. **Handle backpressure properly**:
   - The StreamProvider implementations handle this automatically
   - For custom providers, implement proper backpressure handling

3. **Monitor memory usage**:
   - Streaming keeps memory usage low
   - Large individual records may still cause issues

4. **Error handling**:
   - Always wrap storage operations in try-catch
   - Log errors with sufficient context for debugging

5. **Resource cleanup**:
   - StreamProviders handle resource cleanup automatically
   - Consider implementing periodic cleanup for old data

## Troubleshooting

### Common Issues

1. **Memory errors with large datasets**:
   - Verify streaming is working correctly
   - Check individual record sizes

2. **AWS SDK not found**:
   - Install SDK v3: `npm install @aws-sdk/client-s3 @aws-sdk/lib-storage`
   - In Lambda, include these packages in your deployment bundle

3. **Permission errors**:
   - Verify file system permissions
   - Check IAM permissions for S3 operations

4. **Network timeouts**:
   - Implement retry logic for S3 operations
   - Consider connection pooling for high throughput

### Debug Logging

Enable debug logging to troubleshoot streaming issues:

```typescript
// Add debugging to your StreamProvider implementation
console.log(`Creating write stream for: ${resourcePath}`);
console.log(`Data size: ${data.length} bytes`);
```