import { S3BucketDeltaStorage } from '../src/delta-strategy/file/storage/S3Storage';
import { StreamProvider } from '../src/delta-strategy/file/storage/StreamProvider';
import { FieldSet } from '../src/InputTypes';
import { Readable, Writable } from 'stream';
import { EventEmitter } from 'events';

// Mock StreamProvider for testing S3BucketDeltaStorage
class MockS3StreamProvider extends EventEmitter implements StreamProvider {
  private storage: Map<string, string> = new Map();

  async createReadStream(resourcePath: string): Promise<Readable | null> {
    if (!this.storage.has(resourcePath)) {
      return null;
    }

    const data = this.storage.get(resourcePath)!;
    const readable = new Readable({
      read() {
        this.push(data);
        this.push(null); // End stream
      }
    });

    return readable;
  }

  async createWriteStream(resourcePath: string): Promise<Writable> {
    const chunks: string[] = [];
    const mockProvider = this; // Capture reference to avoid `this` binding issues
    
    const writable = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(chunk.toString());
        callback();
      }
    });

    // Override the end method to trigger finish after data is stored
    const originalEnd = writable.end.bind(writable);
    writable.end = function(chunk?: any) {
      if (chunk) {
        chunks.push(chunk.toString());
      }
      // Store data before emitting finish
      mockProvider.storage.set(resourcePath, chunks.join(''));
      // Use setImmediate to emit finish asynchronously
      setImmediate(() => {
        writable.emit('finish');
      });
      return writable;
    };

    return writable;
  }

  async moveResource(sourcePath: string, destinationPath: string): Promise<void> {
    if (this.storage.has(sourcePath)) {
      const data = this.storage.get(sourcePath)!;
      this.storage.set(destinationPath, data);
      this.storage.delete(sourcePath);
    }
  }

  async deleteResource(resourcePath: string): Promise<void> {
    this.storage.delete(resourcePath);
  }

  async resourceExists(resourcePath: string): Promise<boolean> {
    return this.storage.has(resourcePath);
  }

  async ensureParent(_resourcePath: string): Promise<void> {
    // No-op for mock (S3 doesn't require directory creation)
  }

  // Test helper methods
  getStoredData(resourcePath: string): string | undefined {
    return this.storage.get(resourcePath);
  }

  getAllStoredPaths(): string[] {
    return Array.from(this.storage.keys());
  }

  clear(): void {
    this.storage.clear();
  }
}

describe('S3BucketDeltaStorage', () => {
  let deltaStorage: S3BucketDeltaStorage;
  let mockProvider: MockS3StreamProvider;

  beforeEach(() => {
    mockProvider = new MockS3StreamProvider();
    // Use mock provider instead of real S3StreamProvider
    deltaStorage = new S3BucketDeltaStorage({
      bucketName: 'test-bucket', 
      keyPrefix: 'test-prefix/', 
      s3Config: {}, 
      streamProvider: mockProvider
    });
  });

  afterEach(() => {
    mockProvider.clear();
  });

  describe('Constructor', () => {
    it('should throw error when bucket name is empty', () => {
      expect(() => {
        new S3BucketDeltaStorage({ bucketName: '', keyPrefix: '', s3Config: {}, streamProvider: mockProvider });
      }).toThrow('S3 bucket name is required');
    });

    it('should create instance with valid bucket name', () => {
      const storage = new S3BucketDeltaStorage({
        bucketName: 'valid-bucket', 
        keyPrefix: 'prefix/', 
        s3Config: {}, 
        streamProvider: mockProvider
      });
      expect(storage.name).toBe('S3 Bucket Delta Storage');
      expect(storage.description).toContain('S3');
    });
  });

  describe('fetchPreviousData', () => {
    it('should throw error for empty clientId', async () => {
      await expect(deltaStorage.fetchPreviousData({ clientId: '' })).rejects.toThrow(
        'clientId is required for fetchPreviousData'
      );
    });

    it('should return empty array when no previous data exists', async () => {
      const result = await deltaStorage.fetchPreviousData({ clientId: 'client1' });
      expect(result).toEqual([]);
    });

    it('should return previous data when it exists', async () => {
      const testData: FieldSet[] = [
        { 
          fieldValues: [
            { id: '1', name: 'John', email: 'john@example.com' }
          ],
          hash: 'hash1'
        },
        { 
          fieldValues: [
            { id: '2', name: 'Jane', email: 'jane@example.com' }
          ],
          hash: 'hash2'
        }
      ];

      // Use updatePreviousData to set up test data
      await deltaStorage.updatePreviousData({ clientId: 'client1', newPreviousData: testData });

      // Now fetch it as previous data
      const result = await deltaStorage.fetchPreviousData({ clientId: 'client1' });
      expect(result).toEqual(testData);
    });
  });

  // Note: FileDeltaStorage no longer has storeCurrentData method
  // This functionality is handled by the delta computation engine

  describe('updatePreviousData', () => {
    it('should throw error for empty clientId', async () => {
      await expect(deltaStorage.updatePreviousData({ clientId: '', newPreviousData: [] })).rejects.toThrow(
        'clientId is required for updatePreviousData'
      );
    });

    it('should handle case when no new data provided (cleanup previous)', async () => {
      const result = await deltaStorage.updatePreviousData({ clientId: 'client1', newPreviousData: [] });
      
      expect(result.status).toBe('success');
      expect(result.message).toContain('Cleaned up previous input');
      expect(result.storage).toBe('s3');
    });

    it('should update previous data successfully with new data', async () => {
      const testData: FieldSet[] = [
        { 
          fieldValues: [
            { id: '1', name: 'John', email: 'john@example.com' }
          ],
          hash: 'hash1'
        }
      ];

      // Update previous data
      const result = await deltaStorage.updatePreviousData({ clientId: 'client1', newPreviousData: testData });
      
      expect(result.status).toBe('success');
      expect(result.message).toContain('Updated previous input');

      // Verify data was stored as previous
      expect(await mockProvider.resourceExists('client1/previous-input.ndjson')).toBe(true);
      
      // Fetch and verify the data
      const fetchedData = await deltaStorage.fetchPreviousData({ clientId: 'client1' });
      expect(fetchedData).toEqual(testData);
    });
  });

  // Note: FileDeltaStorage no longer has fetchDelta method
  // This functionality is handled by the delta computation engine

  describe('Integration workflow', () => {
    it('should handle complete workflow with multiple clients', async () => {
      const client1Data: FieldSet[] = [
        { 
          fieldValues: [
            { id: '1', name: 'Alice', email: 'alice@example.com' }
          ],
          hash: 'alice-hash'
        }
      ];
      const client2Data: FieldSet[] = [
        { 
          fieldValues: [
            { id: '1', name: 'Bob', email: 'bob@example.com' },
            { id: '2', name: 'Charlie', email: 'charlie@example.com' }
          ],
          hash: 'bob-charlie-hash'
        }
      ];

      // Update previous data for both clients
      await deltaStorage.updatePreviousData({ clientId: 'client1', newPreviousData: client1Data });
      await deltaStorage.updatePreviousData({ clientId: 'client2', newPreviousData: client2Data });

      // Verify both previous files exist
      expect(await mockProvider.resourceExists('client1/previous-input.ndjson')).toBe(true);
      expect(await mockProvider.resourceExists('client2/previous-input.ndjson')).toBe(true);

      // Fetch previous data for client1
      const previousData = await deltaStorage.fetchPreviousData({ clientId: 'client1' });
      expect(previousData).toEqual(client1Data);

      // Fetch previous data for client2
      const client2Previous = await deltaStorage.fetchPreviousData({ clientId: 'client2' });
      expect(client2Previous).toEqual(client2Data);
      
      // Clear client1 previous data
      await deltaStorage.updatePreviousData({ clientId: 'client1', newPreviousData: [] });
      expect(await mockProvider.resourceExists('client1/previous-input.ndjson')).toBe(false);
      
      // Client2 should still have its data
      const client2StillThere = await deltaStorage.fetchPreviousData({ clientId: 'client2' });
      expect(client2StillThere).toEqual(client2Data);
    });

    it('should handle large datasets efficiently', async () => {
      // Create a larger dataset to test streaming
      const largeData: FieldSet[] = [];
      for (let i = 0; i < 1000; i++) {
        largeData.push({
          fieldValues: [
            {
              id: i.toString(),
              name: `User${i}`,
              email: `user${i}@example.com`,
              score: Math.random() * 100
            }
          ],
          hash: `hash-${i}`
        });
      }

      // Update previous data with large dataset
      const updateResult = await deltaStorage.updatePreviousData({ clientId: 'large-client', newPreviousData: largeData });
      expect(updateResult.status).toBe('success');

      // Fetch and verify all data
      const fetchedData = await deltaStorage.fetchPreviousData({ clientId: 'large-client' });
      expect(fetchedData).toHaveLength(1000);
      expect(fetchedData[0]).toEqual(largeData[0]);
      expect(fetchedData[999]).toEqual(largeData[999]);
    });
  });
});