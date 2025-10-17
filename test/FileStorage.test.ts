import { FileSystemDeltaStorage } from '../src/delta-strategy/file/storage/FileStorage';
import { StreamProvider } from '../src/delta-strategy/file/storage/StreamProvider';
import { FieldSet } from '../src/InputTypes';
import { Readable, Writable } from 'stream';
import { EventEmitter } from 'events';

// Mock StreamProvider for testing FileSystemDeltaStorage
class MockFileSystemStreamProvider extends EventEmitter implements StreamProvider {
  private storage: Map<string, string> = new Map();
  private errors: Map<string, Error> = new Map();

  setError(operation: string, error: Error): void {
    this.errors.set(operation, error);
  }

  clearErrors(): void {
    this.errors.clear();
  }

  async createReadStream(resourcePath: string): Promise<Readable | null> {
    if (this.errors.has('createReadStream')) {
      throw this.errors.get('createReadStream');
    }
    
    if (!this.storage.has(resourcePath)) {
      return null;
    }

    const data = this.storage.get(resourcePath)!;
    const mockProvider = this;
    
    const readable = new Readable({
      read() {
        if (mockProvider.errors.has('readline')) {
          // Emit error instead of pushing data
          this.emit('error', mockProvider.errors.get('readline'));
        } else {
          this.push(data);
          this.push(null); // End stream
        }
      }
    });

    return readable;
  }

  async createWriteStream(resourcePath: string): Promise<Writable> {
    if (this.errors.has('createWriteStream')) {
      throw this.errors.get('createWriteStream');
    }

    const chunks: string[] = [];
    const mockProvider = this;
    
    const writable = new Writable({
      write(chunk, _encoding, callback) {
        if (mockProvider.errors.has('write')) {
          callback(mockProvider.errors.get('write'));
          return;
        }
        chunks.push(chunk.toString());
        callback();
      }
    });

    // Override the end method to trigger finish after data is stored
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

    // Simulate write stream error if requested
    if (this.errors.has('writeStream')) {
      setImmediate(() => {
        writable.emit('error', this.errors.get('writeStream'));
      });
    }

    return writable;
  }

  async moveResource(sourcePath: string, destinationPath: string): Promise<void> {
    if (this.errors.has('moveResource')) {
      throw this.errors.get('moveResource');
    }
    
    if (this.storage.has(sourcePath)) {
      const data = this.storage.get(sourcePath)!;
      this.storage.set(destinationPath, data);
      this.storage.delete(sourcePath);
    }
  }

  async deleteResource(resourcePath: string): Promise<void> {
    if (this.errors.has('deleteResource')) {
      throw this.errors.get('deleteResource');
    }
    this.storage.delete(resourcePath);
  }

  async resourceExists(resourcePath: string): Promise<boolean> {
    if (this.errors.has('resourceExists')) {
      throw this.errors.get('resourceExists');
    }
    return this.storage.has(resourcePath);
  }

  async ensureParent(_resourcePath: string): Promise<void> {
    if (this.errors.has('ensureParent')) {
      throw this.errors.get('ensureParent');
    }
    // No-op for mock
  }

  // Test helper methods
  setStoredData(resourcePath: string, data: string): void {
    this.storage.set(resourcePath, data);
  }

  getStoredData(resourcePath: string): string | undefined {
    return this.storage.get(resourcePath);
  }

  getAllStoredPaths(): string[] {
    return Array.from(this.storage.keys());
  }

  clear(): void {
    this.storage.clear();
    this.errors.clear();
  }
}

describe('FileSystemDeltaStorage', () => {
  let storage: FileSystemDeltaStorage;
  let mockProvider: MockFileSystemStreamProvider;
  const testStoragePath = '/test/storage/path';
  const testClientId = 'test-client-001';
  
  // Sample test data
  const testFieldSets: FieldSet[] = [
    {
      fieldValues: [
        { id: 1 },
        { fullname: 'Alice Anderson' },
        { dob: '1990-01-01T00:00:00.000Z' }
      ],
      hash: 'hash-alice'
    },
    {
      fieldValues: [
        { id: 2 },
        { fullname: 'Bob Brown' },
        { dob: '1992-02-02T00:00:00.000Z' }
      ],
      hash: 'hash-bob'
    }
  ];

  beforeEach(() => {
    mockProvider = new MockFileSystemStreamProvider();
    storage = new FileSystemDeltaStorage(testStoragePath, mockProvider);
  });

  afterEach(() => {
    mockProvider.clear();
  });

  describe('constructor', () => {
    it('should create storage instance with valid path', () => {
      expect(storage.name).toBe('File System Delta Storage');
      expect(storage.description).toContain('streaming I/O');
    });

    it('should throw error with empty storage path', () => {
      expect(() => {
        new FileSystemDeltaStorage('');
      }).toThrow('Storage path is required');
    });
  });

  // Note: FileDeltaStorage no longer has storeCurrentData method
  // This functionality is handled by the delta computation engine

  describe('fetchPreviousData', () => {
    it('should successfully read field sets from NDJSON file', async () => {
      // Manually set up test data in the previous input file
      const ndjsonData = testFieldSets.map(fs => JSON.stringify(fs)).join('\n');
      mockProvider.setStoredData(`${testClientId}/previous-input.ndjson`, ndjsonData);
      
      const result = await storage.fetchPreviousData({ clientId: testClientId });
      
      expect(result).toEqual(testFieldSets);
    });

    it('should return empty array when file does not exist', async () => {
      const result = await storage.fetchPreviousData({ clientId: testClientId });
      
      expect(result).toEqual([]);
    });

    it('should handle malformed JSON lines gracefully', async () => {
      // Set up malformed NDJSON data
      mockProvider.setStoredData(`${testClientId}/previous-input.ndjson`, '{"valid": "json"}\n{"invalid": json}\n');
      
      const fetchPromise = storage.fetchPreviousData({ clientId: testClientId });
      
      await expect(fetchPromise).rejects.toThrow('Failed to parse NDJSON line:');
    });

    it('should handle readline errors', async () => {
      mockProvider.setStoredData(`${testClientId}/previous-input.ndjson`, '{"test": "data"}');
      mockProvider.setError('readline', new Error('Read error'));
      
      const fetchPromise = storage.fetchPreviousData({ clientId: testClientId });
      
      await expect(fetchPromise).rejects.toThrow('Failed to read NDJSON stream: Error: Read error');
    });

    it('should require clientId', async () => {
      await expect(storage.fetchPreviousData({ clientId: '' }))
        .rejects.toThrow('clientId is required for fetchPreviousData');
    });
  });

  describe('updatePreviousData', () => {
    it('should successfully update previous data when new data provided', async () => {
      const result = await storage.updatePreviousData({ clientId: testClientId, newPreviousData: testFieldSets });

      expect(result.status).toBe('success');
      expect(result.message).toContain('Updated previous input');
      
      // Verify data was stored as previous
      const storedData = mockProvider.getStoredData(`${testClientId}/previous-input.ndjson`);
      expect(storedData).toBeDefined();
      
      const lines = storedData!.trim().split('\n');
      expect(lines).toHaveLength(2);
      
      const parsedData = lines.map(line => JSON.parse(line));
      expect(parsedData).toEqual(testFieldSets);
    });

    it('should handle case when no new data provided (cleanup previous)', async () => {
      // Create some existing previous data
      mockProvider.setStoredData(`${testClientId}/previous-input.ndjson`, 'old test data');
      
      const result = await storage.updatePreviousData({ clientId: testClientId, newPreviousData: [] });

      expect(result.status).toBe('success');
      expect(result.message).toContain('Cleaned up previous input');
      
      // Previous should be cleaned up
      expect(await mockProvider.resourceExists(`${testClientId}/previous-input.ndjson`)).toBe(false);
    });

    it('should require clientId', async () => {
      await expect(storage.updatePreviousData({ clientId: '', newPreviousData: [] }))
        .rejects.toThrow('clientId is required for updatePreviousData');
    });
  });

  // Note: FileDeltaStorage no longer has fetchDelta method
  // This functionality is handled by the delta computation engine

  describe('Integration workflow', () => {
    it('should handle complete workflow with multiple data updates', async () => {
      // First cycle: update previous data
      await storage.updatePreviousData({ clientId: testClientId, newPreviousData: [testFieldSets[0]] });
      
      // Verify we can fetch the previous data
      const previousData = await storage.fetchPreviousData({ clientId: testClientId });
      expect(previousData).toEqual([testFieldSets[0]]);
      
      // Second cycle: update with new data
      await storage.updatePreviousData({ clientId: testClientId, newPreviousData: [testFieldSets[1]] });
      
      // Now previous should be the second data
      const newPreviousData = await storage.fetchPreviousData({ clientId: testClientId });
      expect(newPreviousData).toEqual([testFieldSets[1]]);
    });

    it('should handle large datasets efficiently', async () => {
      // Create a larger dataset to test streaming
      const largeData: FieldSet[] = [];
      for (let i = 0; i < 1000; i++) {
        largeData.push({
          fieldValues: [
            { id: i, name: `User${i}`, email: `user${i}@example.com` }
          ],
          hash: `hash-${i}`
        });
      }

      // Update previous data with large dataset
      const updateResult = await storage.updatePreviousData({ clientId: 'large-client', newPreviousData: largeData });
      expect(updateResult.status).toBe('success');

      // Fetch and verify
      const fetchedData = await storage.fetchPreviousData({ clientId: 'large-client' });
      
      expect(fetchedData).toHaveLength(1000);
      expect(fetchedData[0]).toEqual(largeData[0]);
      expect(fetchedData[999]).toEqual(largeData[999]);
    });
  });
});