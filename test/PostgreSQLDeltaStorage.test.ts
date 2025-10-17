import { DatabaseProvider } from '../src/delta-storage/db/DatabaseProvider';
import { FieldSetEntity } from '../src/delta-storage/db/entities/FieldSetEntity';
import { PostgreSQLDeltaStorage } from '../src/delta-storage/db/PostgreSQLDeltaStorage';
import { FieldSet } from '../src/InputTypes';

describe('PostgreSQLDeltaStorage', () => {
  let deltaStorage: PostgreSQLDeltaStorage;
  let testClientId: string;
  
  // Primary key fields for testing
  const primaryKeyFields = new Set(['id', 'dob']);

  // Sample test data (reduced field sets - primary keys + hash only)
  const testFieldSets: FieldSet[] = [
    {
      fieldValues: [
        { id: 1 },
        { dob: '1990-01-01T00:00:00.000Z' }
      ],
      hash: 'hash-alice'
    },
    {
      fieldValues: [
        { id: 2 },
        { dob: '1992-02-02T00:00:00.000Z' }
      ],
      hash: 'hash-bob'
    }
  ];

  const updatedTestFieldSets: FieldSet[] = [
    {
      fieldValues: [
        { id: 1 },
        { dob: '1990-01-01T00:00:00.000Z' }
      ],
      hash: 'hash-alice-updated' // New hash due to data change
    },
    {
      fieldValues: [
        { id: 3 },
        { dob: '1988-03-03T00:00:00.000Z' }
      ],
      hash: 'hash-charlie'
    }
  ];

  beforeEach(async () => {
    // Use in-memory SQLite for fast, isolated testing
    const config = DatabaseProvider.createInMemorySQLiteConfig();
    deltaStorage = new PostgreSQLDeltaStorage(config);
    await deltaStorage.initialize();
    
    testClientId = `test-client-${Date.now()}`;
  });

  afterEach(async () => {
    await deltaStorage.close();
  });

  describe('Initialization', () => {
    it('should initialize successfully', async () => {
      expect(deltaStorage.name).toBe('PostgreSQL Delta Storage');
      expect(deltaStorage.description).toContain('PostgreSQL');
    });

    it('should throw error when operations called before initialization', async () => {
      const uninitializedStorage = new PostgreSQLDeltaStorage(
        DatabaseProvider.createInMemorySQLiteConfig()
      );
      
      await expect(uninitializedStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields))
        .rejects.toThrow('not initialized');
    });
  });

  describe('storeCurrentData', () => {
    it('should store field sets successfully', async () => {
      const result = await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);

      expect(result.status).toBe('success');
      expect(result.recordCount).toBe(2);
      expect(result.storage).toBe('postgresql');
      expect(result.processingTime).toBeGreaterThan(0);
    });

    it('should require clientId', async () => {
      await expect(deltaStorage.storeCurrentData('', testFieldSets, primaryKeyFields))
        .rejects.toThrow('clientId is required');
    });

    it('should handle empty data array', async () => {
      const result = await deltaStorage.storeCurrentData(testClientId, [], primaryKeyFields);

      expect(result.status).toBe('success');
      expect(result.recordCount).toBe(0);
    });

    it('should filter out records without hashes', async () => {
      const dataWithoutHashes = [
        { fieldValues: [{ id: 1 }] }, // No hash
        { fieldValues: [{ id: 2 }], hash: 'valid-hash' } // Has hash
      ];

      const result = await deltaStorage.storeCurrentData(testClientId, dataWithoutHashes as FieldSet[], primaryKeyFields);

      expect(result.recordCount).toBe(1); // Only the record with hash should be stored
    });

    it('should promote existing current data to previous', async () => {
      // Store initial data
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);

      // Store new data (should promote previous current to previous)
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets, primaryKeyFields);

      // Verify by computing delta - should show the transition
      const delta = await deltaStorage.fetchDelta(testClientId, primaryKeyFields);
      
      // Alice (id=1) should be updated (same primary key, different hash)
      // Bob (id=2) should be removed (not in new data)
      // Charlie (id=3) should be added (new record)
      expect(delta.added).toHaveLength(1); // Charlie
      expect(delta.updated).toHaveLength(1); // Alice
      expect(delta.removed).toHaveLength(1); // Bob
      
      // Verify specific records (values are strings after reconstruction)
      expect(delta.added[0].fieldValues.find(fv => fv.id)?.id).toBe('3'); // Charlie
      expect(delta.updated![0].fieldValues.find(fv => fv.id)?.id).toBe('1'); // Alice
      expect(delta.removed[0].fieldValues.find(fv => fv.id)?.id).toBe('2'); // Bob
    });
  });

  describe('fetchDelta', () => {
    it('should return empty delta when no previous data exists', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
      
      const delta = await deltaStorage.fetchDelta(testClientId, primaryKeyFields);

      expect(delta.added).toHaveLength(2); // All current records are "added"
      expect(delta.updated).toHaveLength(0); // No updates when no previous data
      expect(delta.removed).toHaveLength(0); // No previous records to remove
    });

    it('should compute delta correctly with previous data', async () => {
      // Store initial data
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
      
      // Store updated data (promotes first data to previous)
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets, primaryKeyFields);

      const delta = await deltaStorage.fetchDelta(testClientId, primaryKeyFields);

      // Verify delta structure - now includes updated array
      expect(delta.added).toBeDefined();
      expect(delta.updated).toBeDefined();
      expect(delta.removed).toBeDefined();
      expect(Array.isArray(delta.added)).toBe(true);
      expect(Array.isArray(delta.updated)).toBe(true);
      expect(Array.isArray(delta.removed)).toBe(true);

      // Should have some changes due to Alice's name update and Bob's removal
      const totalChanges = delta.added.length + delta.updated!.length + delta.removed.length;
      expect(totalChanges).toBeGreaterThan(0);
    });

    it('should properly categorize added, updated, and removed records', async () => {
      // Store initial data: Alice and Bob
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
      
      // Store updated data: Alice (updated), Charlie (added), Bob (removed)
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets, primaryKeyFields);

      const delta = await deltaStorage.fetchDelta(testClientId, primaryKeyFields);

      // Verify three-way categorization
      expect(delta.added).toHaveLength(1);
      expect(delta.updated).toHaveLength(1);
      expect(delta.removed).toHaveLength(1);

      // Verify added record (Charlie - new primary key)
      const addedRecord = delta.added[0];
      expect(addedRecord.fieldValues.find(fv => fv.id)?.id).toBe('3'); // Values are strings after reconstruction
      expect(addedRecord.hash).toBe('hash-charlie');

      // Verify updated record (Alice - same primary key, different hash)
      const updatedRecord = delta.updated![0];
      expect(updatedRecord.fieldValues.find(fv => fv.id)?.id).toBe('1');
      expect(updatedRecord.fieldValues.find(fv => fv.dob)?.dob).toBe('1990-01-01T00:00:00.000Z');
      expect(updatedRecord.hash).toBe('hash-alice-updated');

      // Verify removed record (Bob - primary key no longer exists)
      const removedRecord = delta.removed[0];
      expect(removedRecord.fieldValues.find(fv => fv.id)?.id).toBe('2');
      expect(removedRecord.hash).toBe('hash-bob');
    });

    it('should require clientId', async () => {
      await expect(deltaStorage.fetchDelta('', primaryKeyFields))
        .rejects.toThrow('clientId is required');
    });

    it('should record delta computation in history', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets, primaryKeyFields);
      
      await deltaStorage.fetchDelta(testClientId, primaryKeyFields);

      const history = await deltaStorage.getDeltaHistory(testClientId, 1);
      expect(history).toHaveLength(1);
      expect(history[0].clientId).toBe(testClientId);
      expect(typeof history[0].deltaMetadata?.computationTime).toBe('number');
    });
  });

  describe('updatePreviousData', () => {
    beforeEach(async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
    });

    it('should promote current to previous when no new data provided', async () => {
      const result = await deltaStorage.updatePreviousData(testClientId, []);

      expect(result.status).toBe('success');
      expect(result.action).toContain('promoted current to previous');
      expect(result.storage).toBe('postgresql');
    });

    it('should replace previous data when new data provided', async () => {
      const newPreviousData = [testFieldSets[0]]; // Only first record

      const result = await deltaStorage.updatePreviousData(testClientId, newPreviousData, primaryKeyFields);

      expect(result.status).toBe('success');
      expect(result.action).toContain('replaced previous data');
      expect(result.recordCount).toBe(1);
    });

    it('should require clientId', async () => {
      await expect(deltaStorage.updatePreviousData('', []))
        .rejects.toThrow('clientId is required');
    });
  });

  describe('Utility Methods', () => {
    it('should retrieve delta history', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
      await deltaStorage.fetchDelta(testClientId, primaryKeyFields); // Creates history entry
      
      const history = await deltaStorage.getDeltaHistory(testClientId);

      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThan(0);
      expect(history[0].clientId).toBe(testClientId);
    });

    it('should cleanup client data', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
      await deltaStorage.fetchDelta(testClientId, primaryKeyFields); // Creates history entry
      
      // Verify data exists before cleanup
      const historyBefore = await deltaStorage.getDeltaHistory(testClientId);
      expect(historyBefore.length).toBeGreaterThan(0);
      
      await deltaStorage.cleanup(testClientId);

      // Verify data is cleaned up (don't call fetchDelta as it creates new history)
      const historyAfter = await deltaStorage.getDeltaHistory(testClientId);
      expect(historyAfter).toHaveLength(0);
      
      // Verify no field sets remain
      const recordCount = await deltaStorage['getRecordCount'](testClientId, 'current');
      expect(recordCount).toBe(0);
    });
  });

  describe('Entity Conversion', () => {
    it('should convert key and hash FieldSet to Entity and back correctly', () => {
      // Create a key and hash field set (primary key fields only)
      const keyAndHashFieldSet = {
        fieldValues: [
          { id: 1 },
          { dob: '1990-01-01T00:00:00.000Z' }
        ],
        hash: 'hash-alice'
      };
      
      const primaryKeyFields = new Set(['id', 'dob']);
      const entity = FieldSetEntity.fromKeyAndHashFieldSet(keyAndHashFieldSet, primaryKeyFields);

      expect(entity.hash).toBe(keyAndHashFieldSet.hash);
      expect(entity.primaryKey).toBe('1|1990-01-01T00:00:00.000Z');

      const convertedBack = entity.toKeyAndHashFieldSet(primaryKeyFields);
      expect(convertedBack.hash).toBe(keyAndHashFieldSet.hash);
      // Note: Values are stored as strings, so expect string conversion
      expect(convertedBack.fieldValues).toEqual([
        { id: '1' },
        { dob: '1990-01-01T00:00:00.000Z' }
      ]);
    });

    it('should handle single primary key correctly', () => {
      const keyAndHashFieldSet = {
        fieldValues: [{ id: 42 }],
        hash: 'hash-single-pk'
      };
      
      const primaryKeyFields = new Set(['id']);
      const entity = FieldSetEntity.fromKeyAndHashFieldSet(keyAndHashFieldSet, primaryKeyFields);

      expect(entity.primaryKey).toBe('42');

      const convertedBack = entity.toKeyAndHashFieldSet(primaryKeyFields);
      expect(convertedBack.fieldValues).toEqual([{ id: '42' }]); // Note: values are stored as strings
    });

    it('should handle composite primary keys correctly', () => {
      const keyAndHashFieldSet = {
        fieldValues: [
          { userId: 123 },
          { email: 'test@example.com' },
          { department: 'Engineering' }
        ],
        hash: 'hash-composite-pk'
      };
      
      const primaryKeyFields = new Set(['userId', 'email', 'department']);
      const entity = FieldSetEntity.fromKeyAndHashFieldSet(keyAndHashFieldSet, primaryKeyFields);

      expect(entity.primaryKey).toBe('123|test@example.com|Engineering');
      
      const convertedBack = entity.toKeyAndHashFieldSet(primaryKeyFields);
      expect(convertedBack.fieldValues).toEqual([
        { userId: '123' },
        { email: 'test@example.com' },
        { department: 'Engineering' }
      ]);
    });

    it('should throw error when FieldSet has no hash', () => {
      const fieldSetWithoutHash = {
        fieldValues: [{ id: 1 }]
      };
      
      const primaryKeyFields = new Set(['id']);
      
      expect(() => {
        FieldSetEntity.fromKeyAndHashFieldSet(fieldSetWithoutHash as any, primaryKeyFields);
      }).toThrow('FieldSet must have a hash to be stored in database');
    });
  });

  describe('fetchPreviousData', () => {
    beforeEach(async () => {
      // Store initial data and then new data to ensure we have previous data
      await deltaStorage.storeCurrentData(testClientId, testFieldSets, primaryKeyFields);
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets, primaryKeyFields);
    });

    it('should fetch all previous data without limitTo parameter', async () => {
      const previousData = await deltaStorage.fetchPreviousData(testClientId);

      expect(Array.isArray(previousData)).toBe(true);
      expect(previousData.length).toBe(2); // Alice and Bob from testFieldSets
      
      // Should return hash-only FieldSets when no limitTo provided (can't reconstruct field names)
      previousData.forEach(record => {
        expect(record.hash).toBeDefined();
        expect(record.fieldValues).toEqual([]); // Empty since field names can't be determined
      });

      // Verify we got the expected hashes from the original testFieldSets
      const hashes = previousData.map(record => record.hash).sort();
      expect(hashes).toEqual(['hash-alice', 'hash-bob']);
    });

    it('should fetch filtered previous data with limitTo parameter', async () => {
      // Limit to only Alice's record
      const limitToAlice = [testFieldSets[0]]; // Alice's original record

      const previousData = await deltaStorage.fetchPreviousData(testClientId, limitToAlice);

      expect(previousData).toHaveLength(1);
      
      const aliceRecord = previousData[0];
      expect(aliceRecord.hash).toBe('hash-alice');
      expect(aliceRecord.fieldValues).toHaveLength(2); // id and dob fields reconstructed
      
      // Verify field reconstruction from primary key
      expect(aliceRecord.fieldValues.find(fv => fv.id)?.id).toBe('1');
      expect(aliceRecord.fieldValues.find(fv => fv.dob)?.dob).toBe('1990-01-01T00:00:00.000Z');
    });

    it('should fetch multiple filtered records with limitTo parameter', async () => {
      // Limit to both Alice and Bob's records
      const limitToBoth = testFieldSets; // Both original records

      const previousData = await deltaStorage.fetchPreviousData(testClientId, limitToBoth);

      expect(previousData).toHaveLength(2);
      
      // Verify both records are properly reconstructed
      const aliceRecord = previousData.find(record => record.hash === 'hash-alice');
      const bobRecord = previousData.find(record => record.hash === 'hash-bob');
      
      expect(aliceRecord).toBeDefined();
      expect(bobRecord).toBeDefined();
      
      // Verify field reconstruction for both records
      expect(aliceRecord!.fieldValues.find(fv => fv.id)?.id).toBe('1');
      expect(bobRecord!.fieldValues.find(fv => fv.id)?.id).toBe('2');
    });

    it('should return empty array when limiting to non-existent records', async () => {
      // Limit to a record that doesn't exist in previous data
      const limitToNonExistent = [{
        fieldValues: [
          { id: 999 },
          { dob: '2000-01-01T00:00:00.000Z' }
        ],
        hash: 'hash-nonexistent'
      }];

      const previousData = await deltaStorage.fetchPreviousData(testClientId, limitToNonExistent);

      expect(previousData).toHaveLength(0);
    });

    it('should return partial results when limiting to mix of existing and non-existing records', async () => {
      // Mix Alice's existing record with a non-existent one
      const limitToMixed = [
        testFieldSets[0], // Alice exists
        {
          fieldValues: [
            { id: 999 },
            { dob: '2000-01-01T00:00:00.000Z' }
          ],
          hash: 'hash-nonexistent'
        }
      ];

      const previousData = await deltaStorage.fetchPreviousData(testClientId, limitToMixed);

      expect(previousData).toHaveLength(1); // Only Alice should be found
      expect(previousData[0].hash).toBe('hash-alice');
    });

    it('should return empty array when no previous data exists', async () => {
      const newClientId = `test-client-no-previous-${Date.now()}`;
      
      // Store current data only (no previous data)
      await deltaStorage.storeCurrentData(newClientId, testFieldSets, primaryKeyFields);
      
      const previousData = await deltaStorage.fetchPreviousData(newClientId);

      expect(previousData).toHaveLength(0);
    });

    it('should handle empty limitTo array (returns all data since no filter applied)', async () => {
      const previousData = await deltaStorage.fetchPreviousData(testClientId, []);

      // Empty array means no filtering is applied, so all previous data is returned
      expect(previousData).toHaveLength(2);
      expect(previousData.every(record => record.fieldValues.length === 0)).toBe(true); // No field reconstruction without limitTo
    });

    it('should require clientId parameter', async () => {
      await expect(deltaStorage.fetchPreviousData(''))
        .rejects.toThrow('clientId is required for fetchPreviousData');
    });

    it('should handle composite primary keys in limitTo filtering', async () => {
      // Create test data with composite primary keys
      const compositeKeyFields = new Set(['userId', 'department']);
      const compositeTestData = [
        {
          fieldValues: [
            { userId: 100 },
            { department: 'Engineering' }
          ],
          hash: 'hash-eng-100'
        },
        {
          fieldValues: [
            { userId: 200 },
            { department: 'Marketing' }
          ],
          hash: 'hash-mkt-200'
        }
      ];

      const compositeUpdatedData = [
        {
          fieldValues: [
            { userId: 300 },
            { department: 'Sales' }
          ],
          hash: 'hash-sales-300'
        }
      ];

      const compositeClientId = `test-composite-${Date.now()}`;

      // Store initial data and then update to create previous data
      await deltaStorage.storeCurrentData(compositeClientId, compositeTestData, compositeKeyFields);
      await deltaStorage.storeCurrentData(compositeClientId, compositeUpdatedData, compositeKeyFields);

      // Fetch with composite key limitation
      const limitToEng = [compositeTestData[0]]; // Engineering record
      const previousData = await deltaStorage.fetchPreviousData(compositeClientId, limitToEng);

      expect(previousData).toHaveLength(1);
      expect(previousData[0].hash).toBe('hash-eng-100');
      
      // Verify composite key reconstruction
      expect(previousData[0].fieldValues.find(fv => fv.userId)?.userId).toBe('100');
      expect(previousData[0].fieldValues.find(fv => fv.department)?.department).toBe('Engineering');
    });
  });

  describe('Multiple Client Operations', () => {
    it('should handle multiple client operations safely', async () => {
      const client1 = 'client-1';
      const client2 = 'client-2';

      // Run sequential operations (SQLite doesn't support concurrent transactions)
      const result1 = await deltaStorage.storeCurrentData(client1, testFieldSets, primaryKeyFields);
      const result2 = await deltaStorage.storeCurrentData(client2, updatedTestFieldSets, primaryKeyFields);

      expect(result1.status).toBe('success');
      expect(result2.status).toBe('success');

      // Verify both clients' data is intact
      const delta1 = await deltaStorage.fetchDelta(client1, primaryKeyFields);
      const delta2 = await deltaStorage.fetchDelta(client2, primaryKeyFields);

      expect(delta1.added.length).toBeGreaterThan(0);
      expect(delta2.added.length).toBeGreaterThan(0);
    });
  });
});
