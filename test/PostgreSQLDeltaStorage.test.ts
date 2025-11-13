import { DatabaseProvider } from '../src/delta-storage/db/DatabaseProvider';
import { FieldSetEntity } from '../src/delta-storage/db/entities/FieldSetEntity';
import { PostgreSQLDeltaStorage } from '../src/delta-storage/db/PostgreSQLDeltaStorage';
import { FieldSet } from '../src/InputTypes';

describe('PostgreSQLDeltaStorage', () => {
  let deltaStorage: PostgreSQLDeltaStorage;
  let testClientId: string;

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

  const updatedTestFieldSets: FieldSet[] = [
    {
      fieldValues: [
        { id: 1 },
        { fullname: 'Alice Anderson-Smith' }, // Updated name
        { dob: '1990-01-01T00:00:00.000Z' }
      ],
      hash: 'hash-alice-updated' // New hash due to name change
    },
    {
      fieldValues: [
        { id: 3 },
        { fullname: 'Charlie Chen' }, // New record
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
      
      await expect(uninitializedStorage.storeCurrentData(testClientId, testFieldSets))
        .rejects.toThrow('not initialized');
    });
  });

  describe('storeCurrentData', () => {
    it('should store field sets successfully', async () => {
      const result = await deltaStorage.storeCurrentData(testClientId, testFieldSets);

      expect(result.status).toBe('success');
      expect(result.recordCount).toBe(2);
      expect(result.storage).toBe('postgresql');
      expect(result.processingTime).toBeGreaterThan(0);
    });

    it('should require clientId', async () => {
      await expect(deltaStorage.storeCurrentData('', testFieldSets))
        .rejects.toThrow('clientId is required');
    });

    it('should handle empty data array', async () => {
      const result = await deltaStorage.storeCurrentData(testClientId, []);

      expect(result.status).toBe('success');
      expect(result.recordCount).toBe(0);
    });

    it('should filter out records without hashes', async () => {
      const dataWithoutHashes = [
        { fieldValues: [{ id: 1 }] }, // No hash
        { fieldValues: [{ id: 2 }], hash: 'valid-hash' } // Has hash
      ];

      const result = await deltaStorage.storeCurrentData(testClientId, dataWithoutHashes as FieldSet[]);

      expect(result.recordCount).toBe(1); // Only the record with hash should be stored
    });

    it('should promote existing current data to previous', async () => {
      // Store initial data
      await deltaStorage.storeCurrentData(testClientId, testFieldSets);

      // Store new data (should promote previous current to previous)
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets);

      // Verify by computing delta - should show the transition
      const delta = await deltaStorage.fetchDelta(testClientId);
      
      // Alice should be removed (old hash) and added (new hash) due to name change
      // Bob should be removed (not in new data)
      // Charlie should be added (new record)
      expect(delta.added.length).toBeGreaterThan(0);
      expect(delta.removed.length).toBeGreaterThan(0);
    });
  });

  describe('fetchDelta', () => {
    it('should return empty delta when no previous data exists', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets);
      
      const delta = await deltaStorage.fetchDelta(testClientId);

      expect(delta.added).toHaveLength(2); // All current records are "added"
      expect(delta.removed).toHaveLength(0); // No previous records to remove
    });

    it('should compute delta correctly with previous data', async () => {
      // Store initial data
      await deltaStorage.storeCurrentData(testClientId, testFieldSets);
      
      // Store updated data (promotes first data to previous)
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets);

      const delta = await deltaStorage.fetchDelta(testClientId);

      // Verify delta structure
      expect(delta.added).toBeDefined();
      expect(delta.removed).toBeDefined();
      expect(Array.isArray(delta.added)).toBe(true);
      expect(Array.isArray(delta.removed)).toBe(true);

      // Should have some changes due to Alice's name update and Bob's removal
      const totalChanges = delta.added.length + delta.removed.length;
      expect(totalChanges).toBeGreaterThan(0);
    });

    it('should require clientId', async () => {
      await expect(deltaStorage.fetchDelta(''))
        .rejects.toThrow('clientId is required');
    });

    it('should record delta computation in history', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets);
      await deltaStorage.storeCurrentData(testClientId, updatedTestFieldSets);
      
      await deltaStorage.fetchDelta(testClientId);

      const history = await deltaStorage.getDeltaHistory(testClientId, 1);
      expect(history).toHaveLength(1);
      expect(history[0].clientId).toBe(testClientId);
      expect(typeof history[0].deltaMetadata?.computationTime).toBe('number');
    });
  });

  describe('updatePreviousData', () => {
    beforeEach(async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets);
    });

    it('should promote current to previous when no new data provided', async () => {
      const result = await deltaStorage.updatePreviousData(testClientId);

      expect(result.status).toBe('success');
      expect(result.action).toContain('promoted current to previous');
      expect(result.storage).toBe('postgresql');
    });

    it('should replace previous data when new data provided', async () => {
      const newPreviousData = [testFieldSets[0]]; // Only first record

      const result = await deltaStorage.updatePreviousData(testClientId, newPreviousData);

      expect(result.status).toBe('success');
      expect(result.action).toContain('replaced previous data');
      expect(result.recordCount).toBe(1);
    });

    it('should require clientId', async () => {
      await expect(deltaStorage.updatePreviousData(''))
        .rejects.toThrow('clientId is required');
    });
  });

  describe('Utility Methods', () => {
    it('should retrieve delta history', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets);
      await deltaStorage.fetchDelta(testClientId); // Creates history entry
      
      const history = await deltaStorage.getDeltaHistory(testClientId);

      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThan(0);
      expect(history[0].clientId).toBe(testClientId);
    });

    it('should cleanup client data', async () => {
      await deltaStorage.storeCurrentData(testClientId, testFieldSets);
      await deltaStorage.fetchDelta(testClientId); // Creates history entry
      
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
    it('should convert FieldSet to Entity and back correctly', () => {
      const fieldSet = testFieldSets[0];
      const entity = FieldSetEntity.fromFieldSet(testClientId, fieldSet, 'current');

      expect(entity.clientId).toBe(testClientId);
      expect(entity.hash).toBe(fieldSet.hash);
      expect(entity.dataType).toBe('current');
      expect(entity.fieldValues).toEqual(fieldSet.fieldValues);

      const convertedBack = entity.toFieldSet();
      expect(convertedBack.hash).toBe(fieldSet.hash);
      expect(convertedBack.fieldValues).toEqual(fieldSet.fieldValues);
    });

    it('should handle validation messages correctly', () => {
      const fieldSetWithValidation: FieldSet = {
        ...testFieldSets[0],
        validationMessages: new Map([['field1', 'error message']])
      };

      const entity = FieldSetEntity.fromFieldSet(testClientId, fieldSetWithValidation, 'current');
      const convertedBack = entity.toFieldSet();

      expect(convertedBack.validationMessages).toBeInstanceOf(Map);
      expect(convertedBack.validationMessages?.get('field1')).toBe('error message');
    });
  });

  describe('Multiple Client Operations', () => {
    it('should handle multiple client operations safely', async () => {
      const client1 = 'client-1';
      const client2 = 'client-2';

      // Run sequential operations (SQLite doesn't support concurrent transactions)
      const result1 = await deltaStorage.storeCurrentData(client1, testFieldSets);
      const result2 = await deltaStorage.storeCurrentData(client2, updatedTestFieldSets);

      expect(result1.status).toBe('success');
      expect(result2.status).toBe('success');

      // Verify both clients' data is intact
      const delta1 = await deltaStorage.fetchDelta(client1);
      const delta2 = await deltaStorage.fetchDelta(client2);

      expect(delta1.added.length).toBeGreaterThan(0);
      expect(delta2.added.length).toBeGreaterThan(0);
    });
  });
});