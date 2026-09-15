import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoDBDeltaStorage } from '../src/delta-strategy/dynamodb/storage/DynamoDBDeltaStorage';
import { FieldSet } from '../src/InputTypes';

describe('DynamoDBDeltaStorage', () => {
  let sendSpy: jest.SpyInstance;

  beforeEach(() => {
    sendSpy = jest.spyOn(DynamoDBDocumentClient.prototype, 'send');
  });

  afterEach(() => {
    sendSpy.mockRestore();
  });

  const buildStorage = () => new DynamoDBDeltaStorage({
    region: 'us-east-1',
    personCurrentStateTableName: 'person-current-state',
    personHistoryTableName: 'person-history'
  });

  describe('updatePreviousData', () => {
    it('extracts the primary key value regardless of field name (e.g. sourceIdentifier, not personId)', async () => {
      sendSpy.mockResolvedValue({});
      const storage = buildStorage();

      const newPreviousData: FieldSet[] = [
        { fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: 'hash1' }
      ];

      await storage.updatePreviousData({ clientId: 'unused', newPreviousData });

      const batchWriteCall = sendSpy.mock.calls.find(([cmd]: any[]) =>
        cmd.input.RequestItems?.['person-current-state']
      );
      expect(batchWriteCall).toBeDefined();
      const items = batchWriteCall[0].input.RequestItems['person-current-state'];
      expect(items[0].PutRequest.Item.personId).toBe('U0000001');
      expect(items[0].PutRequest.Item.hash).toBe('hash1');
    });

    it('uses primaryKeyFields to select the correct field when provided', async () => {
      sendSpy.mockResolvedValue({});
      const storage = buildStorage();

      const newPreviousData: FieldSet[] = [
        { fieldValues: [{ someOtherField: 'ignored' }, { sourceIdentifier: 'U0000002' }], hash: 'hash2' }
      ];

      await storage.updatePreviousData({
        clientId: 'unused',
        newPreviousData,
        primaryKeyFields: new Set(['sourceIdentifier'])
      });

      const batchWriteCall = sendSpy.mock.calls.find(([cmd]: any[]) =>
        cmd.input.RequestItems?.['person-current-state']
      );
      const items = batchWriteCall[0].input.RequestItems['person-current-state'];
      expect(items[0].PutRequest.Item.personId).toBe('U0000002');
    });

    it('skips records with no resolvable primary key or hash', async () => {
      const storage = buildStorage();

      await storage.updatePreviousData({
        clientId: 'unused',
        newPreviousData: [{ fieldValues: [], hash: 'hash1' }]
      });

      expect(sendSpy).not.toHaveBeenCalled();
    });
  });

  describe('fetchPreviousData', () => {
    it('round-trips the caller\'s primary key field name (not a hardcoded "personId")', async () => {
      sendSpy.mockResolvedValue({
        Responses: {
          'person-current-state': [{ personId: 'U0000001', hash: 'hash1' }]
        }
      });
      const storage = buildStorage();

      const limitTo: FieldSet[] = [{ fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: '' }];
      const result = await storage.fetchPreviousData({ clientId: 'unused', limitTo });

      expect(result).toEqual([
        { fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: 'hash1' }
      ]);
    });

    it('returns an empty array when limitTo is empty', async () => {
      const storage = buildStorage();
      const result = await storage.fetchPreviousData({ clientId: 'unused', limitTo: [] });
      expect(result).toEqual([]);
      expect(sendSpy).not.toHaveBeenCalled();
    });
  });
});
