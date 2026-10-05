import { BatchGetCommand, BatchWriteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
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
        cmd instanceof BatchWriteCommand && cmd.input.RequestItems?.['person-current-state']
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
        cmd instanceof BatchWriteCommand && cmd.input.RequestItems?.['person-current-state']
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

    it('stamps records with the configured syncRunId instead of a freshly-generated timestamp', async () => {
      sendSpy.mockResolvedValue({});
      const storage = new DynamoDBDeltaStorage({
        region: 'us-east-1',
        personCurrentStateTableName: 'person-current-state',
        personHistoryTableName: 'person-history',
        syncRunId: '2026-09-15T03:06:06.027Z'
      });

      const newPreviousData: FieldSet[] = [
        { fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: 'hash1' },
        { fieldValues: [{ sourceIdentifier: 'U0000002' }], hash: 'hash2' }
      ];

      await storage.updatePreviousData({ clientId: 'unused', newPreviousData });

      const batchWriteCall = sendSpy.mock.calls.find(([cmd]: any[]) =>
        cmd instanceof BatchWriteCommand && cmd.input.RequestItems?.['person-current-state']
      );
      const items = batchWriteCall[0].input.RequestItems['person-current-state'];
      expect(items[0].PutRequest.Item.syncRunId).toBe('2026-09-15T03:06:06.027Z');
      expect(items[1].PutRequest.Item.syncRunId).toBe('2026-09-15T03:06:06.027Z');
    });

    describe('change classification against stored hashes', () => {
      const writtenItems = (tableName: string): any[] => sendSpy.mock.calls
        .filter(([cmd]: any[]) => cmd instanceof BatchWriteCommand && cmd.input.RequestItems?.[tableName])
        .flatMap(([cmd]: any[]) => cmd.input.RequestItems[tableName].map((r: any) => r.PutRequest.Item));

      const mockStoredHashes = (stored: Record<string, string>) => {
        sendSpy.mockImplementation(async (cmd: any) => {
          if (cmd instanceof BatchGetCommand) {
            const keys = cmd.input.RequestItems!['person-current-state'].Keys as { personId: string }[];
            return {
              Responses: {
                'person-current-state': keys
                  .filter(({ personId }) => personId in stored)
                  .map(({ personId }) => ({ personId, hash: stored[personId] }))
              }
            };
          }
          return {};
        });
      };

      it('writes NEW and UPDATED (with previousHash) records and skips UNCHANGED ones in both tables', async () => {
        mockStoredHashes({ U0000002: 'hash2', U0000003: 'old-hash3' });
        const storage = buildStorage();

        await storage.updatePreviousData({
          clientId: 'unused',
          newPreviousData: [
            { fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: 'hash1' }, // no stored record
            { fieldValues: [{ sourceIdentifier: 'U0000002' }], hash: 'hash2' }, // same hash
            { fieldValues: [{ sourceIdentifier: 'U0000003' }], hash: 'hash3' }, // different hash
          ]
        });

        expect(writtenItems('person-current-state').map(i => i.personId)).toEqual(['U0000001', 'U0000003']);
        expect(writtenItems('person-history')).toEqual([
          { personId: 'U0000001', syncRunId: expect.any(String), hash: 'hash1', changeType: 'NEW' },
          { personId: 'U0000003', syncRunId: expect.any(String), hash: 'hash3', changeType: 'UPDATED', previousHash: 'old-hash3' },
        ]);
      });

      it('writes nothing when every record is unchanged', async () => {
        mockStoredHashes({ U0000001: 'hash1' });
        const storage = buildStorage();

        await storage.updatePreviousData({
          clientId: 'unused',
          newPreviousData: [{ fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: 'hash1' }]
        });

        expect(sendSpy.mock.calls.some(([cmd]: any[]) => cmd instanceof BatchWriteCommand)).toBe(false);
      });

      it('honors a changeType supplied on the record without looking up its stored hash', async () => {
        mockStoredHashes({ U0000001: 'hash1' });
        const storage = buildStorage();

        await storage.updatePreviousData({
          clientId: 'unused',
          newPreviousData: [{ fieldValues: [{ sourceIdentifier: 'U0000001' }, { changeType: 'UPDATED' }, { previousHash: 'prev' }], hash: 'hash1' }],
          primaryKeyFields: new Set(['sourceIdentifier'])
        });

        expect(sendSpy.mock.calls.some(([cmd]: any[]) => cmd instanceof BatchGetCommand)).toBe(false);
        expect(writtenItems('person-history')[0]).toMatchObject({ changeType: 'UPDATED', previousHash: 'prev' });
      });

      it('retries unprocessed keys so existing persons are not misclassified as NEW', async () => {
        let getCalls = 0;
        sendSpy.mockImplementation(async (cmd: any) => {
          if (cmd instanceof BatchGetCommand) {
            getCalls++;
            const keys = cmd.input.RequestItems!['person-current-state'].Keys;
            return getCalls === 1
              ? { Responses: { 'person-current-state': [] }, UnprocessedKeys: { 'person-current-state': { Keys: keys } } }
              : { Responses: { 'person-current-state': [{ personId: 'U0000001', hash: 'old-hash1' }] } };
          }
          return {};
        });
        const storage = buildStorage();

        await storage.updatePreviousData({
          clientId: 'unused',
          newPreviousData: [{ fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: 'hash1' }]
        });

        expect(getCalls).toBe(2);
        expect(writtenItems('person-history')[0]).toMatchObject({ changeType: 'UPDATED', previousHash: 'old-hash1' });
      });
    });

    describe('unprocessed write retries', () => {
      const newPreviousData: FieldSet[] = [{ fieldValues: [{ sourceIdentifier: 'U0000001' }], hash: 'hash1' }];

      it('resends unprocessed write requests until none remain', async () => {
        const writesPerTable: Record<string, number> = {};
        sendSpy.mockImplementation(async (cmd: any) => {
          if (cmd instanceof BatchWriteCommand) {
            const [tableName] = Object.keys(cmd.input.RequestItems!);
            writesPerTable[tableName] = (writesPerTable[tableName] || 0) + 1;
            // Throttle each table's first write
            return writesPerTable[tableName] === 1
              ? { UnprocessedItems: { [tableName]: cmd.input.RequestItems![tableName] } }
              : {};
          }
          return {};
        });
        const storage = buildStorage();

        await storage.updatePreviousData({ clientId: 'unused', newPreviousData });

        expect(writesPerTable).toEqual({ 'person-current-state': 2, 'person-history': 2 });
      });

      it('throws (without writing history) when PersonCurrentState writes stay unprocessed', async () => {
        sendSpy.mockImplementation(async (cmd: any) => {
          if (cmd instanceof BatchWriteCommand) {
            return { UnprocessedItems: cmd.input.RequestItems };
          }
          return {};
        });
        const storage = buildStorage();

        await expect(storage.updatePreviousData({ clientId: 'unused', newPreviousData }))
          .rejects.toThrow(/person-current-state: 1 request\(s\) still unprocessed after 5 attempts/);
        expect(sendSpy.mock.calls.some(([cmd]: any[]) =>
          cmd instanceof BatchWriteCommand && cmd.input.RequestItems?.['person-history']
        )).toBe(false);
      });
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
