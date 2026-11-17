/**
 * Tests DeltaStrategyForDynamoDB.computeDelta() (src/delta-strategy/dynamodb/RunnerStrategyForDynamoDb.ts) against the
 * real DynamoDBDeltaStorage, with DynamoDB's BatchGet answered by a stub: a chunk of records whose field sets carry
 * several fields that hold the same identifier, the primary key not first among them (as a mapper may produce), must
 * still be matched to their stored records. A record whose stored hash differs, or is marked deleted, is updated, not
 * added; an unchanged one is neither; one with no stored record is added; nothing is removed.
 */
import { BatchGetCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DeltaStrategyForDynamoDB } from '../src/delta-strategy/dynamodb/RunnerStrategyForDynamoDb';
import { DynamoDBDeltaStorage } from '../src/delta-strategy/dynamodb/storage/DynamoDBDeltaStorage';
import { DELETED_HASH_PREFIX } from '../src/delta-strategy/DeletedHashMarker';
import { FieldDefinition, FieldSet } from '../src/InputTypes';
import { InputUtilsDecorator } from '../src/InputUtils';

const TABLE = 'person-current-state';

const config = { region: 'us-east-1', personCurrentStateTableName: TABLE, personHistoryTableName: 'person-history' };

/** Three fields with the same identifier, the primary key last, then a field that changes */
const fieldDefinitions: FieldDefinition[] = [
  { name: 'id', type: 'string', required: true },
  { name: 'employeeId', type: 'string', required: false },
  { name: 'sourceIdentifier', type: 'string', required: true, isPrimaryKey: true },
  { name: 'name', type: 'string', required: true }
];

const record = (id: string, hash: string, name = `Person ${id}`): FieldSet => ({
  fieldValues: [{ id }, { employeeId: id }, { sourceIdentifier: id }, { name }],
  hash
});

/** DynamoDB's answer to the chunk's BatchGet: the stored rows, keyed by personId */
const stored = (rows: Record<string, string>) => async (command: any) => {
  if (command instanceof BatchGetCommand) {
    const keys = command.input.RequestItems![TABLE].Keys as { personId: string }[];
    return { Responses: { [TABLE]: keys.filter(({ personId }) => rows[personId]).map(({ personId }) => ({ personId, hash: rows[personId] })) } };
  }
  return {};
};

describe('DeltaStrategyForDynamoDB.computeDelta()', () => {
  let sendSpy: jest.SpyInstance;
  beforeEach(() => { sendSpy = jest.spyOn(DynamoDBDocumentClient.prototype, 'send'); });
  afterEach(() => sendSpy.mockRestore());

  const computeDelta = async (current: FieldSet[], rows: Record<string, string>) => {
    sendSpy.mockImplementation(stored(rows) as any);
    const strategy = new DeltaStrategyForDynamoDB({ clientId: 'client', config });
    const storage = new DynamoDBDeltaStorage(config);
    return strategy.computeDelta({
      storage, currentFieldSets: current, clientId: 'client',
      inputUtils: new InputUtilsDecorator({ fieldDefinitions, fieldSets: current })
    });
  };
  const ids = (fieldSets: FieldSet[] = []) => fieldSets.map(fs => (fs.fieldValues.find(fv => 'sourceIdentifier' in fv) as any).sourceIdentifier).sort();

  it('updates a record whose stored hash differs, rather than adding it', async () => {
    const delta = await computeDelta([record('U0000001', 'new-hash')], { U0000001: 'old-hash' });
    expect(ids(delta.updated)).toEqual(['U0000001']);
    expect(delta.added).toEqual([]);
  });

  it('updates a record that returns after being marked deleted, so it is reactivated rather than created', async () => {
    const delta = await computeDelta([record('U0000501', 'hash-501')], { U0000501: `${DELETED_HASH_PREFIX}hash-501` });
    expect(ids(delta.updated)).toEqual(['U0000501']);
    expect(delta.added).toEqual([]);
  });

  it('leaves out a record whose stored hash is the same', async () => {
    const delta = await computeDelta([record('U0000002', 'same-hash')], { U0000002: 'same-hash' });
    expect(delta.added).toEqual([]);
    expect(delta.updated ?? []).toEqual([]);
  });

  it('adds a record with no stored record', async () => {
    const delta = await computeDelta([record('U0000003', 'hash-3')], {});
    expect(ids(delta.added)).toEqual(['U0000003']);
    expect(delta.updated ?? []).toEqual([]);
  });

  it('sorts a mixed chunk, and removes nothing', async () => {
    const delta = await computeDelta(
      [record('U0000001', 'new-hash'), record('U0000002', 'same-hash'), record('U0000003', 'hash-3'), record('U0000501', 'hash-501')],
      { U0000001: 'old-hash', U0000002: 'same-hash', U0000501: `${DELETED_HASH_PREFIX}hash-501` }
    );
    expect(ids(delta.added)).toEqual(['U0000003']);
    expect(ids(delta.updated)).toEqual(['U0000001', 'U0000501']);
    expect(delta.removed).toEqual([]);
  });

  it('looks the chunk up by its primary key alone', async () => {
    await computeDelta([record('U0000001', 'h1'), record('U0000002', 'h2')], {});
    const batchGet: any = sendSpy.mock.calls.map(([command]) => command).find(command => command instanceof BatchGetCommand);
    expect(batchGet.input.RequestItems[TABLE].Keys).toEqual([{ personId: 'U0000001' }, { personId: 'U0000002' }]);
  });
});
