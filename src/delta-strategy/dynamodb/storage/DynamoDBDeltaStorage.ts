import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { 
  DynamoDBDocumentClient, 
  BatchGetCommand,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandOutput,
  QueryCommand
} from '@aws-sdk/lib-dynamodb';
import { DeltaStorage } from '../../../DeltaTypes';
import { Field, FieldSet } from '../../../InputTypes';

/**
 * Configuration for DynamoDB delta storage
 */
export interface DynamoDBDeltaStorageConfig {
  /** AWS region for DynamoDB */
  region: string;
  
  /** Table name for PersonCurrentState (one record per person, overwrite mode) */
  personCurrentStateTableName: string;
  
  /** Table name for PersonHistory (append-only audit trail) */
  personHistoryTableName: string;
  
  /** GSI name for querying PersonCurrentState by syncRunId */
  currentStateGSIName?: string;
  
  /** The actual sync run's ID, stamped onto every record written by updatePreviousData().
   *  Falls back to a freshly-generated timestamp if omitted, but every caller within the same
   *  sync run should pass the same value so records can be correlated by syncRunId. */
  syncRunId?: string;
  
  /** Optional DynamoDB client configuration */
  clientConfig?: any;
}

/**
 * DynamoDB-based implementation of DeltaStorage that stores person hash state in two tables:
 * 
 * 1. PersonCurrentState: Single record per person (overwrite mode)
 *    - PK: personId
 *    - Attributes: hash, syncRunId, changeType
 *    - Used by processors for BatchGetItem to fetch previous hashes
 * 
 * 2. PersonHistory: Append-only audit trail
 *    - PK: personId, SK: syncRunId
 *    - Attributes: hash, changeType, previousHash (for UPDATED)
 *    - Write policy: NEW, UPDATED, DELETED only (skip UNCHANGED)
 * 
 * Key Design Features:
 * - Table names passed as constructor parameters (defined in fargate project)
 * - Batch operations for efficiency (up to 100 items per request)
 * - Chunk-scoped fetches (only fetch personIds in current chunk)
 * - No UNCHANGED writes (saves storage and write capacity)
 * - Eliminates "maintain the illusion" problem (no mini-deltas, no marker files)
 */
export class DynamoDBDeltaStorage implements DeltaStorage {
  public readonly name = 'DynamoDB Delta Storage';
  public readonly description = 'DynamoDB-based delta storage using PersonCurrentState and PersonHistory tables with batch operations';

  private client: DynamoDBDocumentClient;
  private config: DynamoDBDeltaStorageConfig;

  constructor(config: DynamoDBDeltaStorageConfig) {
    this.config = config;
    
    const dynamoClient = new DynamoDBClient({
      region: config.region,
      ...config.clientConfig
    });
    
    this.client = DynamoDBDocumentClient.from(dynamoClient);
  }

  /**
   * Fetches previous hash data for a list of personIds from PersonCurrentState table.
   * Uses BatchGetItem for efficiency (up to 100 items per request).
   * 
   * @param params.clientId - Not used in DynamoDB implementation (kept for interface compatibility)
   * @param params.limitTo - FieldSets containing personIds to fetch (chunk-scoped). Each FieldSet is
   *   expected to already be reduced to just its primary key field(s) (e.g. by
   *   InputUtilsDecorator.getKeyAndHashFieldSets() or DeltaStrategy.buildLimitToArray()), so the
   *   field name itself is not assumed - whatever it's actually called (e.g. sourceIdentifier), only
   *   its value is used to key the DynamoDB lookup.
   * @returns Array of FieldSets with the same field name as the input plus hash
   */
  async fetchPreviousData(params: { clientId: string; limitTo?: FieldSet[] }): Promise<FieldSet[]> {
    const { limitTo } = params;
    
    if (!limitTo || limitTo.length === 0) {
      // If no limitTo specified, return empty array
      // (In DynamoDB strategy, we always work with specific chunks)
      return [];
    }

    const { personCurrentStateTableName } = this.config;
    const idEntries = limitTo
      .map(fs => {
        const field = fs.fieldValues[0];
        if (!field) return undefined;
        const [fieldName] = Object.keys(field);
        return fieldName ? { fieldName, value: field[fieldName] as string } : undefined;
      })
      .filter((e): e is { fieldName: string; value: string } => !!e && !!e.value);

    if (idEntries.length === 0) {
      return [];
    }

    // Preserve the caller's primary key field name (e.g. sourceIdentifier) in the output, so
    // downstream matching (e.g. InputUtilsDecorator.restorePreviousHashesForFailures()) still works.
    const fieldName = idEntries[0].fieldName;
    const personIds = idEntries.map(e => e.value);

    // Batch get in chunks of 100 (DynamoDB limit)
    const batchSize = 100;
    const results: FieldSet[] = [];

    for (let i = 0; i < personIds.length; i += batchSize) {
      const batch = personIds.slice(i, i + batchSize);
      
      const command = new BatchGetCommand({
        RequestItems: {
          [personCurrentStateTableName]: {
            Keys: batch.map(personId => ({ personId }))
          }
        }
      });

      const response = await this.client.send(command);
      const items = response.Responses?.[personCurrentStateTableName] || [];

      // Convert DynamoDB items to FieldSets
      for (const item of items as any[]) {
        results.push({
          fieldValues: [
            { [fieldName]: item.personId }
          ],
          hash: item.hash
        });
      }
    }

    return results;
  }

  /**
   * Checks if updating previous data would overwrite existing data.
   * For DynamoDB, we always overwrite in PersonCurrentState (by design),
   * but we never overwrite in PersonHistory (append-only).
   * 
   * @param clientId - Not used in DynamoDB implementation
   * @returns Always returns true (we overwrite PersonCurrentState)
   */
  async wouldOverwritePreviousData(clientId: string): Promise<boolean> {
    // DynamoDB strategy always overwrites PersonCurrentState records
    // This is by design (single record per person)
    return true;
  }

  /**
   * Updates PersonCurrentState and PersonHistory tables with new hash state.
   * 
   * PersonCurrentState: Overwrites existing records (one record per person)
   * PersonHistory: Appends new records (audit trail)
   * 
   * Write Policy:
   * - NEW: Write to both tables
   * - UPDATED: Write to both tables (include previousHash in history)
   * - UNCHANGED: Skip entirely (neither table is written)
   * - DELETED: Only write to PersonHistory (handled by merger, not processors)
   *
   * Change classification: callers (EndToEnd) pass every record in the chunk, changed or not, so
   * each record's changeType is determined here by comparing its hash with the one stored in
   * PersonCurrentState: no stored record -> NEW, same hash -> UNCHANGED, different hash ->
   * UPDATED (with the stored hash as previousHash). A changeType/previousHash field already
   * present on a record takes precedence over this classification.
   *
   * @param params.clientId - Not used in DynamoDB implementation
   * @param params.newPreviousData - FieldSets whose sole field is the primary key (e.g.
   *   sourceIdentifier), plus hash and optional changeType/previousHash metadata. The DynamoDB
   *   item is always stored under the "personId" attribute regardless of the FieldSet's field name.
   * @param params.primaryKeyFields - Names the primary key field(s) so the correct field can be
   *   found by name regardless of what the caller's DataMapper calls it (e.g. sourceIdentifier);
   *   falls back to the FieldSet's sole field if not provided.
   * @param params.failureCount - Not used in DynamoDB implementation
   * @param params.cleanup - Not used in DynamoDB implementation
   */
  async updatePreviousData(params: {
    clientId: string;
    newPreviousData: FieldSet[];
    primaryKeyFields?: Set<string>;
    failureCount?: number;
    cleanup?: boolean;
  }): Promise<void> {
    const { newPreviousData, primaryKeyFields } = params;
    
    if (!newPreviousData || newPreviousData.length === 0) {
      return;
    }

    const { personCurrentStateTableName, personHistoryTableName } = this.config;
    const syncRunId = this.config.syncRunId ?? new Date().toISOString();

    // Resolve each record's personId, hash and any caller-supplied change metadata
    const records: { personId: string; hash: string; explicitChangeType?: string; explicitPreviousHash?: string }[] = [];
    for (const fieldSet of newPreviousData) {
      const personIdField = primaryKeyFields && primaryKeyFields.size > 0
        ? fieldSet.fieldValues.find((fv: Field) => Object.keys(fv).some(k => primaryKeyFields.has(k)))
        : fieldSet.fieldValues[0];
      const personId = personIdField ? Object.values(personIdField)[0] as string : undefined;
      const hash = fieldSet.hash;
      const changeTypeField = fieldSet.fieldValues.find((fv: Field) => 'changeType' in fv);
      const previousHashField = fieldSet.fieldValues.find((fv: Field) => 'previousHash' in fv);

      if (!personId || !hash) {
        continue; // Skip invalid records
      }

      records.push({
        personId,
        hash,
        explicitChangeType: changeTypeField?.['changeType'] as string | undefined,
        explicitPreviousHash: previousHashField?.['previousHash'] as string | undefined
      });
    }

    if (records.length === 0) {
      return;
    }

    // Look up stored hashes for records whose changeType must be classified here
    const storedHashes = await this.getStoredHashes(
      records.filter(r => !r.explicitChangeType).map(r => r.personId)
    );

    // Prepare items for batch write
    const currentStateItems: any[] = [];
    const historyItems: any[] = [];
    let newCount = 0, updatedCount = 0, unchangedCount = 0;

    for (const { personId, hash, explicitChangeType, explicitPreviousHash } of records) {
      const storedHash = storedHashes.get(personId);
      const changeType = explicitChangeType
        ?? (storedHash === undefined ? 'NEW' : storedHash === hash ? 'UNCHANGED' : 'UPDATED');
      const previousHash = explicitPreviousHash ?? storedHash;

      if (changeType === 'NEW') newCount++;
      else if (changeType === 'UPDATED') updatedCount++;
      else if (changeType === 'UNCHANGED') unchangedCount++;

      // Skip UNCHANGED records entirely
      if (changeType === 'UNCHANGED') {
        continue;
      }

      // PersonCurrentState: Overwrite mode (no changeType field needed)
      currentStateItems.push({
        PutRequest: {
          Item: {
            personId,
            hash,
            syncRunId
          }
        }
      });

      // PersonHistory: Append mode
      const historyItem: any = {
        personId,
        syncRunId,
        hash,
        changeType
      };

      if (changeType === 'UPDATED' && previousHash) {
        historyItem.previousHash = previousHash;
      }

      historyItems.push({
        PutRequest: { Item: historyItem }
      });
    }

    console.log(`DynamoDBDeltaStorage: ${newCount} NEW, ${updatedCount} UPDATED, ${unchangedCount} UNCHANGED (not written) of ${records.length} record(s)`);

    // PersonCurrentState is written before PersonHistory: if writing fails part way, a person may
    // lack a history entry, but is never left with a history entry that PersonCurrentState doesn't
    // reflect - which would get the same change written to history again on the next run.
    await this.batchWriteWithRetry(personCurrentStateTableName, currentStateItems);
    await this.batchWriteWithRetry(personHistoryTableName, historyItems);
  }

  /**
   * Batch write requests to a table in chunks of 25 (DynamoDB limit for BatchWriteItem).
   * BatchWriteItem does not throw when throttled - it returns the requests it skipped as
   * UnprocessedItems - so those are retried with exponential backoff, and an error is thrown if
   * any remain after the final attempt, rather than letting writes be silently dropped.
   *
   * @param tableName - Table to write to
   * @param requests - PutRequest/DeleteRequest objects
   */
  private async batchWriteWithRetry(tableName: string, requests: any[]): Promise<void> {
    const batchSize = 25;
    const maxAttempts = 5;

    for (let i = 0; i < requests.length; i += batchSize) {
      let pending: any[] | undefined = requests.slice(i, i + batchSize);

      for (let attempt = 1; pending && pending.length > 0; attempt++) {
        if (attempt > maxAttempts) {
          throw new Error(`Failed to write to ${tableName}: ${pending.length} request(s) still unprocessed after ${maxAttempts} attempts`);
        }
        if (attempt > 1) {
          await new Promise(resolve => setTimeout(resolve, 100 * 2 ** (attempt - 2)));
        }

        const response: BatchWriteCommandOutput = await this.client.send(new BatchWriteCommand({
          RequestItems: { [tableName]: pending }
        }));

        pending = response.UnprocessedItems?.[tableName];
      }
    }
  }

  /**
   * Fetch the stored PersonCurrentState hash for each of the given personIds.
   * Unprocessed keys (e.g. from throttling) are retried, since a person silently missing from the
   * result would be misclassified as NEW.
   *
   * @param personIds - Person identifiers to look up
   * @returns Map of personId to stored hash (only includes persons that have a stored record)
   */
  private async getStoredHashes(personIds: string[]): Promise<Map<string, string>> {
    const { personCurrentStateTableName } = this.config;
    const storedHashes = new Map<string, string>();
    const uniqueIds = Array.from(new Set(personIds));
    const batchSize = 100; // DynamoDB BatchGetItem limit
    const maxAttempts = 5;

    for (let i = 0; i < uniqueIds.length; i += batchSize) {
      let keys: any[] | undefined = uniqueIds.slice(i, i + batchSize).map(personId => ({ personId }));

      for (let attempt = 1; keys && keys.length > 0; attempt++) {
        if (attempt > maxAttempts) {
          throw new Error(`Failed to fetch stored hashes: ${keys.length} key(s) still unprocessed after ${maxAttempts} attempts`);
        }
        if (attempt > 1) {
          await new Promise(resolve => setTimeout(resolve, 100 * 2 ** (attempt - 2)));
        }

        const response: BatchGetCommandOutput = await this.client.send(new BatchGetCommand({
          RequestItems: {
            [personCurrentStateTableName]: { Keys: keys, ProjectionExpression: 'personId, #h', ExpressionAttributeNames: { '#h': 'hash' } }
          }
        }));

        for (const item of (response.Responses?.[personCurrentStateTableName] || []) as any[]) {
          storedHashes.set(item.personId, item.hash);
        }

        keys = response.UnprocessedKeys?.[personCurrentStateTableName]?.Keys;
      }
    }

    return storedHashes;
  }

  /**
   * Query PersonCurrentState by syncRunId to get all persons seen in a specific sync run.
   * Used by merger for deletion detection.
   * 
   * @param syncRunId - ISO timestamp of sync run
   * @returns Array of personIds seen in the sync run
   */
  async getPersonIdsForSyncRun(syncRunId: string): Promise<string[]> {
    const { personCurrentStateTableName, currentStateGSIName } = this.config;
    
    if (!currentStateGSIName) {
      throw new Error('GSI name required for querying by syncRunId');
    }

    const personIds: string[] = [];
    let lastEvaluatedKey: any = undefined;

    do {
      const command = new QueryCommand({
        TableName: personCurrentStateTableName,
        IndexName: currentStateGSIName,
        KeyConditionExpression: 'syncRunId = :syncRunId',
        ExpressionAttributeValues: {
          ':syncRunId': syncRunId
        },
        ProjectionExpression: 'personId',
        ExclusiveStartKey: lastEvaluatedKey
      });

      const response = await this.client.send(command);
      
      if (response.Items) {
        personIds.push(...response.Items.map((item: any) => item.personId));
      }

      lastEvaluatedKey = response.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    return personIds;
  }
}
