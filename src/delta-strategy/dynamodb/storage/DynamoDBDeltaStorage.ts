import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { 
  DynamoDBDocumentClient, 
  BatchGetCommand, 
  BatchWriteCommand,
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
   * - UNCHANGED: Skip entirely (not written to params)
   * - DELETED: Only write to PersonHistory (handled by merger, not processors)
   * 
   * @param params.clientId - Not used in DynamoDB implementation
   * @param params.newPreviousData - FieldSets whose sole field is the primary key (e.g.
   *   sourceIdentifier), plus hash and changeType metadata. The DynamoDB item is always stored
   *   under the "personId" attribute regardless of the FieldSet's field name.
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
    const syncRunId = new Date().toISOString();

    // Prepare items for batch write
    const currentStateItems: any[] = [];
    const historyItems: any[] = [];

    for (const fieldSet of newPreviousData) {
      const personIdField = primaryKeyFields && primaryKeyFields.size > 0
        ? fieldSet.fieldValues.find((fv: Field) => Object.keys(fv).some(k => primaryKeyFields.has(k)))
        : fieldSet.fieldValues[0];
      const personId = personIdField ? Object.values(personIdField)[0] as string : undefined;
      const hash = fieldSet.hash;
      const changeTypeField = fieldSet.fieldValues.find((fv: Field) => 'changeType' in fv);
      const changeType = (changeTypeField?.['changeType'] as string) || 'UPDATED';
      const previousHashField = fieldSet.fieldValues.find((fv: Field) => 'previousHash' in fv);
      const previousHash = previousHashField?.['previousHash'] as string | undefined;

      if (!personId || !hash) {
        continue; // Skip invalid records
      }

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

    // Batch write in chunks of 25 (DynamoDB limit for BatchWriteItem)
    const batchSize = 25;

    // Write to PersonCurrentState
    for (let i = 0; i < currentStateItems.length; i += batchSize) {
      const batch = currentStateItems.slice(i, i + batchSize);
      await this.client.send(new BatchWriteCommand({
        RequestItems: {
          [personCurrentStateTableName]: batch
        }
      }));
    }

    // Write to PersonHistory
    for (let i = 0; i < historyItems.length; i += batchSize) {
      const batch = historyItems.slice(i, i + batchSize);
      await this.client.send(new BatchWriteCommand({
        RequestItems: {
          [personHistoryTableName]: batch
        }
      }));
    }
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
