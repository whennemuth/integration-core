import { BruteForceDeltaEngine, fishOutUpdatedRecordsByPK } from "../DeltaByBruteForce";
import { DeltaParms, DeltaStorage, FishingParms } from "../../DeltaTypes";
import { InputUtilsDecorator } from "../../InputUtils";
import { FieldSet } from "../../InputTypes";
import { DynamoDBDeltaStorage } from "./storage/DynamoDBDeltaStorage";
import { DeltaStrategy } from '../DeltaStrategy';
import { DeltaStrategyParams, DynamoDBConfig } from "../DeltaStrategyParams";

/**
 * DynamoDB storage strategy implementation
 * 
 * Uses DynamoDB tables for delta storage with in-memory delta computation.
 * Similar to file-based strategy (brute force comparison) rather than database strategy (SQL joins).
 * 
 * Delta Computation Flow:
 * 1. Fetch previous hash data from PersonCurrentState (BatchGetItem for chunk)
 * 2. Compare with current data in-memory using brute force delta engine
 * 3. Write changes to PersonCurrentState (hash + syncRunId only) and PersonHistory (full audit)
 * 4. Skip UNCHANGED records entirely (no writes)
 * 
 * Key Features:
 * - Chunk-scoped fetches (only personIds in current chunk)
 * - Batch operations for efficiency (100 per BatchGet, 25 per BatchWrite)
 * - No mini-deltas or marker files (direct table updates)
 * - Eliminates "maintain the illusion" coordination problem
 */
export class DeltaStrategyForDynamoDB extends DeltaStrategy {
  
  constructor(parms: DeltaStrategyParams) {
    super(parms);
    
    // Validate DynamoDBConfig
    const config = parms.config as DynamoDBConfig;
    if (!config.region || !config.personCurrentStateTableName || !config.personHistoryTableName) {
      throw new Error('DynamoDB strategy requires region, personCurrentStateTableName, and personHistoryTableName');
    }
  }
  
  /**
   * Computes delta using brute force comparison (in-memory).
   * Similar to file-based strategy rather than database strategy.
   * 
   * Steps:
   * 1. Fetch previous data for current chunk (BatchGetItem)
   * 2. Run brute force delta engine (in-memory comparison)
   * 3. Return classified changes (NEW, UPDATED, REMOVED)
   * 
   * Note: updatePreviousData() is called separately to write results to DynamoDB
   */
  public async computeDelta(computeParms: {
    storage: DeltaStorage, 
    currentFieldSets: any[], 
    inputUtils: InputUtilsDecorator, 
    clientId: string
  }): Promise<any> {
    const { storage, currentFieldSets, inputUtils, clientId } = computeParms;
    
    // DynamoDB storage: use brute force delta computation (like file-based strategy)
    const deltaEngine = new BruteForceDeltaEngine();

    // Fetch the chunk's stored records (BatchGetItem for only its personIds), asking by primary key alone: the stored
    // records come back named after the field asked by, and the comparison below matches records on the primary key.
    // Asked by whole records, they came back named after a record's first field (an identifier other than the primary
    // key, for some mappers), so no stored record matched, and every changed record was taken for a new one.
    const previous = await storage.fetchPreviousData({
      clientId,
      limitTo: DeltaStrategyForDynamoDB.keyAndHashFieldSets(currentFieldSets, inputUtils.getPrimaryKeys())
    }) || [];

    // Define delta parameters
    const deltaParms = {
      data: { current: currentFieldSets, previous },
      fishOutTheUpdates: (parms: FishingParms) => {
        return fishOutUpdatedRecordsByPK(parms, inputUtils.getPrimaryKeys());
      }
    } satisfies DeltaParms;

    // Compute delta using brute force engine
    return await deltaEngine.computeDelta(deltaParms);
  }

  /**
   * The given records reduced to their primary key fields and hash (as InputUtilsDecorator.getKeyAndHashFieldSets()
   * reduces its own), for looking up their stored records: the records compared, whatever the decorator wraps.
   */
  public static keyAndHashFieldSets(fieldSets: FieldSet[], primaryKeys: Set<string>): FieldSet[] {
    return fieldSets.map(fs => ({
      fieldValues: fs.fieldValues.filter(fv => primaryKeys.has(Object.keys(fv)[0])),
      hash: fs.hash
    }));
  }

  /**
   * Returns DynamoDB delta storage instance configured with table names from config
   */
  public get storage(): DeltaStorage {
    const config = this.parms.config as DynamoDBConfig;
    
    return new DynamoDBDeltaStorage({
      region: config.region,
      personCurrentStateTableName: config.personCurrentStateTableName,
      personHistoryTableName: config.personHistoryTableName,
      currentStateGSIName: config.currentStateGSIName,
      syncRunId: config.syncRunId,
      clientConfig: config.clientConfig
    });
  }
}
