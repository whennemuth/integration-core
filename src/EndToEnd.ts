import { DataMapper } from "./DataMapper";
import { DataSource } from "./DataSource";
import { DataTarget, PushAllParms, SinglePushResult } from "./DataTarget";
import { DeltaStrategy } from "./delta-strategy/DeltaStrategy";
import { isDatabaseConfig } from "./delta-strategy/DeltaStrategyParams";
import { DeltaResult } from "./DeltaTypes";
import { InputParser } from "./InputParser";
import { Field, FieldDefinition, FieldSet, FieldValidator } from "./InputTypes";
import { InputUtilsDecorator } from "./InputUtils";
import { BasicFieldValidator } from "./InputValidation";

/**
 * Statistics returned from EndToEnd integration execution
 */
export interface IntegrationResult {
  /** Total number of records processed (successfully pushed + failed) */
  totalProcessed: number;
  
  /** Number of records successfully pushed to target */
  successCount: number;
  
  /** Number of records that failed to push */
  failureCount: number;
  
  /** Number of records added (from delta computation) */
  addedCount: number;
  
  /** Number of records updated (from delta computation) */
  updatedCount: number;
  
  /** Number of records removed (from delta computation) */
  removedCount: number;
  
  /** Optional: timestamp when execution completed */
  timestamp?: Date;
  
  /** Optional: execution duration in milliseconds */
  duration?: number;
  
  /** Optional: detailed success results */
  successes?: SinglePushResult[];
  
  /** Optional: detailed failure results */
  failures?: SinglePushResult[];
}

export class EndToEnd {

  constructor(private params: { 
    dataSource: DataSource; 
    dataMapper: DataMapper;
    dataTarget: DataTarget; 
    deltaStrategy: DeltaStrategy; 
    fieldValidator?: FieldValidator,
    fieldFilter?: (fieldSet: FieldSet) => FieldSet,
    cleanupPreviousData?: boolean
  }) { }

  public async execute(): Promise<IntegrationResult> {
    const startTime = Date.now();
    
    const { 
      dataSource, dataMapper,dataTarget, deltaStrategy, fieldValidator, 
      fieldFilter = (fs: FieldSet) => fs, cleanupPreviousData = false
    } = this.params;
    const { storage, parms: { config, clientId }, } = deltaStrategy;
  
    // Fetch raw data from the data source
    const rawData = await dataSource.fetchRaw();

    // Convert raw data to Input format using DataMapper
    const unparsedInput = dataMapper.map(rawData);

    // Create field validator factory function    
    const fieldValidatorFactory = (fieldDef: FieldDefinition, field: Field): FieldValidator => 
      fieldValidator ?? BasicFieldValidator.getInstance(fieldDef, field);

    // Create an input parser instance
    const inputParser = new InputParser({ 
      fieldValidator: fieldValidatorFactory, _input: unparsedInput, fieldFilter
    });

    // Parse the Input to validate and hash records
    const parsedInput = inputParser.parse();

    // Get an instance of InputUtilsDecorator for helper methods on the parsed input
    const inputUtils = new InputUtilsDecorator(parsedInput);

    // Reduce down to just the primary keys and the hash (FieldSet size remains the same)
    const keyAndHashFieldSets = inputUtils.getKeyAndHashFieldSets();

    // Compute delta using appropriate strategy based on storage type
    const delta: DeltaResult = await deltaStrategy.computeDelta({
      storage,
      currentFieldSets: parsedInput.fieldSets,
      inputUtils,
      clientId
    });
    
    // If no changes detected, exit early
    if (delta.added.length === 0 && (delta.updated ?? []).length === 0 && delta.removed.length === 0) {
      console.log('No changes detected; skipping push and storage update.');
      return {
        totalProcessed: 0,
        successCount: 0,
        failureCount: 0,
        addedCount: 0,
        updatedCount: 0,
        removedCount: 0,
        timestamp: new Date(),
        duration: Date.now() - startTime
      };
    }

    // Push delta to data target
    const pushResult = await dataTarget.pushAll!(delta as PushAllParms);
    
    // Build limitTo array from push failures and validation errors for efficient database queries
    const limitTo = config && isDatabaseConfig(config) 
      ? DeltaStrategy.buildLimitToArray(keyAndHashFieldSets, pushResult) 
      : undefined;
    
    // Create a corrected "baseline": For records that failed to push or were invalid, restore their 
    // previous hashes if they were pre-existing records, else remove the record entirely.
    const previousInputFieldSets = await storage.fetchPreviousData({ clientId, limitTo });
    const failureCount = inputUtils.restorePreviousHashesForFailures({ 
      currentKeyAndHashFieldSets: keyAndHashFieldSets, 
      previousKeyAndHashFieldSets: previousInputFieldSets, 
      pushResult 
    });
    
    // Update storage with the new baseline data
    const primaryKeyFields = inputUtils.getPrimaryKeys();
    await storage.updatePreviousData({
      // NOTE: A previous data file may not exist, in which case this is not really an update.
      clientId, newPreviousData: keyAndHashFieldSets, primaryKeyFields, failureCount, 
      cleanup: cleanupPreviousData
    });

    // Calculate and return statistics
    const successCount = pushResult.successes?.length ?? 0;
    const failureCountFromPush = pushResult.failures?.length ?? 0;
    
    return {
      totalProcessed: successCount + failureCountFromPush,
      successCount,
      failureCount: failureCountFromPush,
      addedCount: delta.added.length,
      updatedCount: delta.updated?.length ?? 0,
      removedCount: delta.removed.length,
      timestamp: new Date(),
      duration: Date.now() - startTime,
      successes: pushResult.successes,
      failures: pushResult.failures
    };
  }
}