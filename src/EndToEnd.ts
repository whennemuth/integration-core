import { DataMapper } from "./DataMapper";
import { DataSource } from "./DataSource";
import { DataTarget, PushAllParms } from "./DataTarget";
import { DeltaStrategy } from "./delta-strategy/DeltaStrategy";
import { isDatabaseConfig } from "./delta-strategy/DeltaStrategyParams";
import { DeltaResult } from "./DeltaTypes";
import { InputParser } from "./InputParser";
import { Field, FieldDefinition, FieldValidator } from "./InputTypes";
import { InputUtilsDecorator } from "./InputUtils";
import { BasicFieldValidator } from "./InputValidation";

export class EndToEnd {

  constructor(private params: { 
    dataSource: DataSource; 
    dataMapper: DataMapper;
    dataTarget: DataTarget; 
    deltaStrategy: DeltaStrategy; 
    fieldValidator?: FieldValidator 
  }) { }

  public async execute(): Promise<void> {
    const { dataSource, dataMapper,dataTarget, deltaStrategy, fieldValidator } = this.params;
    const { storage, parms: { config, clientId }, } = deltaStrategy;
  
    // Fetch raw data from the data source
    const rawData = await dataSource.fetchRaw();

    // Convert raw data to Input format using DataMapper
    const unparsedInput = dataMapper.map(rawData);

    // Create field validator factory function    
    const fieldValidatorFactory = (fieldDef: FieldDefinition, field: Field): FieldValidator => 
      fieldValidator ?? BasicFieldValidator.getInstance(fieldDef, field);

    // Create an input parser instance
    const inputParser = new InputParser({ fieldValidator: fieldValidatorFactory, _input: unparsedInput });

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
      return;
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
    const primaryKeyFields = inputUtils.getPrimaryKey();
    await storage.updatePreviousData({ 
      clientId, newPreviousData: keyAndHashFieldSets, primaryKeyFields, failureCount
    });
  }
}