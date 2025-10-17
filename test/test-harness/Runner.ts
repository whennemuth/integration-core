import { PushAllParms } from "../../src/DataTarget";
import { InputParser } from "../../src/InputParser";
import { Field, FieldDefinition, FieldValidator } from "../../src/InputTypes";
import { InputUtilsDecorator } from "../../src/InputUtils";
import { BasicFieldValidator } from "../../src/InputValidation";
import { MockDataSource } from "./mock/MockDataSource";
import { MockDataTarget } from "./mock/MockDataTarget";
import { RandomData } from "./mock/RandomData";
import { isDatabaseConfig, RunnerConfig, RunnerParameterSet, storagePath } from './RunnerParams';
import runnerConfig from './RunnerParams.json';
import { RunnerStrategy } from './RunnerStrategy';
import { RunnerStrategyFactory } from './RunnerStrategyFactory';

export class DeltaStorageRunner {

  constructor(private strategy: RunnerStrategy) { }

  public async run(): Promise<void> {
    const { strategy } = this;
    const { config, clientId, populationSize, simulatedPushFailureIndexes = [] } = strategy.parms;
    const storage = strategy.storage;

    // Create a mock data source to generate test data
    const mockDataSource = new MockDataSource({ clientId, storagePath, generator: new RandomData(populationSize) });

    // Create a mock data target that simulates push failures for certain records
    const mockDataTarget = new MockDataTarget({ simulatedPushFailureIndexes });

    // Get the raw data from the mock data source
    const rawData = await mockDataSource.fetchRaw();

    // Convert raw data to Input format
    const unparsedInput = mockDataSource.convertRawToInput(rawData);
    
    // Create field validator factory function
    const fieldValidator = (fieldDef: FieldDefinition, field: Field): FieldValidator => 
      BasicFieldValidator.getInstance(fieldDef, field);

    // Create an input parser instance
    const inputParser = new InputParser({ fieldValidator, _input: unparsedInput });

    // Parse the Input to validate and hash records
    const parsedInput = inputParser.parse();

    // Get an instance of InputUtilsDecorator for helper methods on the parsed input
    const inputUtils = new InputUtilsDecorator(parsedInput);

    // Reduce down to just the primary keys and the hash (FieldSet size remains the same)
    const keyAndHashFieldSets = inputUtils.getKeyAndHashFieldSets();

    // Compute delta using appropriate strategy based on storage type
    const delta = await strategy.computeDelta({
      storage,
      currentFieldSets: parsedInput.fieldSets,
      inputUtils,
      clientId
    });

    // If no changes detected, exit early
    if (delta.added.length === 0 && delta.updated.length === 0 && delta.deleted.length === 0) {
      console.log('No changes detected; skipping push and storage update.');
      return;
    }

    // Push delta to data target
    const pushResult = await mockDataTarget.pushAll!(delta as PushAllParms);
    
    // Build limitTo array from push failures and validation errors for efficient database queries
    const limitTo = config && isDatabaseConfig(config) 
      ? RunnerStrategy.buildLimitToArray(keyAndHashFieldSets, pushResult) 
      : undefined;
    
    // Restore previous hashes for any records that failed to push or were invalid
    const previousInputFieldSets = await storage.fetchPreviousData(clientId, limitTo);
    inputUtils.restorePreviousHashesForFailures({ 
      currentKeyAndHashFieldSets: keyAndHashFieldSets, 
      previousKeyAndHashFieldSets: previousInputFieldSets, 
      pushResult 
    });
    
    // Update storage with the new baseline data after successful processing
    const primaryKeyFields = inputUtils.getPrimaryKey();
    await storage.updatePreviousData(clientId, keyAndHashFieldSets, primaryKeyFields);

    console.log('Test run complete.');
  }
}


/**
 * Main execution block for the DeltaStorageRunner test harness.
 * Set the activeParameterSetId in RunnerParams.json to indicate the scenario you want to run.
 */
(async () => {
  const config = runnerConfig as RunnerConfig;
  
  // Find the active parameter set
  const activeParameterSet = config.parameterSets.find((ps: RunnerParameterSet) => ps.id === config.activeParameterSetId);
  
  if (!activeParameterSet) {
    console.error(`Active parameter set with id ${config.activeParameterSetId} not found in RunnerParams.json`);
    console.log('Available parameter sets:');
    config.parameterSets.forEach((ps: RunnerParameterSet) => {
      console.log(`  ${ps.id}: ${ps.description}`);
    });
    return;
  }

  console.log(`Using parameter set ${activeParameterSet.id}: ${activeParameterSet.description}`);
  
  const strategy = RunnerStrategyFactory.createStrategy(activeParameterSet.parameterSet);
  await new DeltaStorageRunner(strategy).run();
})();