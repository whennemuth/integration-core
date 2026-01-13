import { EndToEnd } from "../../src/EndToEnd";
import { MockDataMapper } from "./mock/MockDataMapper";
import { MockDataSource } from "./mock/MockDataSource";
import { MockDataTarget } from "./mock/MockDataTarget";
import { RandomData } from "./mock/RandomData";
import runnerConfig from './RunnerConfig.json';
import type { RunnerConfig, RunnerParameterSet } from './RunnerParams';
import { storagePath } from './RunnerParams';
import { RunnerStrategy } from "./RunnerStrategy";
import { RunnerStrategyFactory } from './RunnerStrategyFactory';

export class DeltaStorageRunner {

  constructor(private strategy: RunnerStrategy) { }

  public async run(): Promise<void> {
    const { strategy } = this;
    const { clientId, populationSize, simulatedPushFailureIndexes = [] } = strategy.parms;

    // Create a mock data source to generate test data
    const mockDataSource = new MockDataSource({ clientId, storagePath, generator: new RandomData(populationSize) });

    // Create a mock data mapper
    const mockDataMapper = new MockDataMapper();

    // Create a mock data target that simulates push failures for certain records
    const mockDataTarget = new MockDataTarget({ simulatedPushFailureIndexes });

    // Execute end-to-end data flow: fetch, compute delta, push, and store.
    await (new EndToEnd({
      dataSource: mockDataSource,
      dataMapper: mockDataMapper,
      dataTarget: mockDataTarget,
      deltaStrategy: strategy,
    })).execute();

    console.log('Test run complete.');
  }
}


/**
 * Main execution block for the DeltaStorageRunner test harness.
 * Set the activeParameterSetId in RunnerConfig.json to indicate the scenario you want to run.
 */
(async () => {
  const config = runnerConfig as RunnerConfig;
  
  // Find the active parameter set
  const activeParameterSet = config.parameterSets.find((ps: RunnerParameterSet) => ps.id === config.activeParameterSetId);
  
  // Handle case where active parameter set is not found
  if (!activeParameterSet) {
    console.error(`Active parameter set with id ${config.activeParameterSetId} not found in RunnerConfig.json`);
    console.log('Available parameter sets:');
    config.parameterSets.forEach((ps: RunnerParameterSet) => {
      console.log(`  ${ps.id}: ${ps.description}`);
    });
    return;
  }

  // Announce the active parameter set being used
  console.log(`Using parameter set ${activeParameterSet.id}: ${activeParameterSet.description}`);
  
  // Get the strategy for the active parameter set (file-based or database-based)
  const strategy = RunnerStrategyFactory.createStrategy(activeParameterSet.parameterSet);

  // Run the delta storage test
  await new DeltaStorageRunner(strategy).run();
})();