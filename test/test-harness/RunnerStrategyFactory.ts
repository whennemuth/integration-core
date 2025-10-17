import { DeltaStorageStrategyParms, isDatabaseConfig, isS3Config } from './RunnerParams';
import { RunnerStrategy } from './RunnerStrategy';
import { RunnerStrategyForFileSystem, RunnerStrategyForS3Bucket } from './RunnerStrategyForFile';
import { RunnerStrategyForDatabase } from './RunnerStrategyForDatabase';

/**
 * Factory class for creating appropriate RunnerStrategy instances
 * Separated from RunnerStrategy to avoid circular imports
 */
export class RunnerStrategyFactory {
  
  /**
   * Factory method to create appropriate strategy instance based on configuration
   */
  public static createStrategy(parms: DeltaStorageStrategyParms): RunnerStrategy {
    if (!parms.config) {
      // Default to file system storage when no config is provided
      return new RunnerStrategyForFileSystem(parms);
    }
    
    if (isS3Config(parms.config)) {
      return new RunnerStrategyForS3Bucket(parms);
    } 
    else if (isDatabaseConfig(parms.config)) {
      return new RunnerStrategyForDatabase(parms);
    } 
    else {
      throw new Error(`Unsupported storage configuration`);
    }
  }
}