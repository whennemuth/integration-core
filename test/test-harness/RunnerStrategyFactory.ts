import { DeltaStrategyForFileSystem, DeltaStrategyForS3Bucket } from '../../src/delta-strategy/file/RunnerStrategyForFile';
import { RunnerStrategyParms, isDatabaseConfig, isS3Config } from './RunnerParams';
import { RunnerStrategy, RunnerStrategyForFileSystem, S3BucketRunnerStrategy } from './RunnerStrategy';

/**
 * Factory class for creating appropriate RunnerStrategy instances
 * Separated from RunnerStrategy to avoid circular imports
 */
export class RunnerStrategyFactory {
  
  /**
   * Factory method to create appropriate strategy instance based on configuration
   */
  public static createStrategy(parms: RunnerStrategyParms): RunnerStrategy {
    if (!parms.config) {
      // Default to file system storage when no config is provided
      const baseStrategy = new DeltaStrategyForFileSystem(parms);
      return new RunnerStrategyForFileSystem(parms, baseStrategy);
    }
    
    if (isS3Config(parms.config)) {
      const baseStrategy = new DeltaStrategyForS3Bucket(parms);
      return new S3BucketRunnerStrategy(parms, baseStrategy);
    } 
    else if (isDatabaseConfig(parms.config)) {
      const baseStrategy = new DeltaStrategyForFileSystem(parms);
      return new RunnerStrategyForFileSystem(parms, baseStrategy);
    } 
    else {
      throw new Error(`Unsupported storage configuration`);
    }
  }
}

