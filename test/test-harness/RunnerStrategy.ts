import { DeltaStrategyForDatabase } from "../../src/delta-strategy/db/RunnerStrategyForDatabase";
import { DeltaStrategy } from "../../src/delta-strategy/DeltaStrategy";
import { DeltaStrategyForFileSystem, DeltaStrategyForS3Bucket } from "../../src/delta-strategy/file/RunnerStrategyForFile";
import { FileSystemDeltaStorage } from "../../src/delta-strategy/file/storage/FileStorage";
import { DeltaStorage } from "../../src/DeltaTypes";
import { InputUtilsDecorator } from "../../src/InputUtils";
import { DatabaseConfig, RunnerStrategyParms, storagePath } from "./RunnerParams";
import { sqlLiteDbFileAbsolutePath } from "./SqlLiteDb";

/**
 * Abstract decorator class of DeltaStrategy for RunnerStrategy implementations
 */
export abstract class RunnerStrategy extends DeltaStrategy {
  constructor(public parms: RunnerStrategyParms, private baseStrategy: DeltaStrategy) { 
    super(parms);
  }
  public computeDelta(computeParms: { storage: DeltaStorage; currentFieldSets: any[]; inputUtils: InputUtilsDecorator; clientId: string; }): Promise<any> {
    return this.baseStrategy.computeDelta(computeParms);
  }
  public get storage(): DeltaStorage {
    return this.baseStrategy.storage;
  }
}

/**
 * Decorator class of DeltaStrategyForFileSystem for RunnerStrategyForFileSystem implementation
 */
export class RunnerStrategyForFileSystem extends RunnerStrategy {
  constructor(parms: RunnerStrategyParms, baseStrategy: DeltaStrategyForFileSystem) {
    super(parms, baseStrategy);
  }
  public get storage(): DeltaStorage {
    return new FileSystemDeltaStorage(storagePath);
  }
}

/**
 * Decorator class of DeltaStrategyForS3Bucket for S3BucketRunnerStrategy implementation
 */
export class S3BucketRunnerStrategy extends RunnerStrategy {
  constructor(parms: RunnerStrategyParms, baseStrategy: DeltaStrategyForS3Bucket) {
    super(parms, baseStrategy);
  }
}

/**
 * Decorator class of DeltaStrategyForDatabase for RunnerStrategyForDatabase implementation
 */
export class RunnerStrategyForDatabase extends RunnerStrategy {
  constructor(parms: RunnerStrategyParms, baseStrategy: DeltaStrategyForDatabase) {
    super(parms, baseStrategy);
    if( ! parms) return;
    let { config = {} } = this.parms || {};
    if('type' in config && config.type === 'sqlite') {
      (this.parms.config as DatabaseConfig).filename = sqlLiteDbFileAbsolutePath;
    }
  }
}