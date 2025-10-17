import { BruteForceDeltaEngine, fishOutUpdatedRecordsByPK } from "../../src/DeltaByBruteForce";
import { DeltaParms, DeltaStorage, FishingParms } from "../../src/DeltaTypes";
import { InputUtilsDecorator } from "../../src/InputUtils";
import { FileSystemDeltaStorage } from "../../src/delta-storage/file/FileStorage";
import { S3BucketDeltaStorage } from "../../src/delta-storage/file/S3Storage";
import { RunnerStrategy } from './RunnerStrategy';
import { storagePath } from './RunnerParams';

/**
 * Abstract base class for file-based storage strategies (FileSystem and S3)
 * Provides shared brute force delta computation logic
 */
export abstract class FileBasedRunnerStrategy extends RunnerStrategy {
  
  public async computeDelta(computeParms: {
    storage: DeltaStorage, 
    currentFieldSets: any[], 
    inputUtils: InputUtilsDecorator, 
    clientId: string
  }): Promise<any> {
    const { storage, currentFieldSets, inputUtils, clientId } = computeParms;
    // File-based storage: use brute force delta computation
    const deltaEngine = new BruteForceDeltaEngine();

    // Fetch previous input from storage (no limitTo needed for delta computation)
    const previous = await storage.fetchPreviousData(clientId) || [];

    // Define delta parameters
    const deltaParms = {
      data: { current: currentFieldSets, previous },
      fishOutTheUpdates: (parms: FishingParms) => {
        return fishOutUpdatedRecordsByPK(parms, inputUtils.getPrimaryKey());
      }
    } satisfies DeltaParms;

    // Compute delta
    return await deltaEngine.computeDelta(deltaParms);
  }
}

/**
 * File system storage strategy implementation
 */
export class RunnerStrategyForFileSystem extends FileBasedRunnerStrategy {
  
  public get storage(): DeltaStorage {
    return new FileSystemDeltaStorage(storagePath);
  }
}

/**
 * S3 bucket storage strategy implementation
 */
export class RunnerStrategyForS3Bucket extends FileBasedRunnerStrategy {
  
  public get storage(): DeltaStorage {
    const config = this.parms.config as any; // We know it's S3Config from factory
    return new S3BucketDeltaStorage({
      bucketName: config.bucketName,
      keyPrefix: config.keyPrefix || `test-datasets/${this.parms.clientId}`,
      region: config.region
    });
  }
}