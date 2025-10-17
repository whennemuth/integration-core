import { BruteForceDeltaEngine, fishOutUpdatedRecordsByPK } from "../DeltaByBruteForce";
import { DeltaParms, DeltaStorage, FishingParms } from "../../DeltaTypes";
import { InputUtilsDecorator } from "../../InputUtils";
import { FileSystemDeltaStorage } from "./storage/FileStorage";
import { S3BucketDeltaStorage } from "./storage/S3Storage";
import { DeltaStrategy } from '../DeltaStrategy';
import { FileConfig } from "../DeltaStrategyParams";

/**
 * Abstract base class for file-based storage strategies (FileSystem and S3)
 * Provides shared brute force delta computation logic
 */
export abstract class FileBasedDeltaStrategy extends DeltaStrategy {
  
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
    const previous = await storage.fetchPreviousData({ clientId }) || [];

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
export class DeltaStrategyForFileSystem extends FileBasedDeltaStrategy {
  public get storage(): DeltaStorage {
    const { path } = this.parms.config as FileConfig;
    return new FileSystemDeltaStorage(path);
  }
}

/**
 * S3 bucket storage strategy implementation
 */
export class DeltaStrategyForS3Bucket extends FileBasedDeltaStrategy {
  
  public get storage(): DeltaStorage {
    const config = this.parms.config as any; // We know it's S3Config from factory
    return new S3BucketDeltaStorage({
      bucketName: config.bucketName,
      keyPrefix: config.keyPrefix || `test-datasets/${this.parms.clientId}`,
      region: config.region
    });
  }
}