import { PushAllParms } from "../../src/DataTarget";
import { FileSystemDeltaStorage } from "../../src/delta-storage/file/FileStorage";
import { S3BucketDeltaStorage } from "../../src/delta-storage/file/S3Storage";
import { BruteForceDeltaEngine, fishOutUpdatedRecordsByPK } from "../../src/DeltaByBruteForce";
import { DeltaParms, FileDeltaStorage, FishingParms } from "../../src/DeltaTypes";
import { InputParser } from "../../src/InputParser";
import { Field, FieldDefinition, FieldValidator } from "../../src/InputTypes";
import { InputUtilsDecorator } from "../../src/InputUtils";
import { BasicFieldValidator } from "../../src/InputValidation";
import { MockDataSource } from "./MockDataSource";
import { MockDataTarget } from "./MockDataTarget";
import { RandomData } from "./RandomData";

export class DeltaStorageRunner {

  constructor(private storageType: 'file' | 's3') { }

  public static readonly storagePath: string = './test/test-harness/storage';
  public static clientId: string = 'test-client';

  public async run(): Promise<void> {
    const { clientId, storagePath } = DeltaStorageRunner;
    const { storage } = this;

    // Create a mock data source to generate test data
    const mockDataSource = new MockDataSource({ clientId, storagePath, generator: new RandomData(1000) });

    // Create a mock data target that simulates push failures for certain records
    const mockDataTarget = new MockDataTarget({ simulatedPushFailureIndexes: [ 5, 10, 15 ] });

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

    // Reduce down to just the primary keys and the hash
    const reducedFieldSets = inputUtils.getReducedFieldSets();

    // Create delta engine
    const deltaEngine = new BruteForceDeltaEngine();

    // Fetch previous input from storage
    const previousInputFieldSets = await storage.fetchPreviousData(clientId) || [];

    // Define delta parameters
    const deltaParms = {
      data:{
        current: parsedInput.fieldSets,
        previous: previousInputFieldSets
      },
      fishOutTheUpdates: (parms:FishingParms) => {
        return fishOutUpdatedRecordsByPK(parms, inputUtils.getPrimaryKey());
      }
    } satisfies DeltaParms;

    // Compute delta
    const delta = await deltaEngine.computeDelta(deltaParms);

    // Push delta to data target
    const pushResult = await mockDataTarget.pushAll!(delta as PushAllParms);
    
    // Restore previous hashes for any records that failed to push
    inputUtils.restorePreviousHashesForFailures({ currentInputFieldSets: reducedFieldSets, previousInputFieldSets, pushResult });
    
    // Update storage with the new baseline data after successful processing
    await storage.updatePreviousData(clientId, reducedFieldSets);

    console.log('Test run complete.');
  }

  private get storage(): FileDeltaStorage {
    const { clientId, storagePath } = DeltaStorageRunner;
    switch (this.storageType) {
      case 'file':
        return new FileSystemDeltaStorage(storagePath);
      case 's3':
        return new S3BucketDeltaStorage({
          bucketName: 'integration-datasets',
          keyPrefix: `test-datasets/${clientId}`
        });
      default:
        throw new Error(`Unsupported storage type: ${this.storageType}`);
    }
  }
}

(async () => {
  await new DeltaStorageRunner('file').run();
})();