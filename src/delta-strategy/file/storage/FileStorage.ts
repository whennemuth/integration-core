import { FileDeltaStorage } from '../../../DeltaTypes';
import { FieldSet } from '../../../InputTypes';
import { FileSystemStreamProvider } from './FileSystemStreamProvider';
import { NDJSONStreamProcessor, StreamProvider } from './StreamProvider';

export const PREVIOUS_INPUT_FILENAME = 'previous-input.ndjson';

/**
 * @param storagePath - The directory path where data files will be stored
 * @param streamProvider - Optional custom StreamProvider (defaults to FileSystemStreamProvider)
 */
export type FileSystemDeltaStorageParams = {
  storagePath: string, 
  streamProvider?: StreamProvider,
  outputPath?: (baseName: string) => string;
};

/**
 * File-based implementation of FileDeltaStorage that stores only previous data as NDJSON (Newline Delimited JSON) 
 * files in a designated directory using streaming I/O for better performance with large datasets.
 * Each client gets its own subdirectory for organization.
 * 
 * Current data is obtained from live DataSource, only previous data is stored for delta computation.
 * Uses dependency injection with StreamProvider for storage-agnostic streaming operations.
 */
export class FileSystemDeltaStorage implements FileDeltaStorage {
  public readonly name = 'File System Delta Storage';
  public readonly description = 'Stores delta data as NDJSON files using streaming I/O for optimal performance';

  private readonly streamProvider: StreamProvider;
  private readonly streamProcessor: NDJSONStreamProcessor;

  /**
   * Creates a new FileSystemDeltaStorage instance
   * @param parms - Parameters for configuring the file system storage
   */
  constructor(private parms: FileSystemDeltaStorageParams) {
    const { storagePath, streamProvider } = parms;
    if (!storagePath) {
      throw new Error('Storage path is required');
    }
    
    this.streamProvider = streamProvider || new FileSystemStreamProvider(storagePath);
    this.streamProcessor = new NDJSONStreamProcessor();
  }

  /**
   * Gets the file path for previous input data (NDJSON format)
   */
  private getPreviousInputPath = (clientId: string): string => {
    return `${clientId}/${PREVIOUS_INPUT_FILENAME}`;
  }

  /**
   * Technically this function should return a value equal to the baseName since is simply 
   * referring to the name of a file that will act as its replacement. But  this allows for 
   * custom output key prefixes in case the new file is actually representing a subset of
   * the original file and will be merged later to form the "true" previous input file.
   * @param clientId 
   * @returns 
   */
  private getNewPreviousInputKey = (clientId: string): string => {
    const { getPreviousInputPath, parms: { outputPath } } = this;
    const previousKeyBase = getPreviousInputPath(clientId);
    return outputPath ? outputPath(previousKeyBase) : previousKeyBase;
  }

  /**
   * Fetches the previous input data from the file system using streaming NDJSON
   */
  public async fetchPreviousData(params: { clientId: string, limitTo?: FieldSet[] }): Promise<FieldSet[]> {
    const { clientId, limitTo } = params;
    if (!clientId) {
      throw new Error('clientId is required for fetchPreviousData');
    }

    try {
      const previousPath = this.getPreviousInputPath(clientId);

      console.log(`Fetching previous data for client ${clientId} from path: ${previousPath}`);
      
      // Check if resource exists
      const exists = await this.streamProvider.resourceExists(previousPath);
      if (!exists) {
        // No previous data exists yet
        console.log(`No previous data found for client ${clientId} at path: ${previousPath}`);
        return [];
      }

      // Create read stream and use the stream processor to read NDJSON data
      const readStream = await this.streamProvider.createReadStream(previousPath);
      if (!readStream) {
        console.log(`Failed to create read stream for previous data of client ${clientId} at path: ${previousPath}`);
        return [];
      }
      return await this.streamProcessor.readFieldSets(readStream);
    } catch (error) {
      throw new Error(`Failed to fetch previous input for client ${clientId}: ${error}`);
    }
  }



  /**
   * Stores new data as the updated previous input data after successful delta processing.
   * In file-based storage, this is called after a successful push to update the baseline
   * for the next delta computation.
   */
  public async updatePreviousData(params: { clientId: string, newPreviousData: FieldSet[], primaryKeyFields?: Set<string>, cleanup?: boolean }): Promise<any> {
    const { clientId, newPreviousData, primaryKeyFields, cleanup = true } = params;
    if (!clientId) {
      throw new Error('clientId is required for updatePreviousData');
    }

    try {
      const previousPath = this.getNewPreviousInputKey(clientId);
      console.log(`Updating previous data for client ${clientId} at path: ${previousPath}`);
      
      if (newPreviousData.length > 0) {
        console.log(`Storing ${newPreviousData.length} records as new previous data for client ${clientId} at path: ${previousPath}`);

        // Store the new data as the updated previous input
        await this.streamProvider.ensureParent(previousPath);
        const writeStream = await this.streamProvider.createWriteStream(previousPath);
        await this.streamProcessor.writeFieldSets(writeStream, newPreviousData);
        
        const retval = {
          status: 'success',
          message: `Updated previous input for client ${clientId}`,
          action: 'stored new baseline data',
          recordCount: newPreviousData.length,
          timestamp: new Date().toISOString()
        };
        console.log(`✓ ${JSON.stringify(retval)} at path: ${previousPath}`);
        return retval;
      } else {
        console.log(`No new data provided for client ${clientId}.`);
        
        if (cleanup) {
          const previousExists = await this.streamProvider.resourceExists(previousPath);
          if (previousExists) {
            console.log(`Cleaning up previous data for client ${clientId} at path: ${previousPath}`);
            await this.streamProvider.deleteResource(previousPath);
          }
          
          const retval = {
            status: 'success',
            message: `Cleaned up previous input for client ${clientId}`,
            action: 'removed existing previous input',
            timestamp: new Date().toISOString()
          };
          console.log(`✓ ${JSON.stringify(retval)} at path: ${previousPath}`);
          return retval;
        } else {
          console.log(`Cleanup skipped - delta files preserved`);
          
          const retval = {
            status: 'success',
            message: `Baseline update skipped for client ${clientId} (no new data)`,
            action: 'delta files preserved',
            timestamp: new Date().toISOString()
          };
          console.log(`✓ ${JSON.stringify(retval)} at path: ${previousPath}`);
          return retval;
        }
      }
    } catch (error) {
      throw new Error(`Failed to update previous input for client ${clientId}: ${error}`);
    }
  }


}
