import { FileDeltaStorage } from '../../../DeltaTypes';
import { FieldSet } from '../../../InputTypes';
import { FileSystemStreamProvider } from './FileSystemStreamProvider';
import { NDJSONStreamProcessor, StreamProvider } from './StreamProvider';

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
   * @param storagePath - The directory path where data files will be stored
   * @param streamProvider - Optional custom StreamProvider (defaults to FileSystemStreamProvider)
   */
  constructor(storagePath: string, streamProvider?: StreamProvider) {
    if (!storagePath) {
      throw new Error('Storage path is required');
    }
    
    this.streamProvider = streamProvider || new FileSystemStreamProvider(storagePath);
    this.streamProcessor = new NDJSONStreamProcessor();
  }

  /**
   * Gets the file path for previous input data (NDJSON format)
   */
  private getPreviousInputPath(clientId: string): string {
    return `${clientId}/previous-input.ndjson`;
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
      
      // Check if resource exists
      const exists = await this.streamProvider.resourceExists(previousPath);
      if (!exists) {
        // No previous data exists yet
        return [];
      }

      // Create read stream and use the stream processor to read NDJSON data
      const readStream = await this.streamProvider.createReadStream(previousPath);
      if (!readStream) {
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
  public async updatePreviousData(params: { clientId: string, newPreviousData: FieldSet[], primaryKeyFields?: Set<string> }): Promise<any> {
    const { clientId, newPreviousData, primaryKeyFields } = params;
    if (!clientId) {
      throw new Error('clientId is required for updatePreviousData');
    }

    try {
      const previousPath = this.getPreviousInputPath(clientId);
      
      if (newPreviousData.length > 0) {
        // Store the new data as the updated previous input
        await this.streamProvider.ensureParent(previousPath);
        const writeStream = await this.streamProvider.createWriteStream(previousPath);
        await this.streamProcessor.writeFieldSets(writeStream, newPreviousData);
        
        return {
          status: 'success',
          message: `Updated previous input for client ${clientId}`,
          action: 'stored new baseline data',
          recordCount: newPreviousData.length,
          timestamp: new Date().toISOString()
        };
      } else {
        // No new data provided, this might be a cleanup operation
        const previousExists = await this.streamProvider.resourceExists(previousPath);
        if (previousExists) {
          await this.streamProvider.deleteResource(previousPath);
        }
        
        return {
          status: 'success',
          message: `Cleaned up previous input for client ${clientId}`,
          action: 'removed existing previous input',
          timestamp: new Date().toISOString()
        };
      }
    } catch (error) {
      throw new Error(`Failed to update previous input for client ${clientId}: ${error}`);
    }
  }


}
