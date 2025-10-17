import { FileDeltaStorage } from '../../DeltaTypes';
import { FieldSet } from '../../InputTypes';
import { S3StreamProvider } from './S3StreamProvider';
import { NDJSONStreamProcessor, StreamProvider } from './StreamProvider';

export const PREVIOUS_INPUT_FILENAME = 'previous-input.ndjson';

/**
 * S3-based implementation of FileDeltaStorage that stores only previous data as NDJSON (Newline Delimited JSON) 
 * files in an S3 bucket using streaming I/O for better performance with large datasets.
 * Each client gets its own key prefix for organization.
 * 
 * Current data is obtained from live DataSource, only previous data is stored for delta computation.
 * Optimized for AWS Lambda environments with efficient streaming operations.
 */
export class S3BucketDeltaStorage implements FileDeltaStorage {
  public readonly name = 'S3 Bucket Delta Storage';
  public readonly description = 'Stores delta data as NDJSON files in S3 using streaming I/O for optimal Lambda performance';

  private readonly streamProvider: StreamProvider;
  private readonly streamProcessor: NDJSONStreamProcessor;

  /**
   * Creates a new S3BucketDeltaStorage instance
   * @param bucketName - The S3 bucket name where data files will be stored
   * @param keyPrefix - Optional key prefix for organization (defaults to empty)
   * @param s3Config - Optional S3 client configuration
   * @param region - Optional AWS region (will be resolved automatically if not provided)
   * @param streamProvider - Optional custom StreamProvider (defaults to S3StreamProvider)
   */
  constructor(parms: {
    bucketName: string, 
    keyPrefix?: string, 
    s3Config?: any,
    region?: string,
    streamProvider?: StreamProvider
  }) {
    const { bucketName, keyPrefix = '', s3Config, region, streamProvider } = parms;
    if (!bucketName) {
      throw new Error('S3 bucket name is required');
    }
    
    // Create S3StreamProvider with new S3Config-like approach
    this.streamProvider = streamProvider || new S3StreamProvider({
      bucketName,
      keyPrefix,
      region
    }, '', s3Config);
    this.streamProcessor = new NDJSONStreamProcessor();
  }

  /**
   * Fetches the previous input data from S3 using streaming NDJSON
   */
  public async fetchPreviousData(clientId: string): Promise<FieldSet[]> {
    if (!clientId) {
      throw new Error('clientId is required for fetchPreviousInput');
    }

    try {
      // Check if resource exists
      const exists = await this.streamProvider.resourceExists(PREVIOUS_INPUT_FILENAME);
      if (!exists) {
        // No previous data exists yet
        return [];
      }

      // Create read stream and use the stream processor to read NDJSON data
      const readStream = await this.streamProvider.createReadStream(PREVIOUS_INPUT_FILENAME);
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
   * In S3-based storage, this is called after a successful push to update the baseline
   * for the next delta computation.
   */
  public async updatePreviousData(clientId: string, newPreviousData: FieldSet[], primaryKeyFields?: Set<string>): Promise<any> {
    if (!clientId) {
      throw new Error('clientId is required for updatePreviousData');
    }

    try {
      
      if (newPreviousData.length > 0) {
        // Store the new data as the updated previous input
        await this.streamProvider.ensureParent(PREVIOUS_INPUT_FILENAME);
        const writeStream = await this.streamProvider.createWriteStream(PREVIOUS_INPUT_FILENAME);
        await this.streamProcessor.writeFieldSets(writeStream, newPreviousData);
        
        return {
          status: 'success',
          message: `Updated previous input for client ${clientId}`,
          action: 'stored new baseline data',
          recordCount: newPreviousData.length,
          storage: 's3',
          timestamp: new Date().toISOString()
        };
      } else {
        // No new data provided, this might be a cleanup operation
        const previousExists = await this.streamProvider.resourceExists(PREVIOUS_INPUT_FILENAME);
        if (previousExists) {
          await this.streamProvider.deleteResource(PREVIOUS_INPUT_FILENAME);
        }
        
        return {
          status: 'success',
          message: `Cleaned up previous input for client ${clientId}`,
          action: 'removed existing previous input',
          storage: 's3',
          timestamp: new Date().toISOString()
        };
      }
    } catch (error) {
      throw new Error(`Failed to update previous input for client ${clientId}: ${error}`);
    }
  }

  /**
   * S3-specific helper: List all client IDs that have data stored
   */
  public async listClients(): Promise<string[]> {
    if ('listResources' in this.streamProvider) {
      const resources = await (this.streamProvider as any).listResources();
      const clients = new Set<string>();
      
      resources.forEach((resource: string) => {
        const clientId = resource.split('/')[0];
        if (clientId) {
          clients.add(clientId);
        }
      });
      
      return Array.from(clients);
    }
    
    throw new Error('listClients operation not supported by current stream provider');
  }
}