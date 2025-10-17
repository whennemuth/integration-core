import {
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2Output,
  S3Client,
  S3ClientConfig
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { PassThrough, Readable, Writable } from 'stream';
import { resolveAwsRegion } from '../../../Utils';
import { StreamProvider } from './StreamProvider';

// S3Config type (local definition to avoid circular imports)
type S3ConfigLike = {
  bucketName?: string;
  keyPrefix?: string;
  region?: string;
}



/**
 * S3 implementation of StreamProvider for AWS Lambda environments
 * Handles S3 bucket operations using AWS SDK streaming capabilities
 */
export class S3StreamProvider implements StreamProvider {
  private readonly s3: S3Client;
  private readonly bucketName: string;
  private readonly keyPrefix: string;

  constructor(
    bucketNameOrConfig: string | S3ConfigLike,
    keyPrefix: string = '',
    s3Config?: S3ClientConfig
  ) {
    // Handle both old signature and new S3Config-style usage
    if (typeof bucketNameOrConfig === 'string') {
      // Old signature: constructor(bucketName, keyPrefix, s3Config)
      this.bucketName = bucketNameOrConfig;
      this.keyPrefix = keyPrefix.endsWith('/') ? keyPrefix : keyPrefix + '/';
      
      // Resolve region from environment if not explicitly provided
      const resolvedRegion = resolveAwsRegion();
      const clientConfig: S3ClientConfig = { ...s3Config };
      if (resolvedRegion && !clientConfig.region) {
        clientConfig.region = resolvedRegion;
      }
      
      this.s3 = new S3Client(clientConfig);
    } else {
      // New signature: constructor(s3ConfigLike)
      const config = bucketNameOrConfig;
      if (!config.bucketName) {
        throw new Error('S3 bucket name is required');
      }
      
      this.bucketName = config.bucketName;
      this.keyPrefix = (config.keyPrefix || '').endsWith('/') 
        ? (config.keyPrefix || '') 
        : (config.keyPrefix || '') + '/';
      
      // Resolve region with priority order
      const resolvedRegion = resolveAwsRegion(config);
      const clientConfig: S3ClientConfig = { ...s3Config };
      if (resolvedRegion) {
        clientConfig.region = resolvedRegion;
      }
      
      this.s3 = new S3Client(clientConfig);
    }
    
    if (!this.bucketName) {
      throw new Error('S3 bucket name is required');
    }
  }

  private getFullKey(resourcePath: string): string {
    return this.keyPrefix + resourcePath.replace(/^\/+/, '');
  }

  async createReadStream(resourcePath: string): Promise<Readable | null> {
    const key = this.getFullKey(resourcePath);
    
    try {
      // First check if object exists
      const headCommand = new HeadObjectCommand({ 
        Bucket: this.bucketName, 
        Key: key 
      });
      await this.s3.send(headCommand);
      
      // Object exists, create read stream
      const getCommand = new GetObjectCommand({ 
        Bucket: this.bucketName, 
        Key: key 
      });
      const response = await this.s3.send(getCommand);
      return response.Body as Readable;
      
    } catch (error) {
      if ((error as any).name === 'NotFound' || (error as any).name === 'NoSuchKey') {
        return null; // Object doesn't exist
      }
      throw error;
    }
  }

  async createWriteStream(resourcePath: string): Promise<Writable> {
    const key = this.getFullKey(resourcePath);
    await this.ensureParent(resourcePath);
    
    // Create a PassThrough stream that we'll pipe to S3 upload
    const passThrough = new PassThrough();
    
    // Start the S3 upload using @aws-sdk/lib-storage
    const upload = new Upload({
      client: this.s3,
      params: {
        Bucket: this.bucketName,
        Key: key,
        Body: passThrough,
        ContentType: 'application/x-ndjson'
      }
    });

    // Handle upload completion/errors
    upload.done().catch((error: any) => {
      passThrough.destroy(error);
    });

    return passThrough;
  }

  async moveResource(sourcePath: string, destinationPath: string): Promise<void> {
    const sourceKey = this.getFullKey(sourcePath);
    const destKey = this.getFullKey(destinationPath);
    
    // Copy object to new location
    const copyCommand = new CopyObjectCommand({
      Bucket: this.bucketName,
      CopySource: `${this.bucketName}/${sourceKey}`,
      Key: destKey
    });
    await this.s3.send(copyCommand);
    
    // Delete original object
    const deleteCommand = new DeleteObjectCommand({
      Bucket: this.bucketName,
      Key: sourceKey
    });
    await this.s3.send(deleteCommand);
  }

  async deleteResource(resourcePath: string): Promise<void> {
    const key = this.getFullKey(resourcePath);
    
    try {
      const deleteCommand = new DeleteObjectCommand({
        Bucket: this.bucketName,
        Key: key
      });
      await this.s3.send(deleteCommand);
    } catch (error) {
      // S3 delete doesn't fail if object doesn't exist, but handle any other errors
      if ((error as any).name !== 'NoSuchKey') {
        throw error;
      }
    }
  }

  async resourceExists(resourcePath: string): Promise<boolean> {
    const key = this.getFullKey(resourcePath);
    
    try {
      const headCommand = new HeadObjectCommand({
        Bucket: this.bucketName,
        Key: key
      });
      await this.s3.send(headCommand);
      return true;
    } catch (error) {
      if ((error as any).name === 'NotFound' || (error as any).name === 'NoSuchKey') {
        return false;
      }
      throw error;
    }
  }

  async ensureParent(resourcePath: string): Promise<void> {
    // S3 doesn't require explicit directory creation
    // The key prefix structure handles the "directory" concept
    // This is a no-op for S3 but maintains interface compatibility
  }

  /**
   * S3-specific helper: List all objects with a given prefix
   */
  async listResources(prefix: string = ''): Promise<string[]> {
    const fullPrefix = this.getFullKey(prefix);
    
    const listCommand = new ListObjectsV2Command({
      Bucket: this.bucketName,
      Prefix: fullPrefix
    });
    const result: ListObjectsV2Output = await this.s3.send(listCommand);
    
    return (result.Contents || [])
      .map((obj: any) => obj.Key!)
      .map((key: string) => key.replace(this.keyPrefix, ''));
  }

  /**
   * S3-specific helper: Get object metadata
   */
  async getResourceMetadata(resourcePath: string): Promise<any> {
    const key = this.getFullKey(resourcePath);
    
    const headCommand = new HeadObjectCommand({
      Bucket: this.bucketName,
      Key: key
    });
    return this.s3.send(headCommand);
  }
}