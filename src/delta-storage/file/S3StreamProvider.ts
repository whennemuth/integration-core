// Note: This file requires '@aws-sdk/client-s3' to be installed as a dependency
// For AWS Lambda environments, you may need to include it in your deployment package
// For other environments, install with: npm install @aws-sdk/client-s3

import { Readable, Writable, PassThrough } from 'stream';
import { StreamProvider } from './StreamProvider';

// AWS SDK v3 type definitions
interface S3ClientConfig {
  region?: string;
  credentials?: {
    accessKeyId: string;
    secretAccessKey: string;
  };
  [key: string]: any;
}

interface S3Object {
  Key?: string;
  [key: string]: any;
}

interface ListObjectsV2Output {
  Contents?: S3Object[];
  [key: string]: any;
}

// AWS SDK v3 command interfaces
interface S3Commands {
  HeadObjectCommand: any;
  GetObjectCommand: any;
  PutObjectCommand: any;
  CopyObjectCommand: any;
  DeleteObjectCommand: any;
  ListObjectsV2Command: any;
}

interface S3ClientInterface {
  send(command: any): Promise<any>;
}

interface UploadInterface {
  done(): Promise<any>;
}

/**
 * S3 implementation of StreamProvider for AWS Lambda environments
 * Handles S3 bucket operations using AWS SDK streaming capabilities
 */
export class S3StreamProvider implements StreamProvider {
  private readonly s3: S3ClientInterface;
  private readonly bucketName: string;
  private readonly keyPrefix: string;
  private readonly commands: S3Commands;
  private readonly upload: any; // @aws-sdk/lib-storage Upload class

  constructor(bucketName: string, keyPrefix: string = '', s3Config?: S3ClientConfig) {
    if (!bucketName) {
      throw new Error('S3 bucket name is required');
    }
    
    this.bucketName = bucketName;
    this.keyPrefix = keyPrefix.endsWith('/') ? keyPrefix : keyPrefix + '/';
    
    // Dynamically import and instantiate S3 client (SDK v3)
    // This allows the code to compile without @aws-sdk/client-s3 being installed at build time
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { S3Client } = require('@aws-sdk/client-s3');
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { 
        HeadObjectCommand, 
        GetObjectCommand, 
        PutObjectCommand, 
        CopyObjectCommand, 
        DeleteObjectCommand, 
        ListObjectsV2Command 
      } = require('@aws-sdk/client-s3');
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const { Upload } = require('@aws-sdk/lib-storage');
      
      this.s3 = new S3Client(s3Config);
      this.commands = {
        HeadObjectCommand,
        GetObjectCommand,
        PutObjectCommand,
        CopyObjectCommand,
        DeleteObjectCommand,
        ListObjectsV2Command
      };
      this.upload = Upload;
    } catch (error) {
      throw new Error('AWS SDK v3 is required but not found. Install with: npm install @aws-sdk/client-s3 @aws-sdk/lib-storage');
    }
  }

  private getFullKey(resourcePath: string): string {
    return this.keyPrefix + resourcePath.replace(/^\/+/, '');
  }

  async createReadStream(resourcePath: string): Promise<Readable | null> {
    const key = this.getFullKey(resourcePath);
    
    try {
      // First check if object exists
      const headCommand = new this.commands.HeadObjectCommand({ 
        Bucket: this.bucketName, 
        Key: key 
      });
      await this.s3.send(headCommand);
      
      // Object exists, create read stream
      const getCommand = new this.commands.GetObjectCommand({ 
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
    const upload = new this.upload({
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
    const copyCommand = new this.commands.CopyObjectCommand({
      Bucket: this.bucketName,
      CopySource: `${this.bucketName}/${sourceKey}`,
      Key: destKey
    });
    await this.s3.send(copyCommand);
    
    // Delete original object
    const deleteCommand = new this.commands.DeleteObjectCommand({
      Bucket: this.bucketName,
      Key: sourceKey
    });
    await this.s3.send(deleteCommand);
  }

  async deleteResource(resourcePath: string): Promise<void> {
    const key = this.getFullKey(resourcePath);
    
    try {
      const deleteCommand = new this.commands.DeleteObjectCommand({
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
      const headCommand = new this.commands.HeadObjectCommand({
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
    
    const listCommand = new this.commands.ListObjectsV2Command({
      Bucket: this.bucketName,
      Prefix: fullPrefix
    });
    const result: ListObjectsV2Output = await this.s3.send(listCommand);
    
    return (result.Contents || [])
      .map((obj: S3Object) => obj.Key!)
      .map((key: string) => key.replace(this.keyPrefix, ''));
  }

  /**
   * S3-specific helper: Get object metadata
   */
  async getResourceMetadata(resourcePath: string): Promise<any> {
    const key = this.getFullKey(resourcePath);
    
    const headCommand = new this.commands.HeadObjectCommand({
      Bucket: this.bucketName,
      Key: key
    });
    return this.s3.send(headCommand);
  }
}