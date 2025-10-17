import { FieldSet } from '../../../InputTypes';
import { Readable, Writable } from 'stream';

/**
 * Abstract interface for stream-based storage operations
 * Can be implemented for file systems, S3, Azure Blob, etc.
 */
export interface StreamProvider {
  /**
   * Creates a readable stream for the specified resource
   * @param resourcePath - The path/key identifier for the resource
   * @returns Promise<Readable | null> - Returns null if resource doesn't exist
   */
  createReadStream(resourcePath: string): Promise<Readable | null>;

  /**
   * Creates a writable stream for the specified resource
   * @param resourcePath - The path/key identifier for the resource
   * @returns Promise<Writable>
   */
  createWriteStream(resourcePath: string): Promise<Writable>;

  /**
   * Moves/renames a resource from source to destination
   * @param sourcePath - Source resource identifier
   * @param destinationPath - Destination resource identifier
   */
  moveResource(sourcePath: string, destinationPath: string): Promise<void>;

  /**
   * Deletes a resource
   * @param resourcePath - The resource identifier to delete
   */
  deleteResource(resourcePath: string): Promise<void>;

  /**
   * Checks if a resource exists
   * @param resourcePath - The resource identifier to check
   */
  resourceExists(resourcePath: string): Promise<boolean>;

  /**
   * Ensures the parent directory/container exists
   * @param resourcePath - The resource path to ensure parent exists for
   */
  ensureParent(resourcePath: string): Promise<void>;
}

/**
 * Generic NDJSON streaming operations that work with any StreamProvider
 * This class contains all the streaming logic and is storage-agnostic
 */
export class NDJSONStreamProcessor {
  
  /**
   * Reads FieldSet array from NDJSON stream
   */
  async readFieldSets(readStream: Readable): Promise<FieldSet[]> {
    return new Promise((resolve, reject) => {
      const fieldSets: FieldSet[] = [];
      const readline = require('readline');
      
      const rl = readline.createInterface({
        input: readStream,
        crlfDelay: Infinity
      });

      rl.on('line', (line: string) => {
        if (line.trim()) {
          try {
            const fieldSet = JSON.parse(line) as FieldSet;
            fieldSets.push(fieldSet);
          } catch (parseError) {
            reject(new Error(`Failed to parse NDJSON line: ${parseError}`));
          }
        }
      });

      rl.on('close', () => {
        resolve(fieldSets);
      });

      rl.on('error', (error: Error) => {
        reject(new Error(`Failed to read NDJSON stream: ${error}`));
      });
    });
  }

  /**
   * Writes FieldSet array to NDJSON stream with backpressure handling
   */
  async writeFieldSets(writeStream: Writable, fieldSets: FieldSet[]): Promise<number> {
    return new Promise((resolve, reject) => {
      let recordsWritten = 0;

      writeStream.on('error', (error) => {
        reject(new Error(`Failed to write NDJSON stream: ${error}`));
      });

      writeStream.on('finish', () => {
        resolve(recordsWritten);
      });

      const writeNextRecord = (index: number) => {
        if (index >= fieldSets.length) {
          writeStream.end();
          return;
        }

        try {
          const fieldSet = fieldSets[index];
          const jsonLine = JSON.stringify(fieldSet, (key, value) => {
            // If it's a Map and it's empty, return undefined to exclude it
            if (value instanceof Map && value.size === 0) {
              return undefined;
            }
            // Convert non-empty Maps to objects
            if (value instanceof Map) {
              return Object.fromEntries(value);
            }
            return value;
          }) + '\n';
          
          if (writeStream.write(jsonLine)) {
            // Can continue writing immediately
            recordsWritten++;
            setImmediate(() => writeNextRecord(index + 1));
          } else {
            // Handle backpressure - wait for drain event
            writeStream.once('drain', () => {
              recordsWritten++;
              writeNextRecord(index + 1);
            });
          }
        } catch (stringifyError) {
          writeStream.destroy();
          reject(new Error(`Failed to stringify FieldSet: ${stringifyError}`));
        }
      };

      // Start writing records
      writeNextRecord(0);
    });
  }
}