import * as fs from 'fs/promises';
import { createWriteStream, createReadStream } from 'fs';
import * as path from 'path';
import { Readable, Writable } from 'stream';
import { StreamProvider } from './StreamProvider';

/**
 * File system implementation of StreamProvider
 * Handles local file system operations using Node.js fs module
 */
export class FileSystemStreamProvider implements StreamProvider {
  private readonly basePath: string;

  constructor(basePath: string) {
    if (!basePath) {
      throw new Error('Base path is required');
    }
    this.basePath = path.resolve(basePath);
  }

  async createReadStream(resourcePath: string): Promise<Readable | null> {
    const fullPath = path.join(this.basePath, resourcePath);
    
    try {
      await fs.access(fullPath);
      return createReadStream(fullPath, { encoding: 'utf-8' });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return null; // File doesn't exist
      }
      throw error;
    }
  }

  async createWriteStream(resourcePath: string): Promise<Writable> {
    const fullPath = path.join(this.basePath, resourcePath);
    await this.ensureParent(resourcePath);
    return createWriteStream(fullPath, { encoding: 'utf-8' });
  }

  async moveResource(sourcePath: string, destinationPath: string): Promise<void> {
    const sourceFullPath = path.join(this.basePath, sourcePath);
    const destFullPath = path.join(this.basePath, destinationPath);
    await fs.rename(sourceFullPath, destFullPath);
  }

  async deleteResource(resourcePath: string): Promise<void> {
    const fullPath = path.join(this.basePath, resourcePath);
    try {
      await fs.unlink(fullPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        throw error;
      }
      // Ignore if file doesn't exist
    }
  }

  async resourceExists(resourcePath: string): Promise<boolean> {
    const fullPath = path.join(this.basePath, resourcePath);
    try {
      await fs.access(fullPath);
      return true;
    } catch {
      return false;
    }
  }

  async ensureParent(resourcePath: string): Promise<void> {
    const fullPath = path.join(this.basePath, resourcePath);
    const dirPath = path.dirname(fullPath);
    await fs.mkdir(dirPath, { recursive: true });
  }
}