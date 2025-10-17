/**
 * DeltaStrategyParams.ts - Configuration types and constants for an End-to-End delta cycle.
 *
 * This module defines the type system and global constants used to configure and 
 * execute the End-to-End delta cycle across different storage backends
 * (file system, S3, and database storage).
 */

/**
 * Database configuration for delta storage
 * Supports SQLite (in-memory and file-based), PostgreSQL, and MySQL databases.
 * Used to configure database connections and behavior for delta storage operations.
 */
export type DatabaseConfig = {
  type: 'sqlite' | 'postgresql' | 'mysql';
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  database?: string;
  filename?: string; // For SQLite file databases
  ssl?: boolean;
  synchronize?: boolean;
  logging?: boolean;
};

/**
 * File-based configuration for local delta storage
 */
export type FileConfig = {
  path: string;
};

/**
 * S3 configuration for cloud-based delta storage
 * Configures AWS S3 bucket settings for storing delta data in the cloud.
 */
export type S3Config = {
  bucketName: string;
  keyPrefix?: string;
  region?: string;
};

/**
 * Parameters for configuring an End-to-End delta cycle.
 * Contains test configuration including client ID, data size, failure simulation,
 * and optional storage backend configuration.
 */
export type DeltaStrategyParams = {
  clientId: string;
  config?: DatabaseConfig | S3Config | FileConfig;
};

/**
 * Type guard to check if config is DatabaseConfig
 */
export const isDatabaseConfig = (config: DatabaseConfig | S3Config | FileConfig): config is DatabaseConfig => {
  return 'type' in config && ['sqlite', 'postgresql', 'mysql'].includes((config as DatabaseConfig).type);
};

/**
 * Type guard to check if config is S3Config
 */
export const isS3Config = (config: DatabaseConfig | S3Config | FileConfig): config is S3Config => {
  return 'bucketName' in config;
};

