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
  /** Technically, this function should return a value equal to path, but allows for custom output paths */
  outputPath?: (baseName: string) => string;
};

/**
 * S3 configuration for cloud-based delta storage
 * Configures AWS S3 bucket settings for storing delta data in the cloud.
 */
export type S3Config = {
  bucketName: string;
  keyPrefix?: string;
  /** Technically, this function should return a value equal to keyPrefix, but allows for custom output key prefixes */
  outputKeyPrefix?: (baseName: string) => string;
  region?: string;
};

/**
 * DynamoDB configuration for delta storage
 * Configures AWS DynamoDB tables for storing person hash state and history.
 * Table names should be provided by the infrastructure layer (fargate project).
 */
export type DynamoDBConfig = {
  region: string;
  personCurrentStateTableName: string;
  personHistoryTableName: string;
  currentStateGSIName?: string;
  /** The actual sync run's ID (e.g. the chunk directory's ISO timestamp), stored on every
   *  PersonCurrentState/PersonHistory record written during this run. Falls back to a
   *  freshly-generated timestamp if omitted (not recommended - records from the same run
   *  would then get different syncRunId values depending on when each was written). */
  syncRunId?: string;
  clientConfig?: any;
};

/**
 * Parameters for configuring an End-to-End delta cycle.
 * Contains test configuration including client ID, data size, failure simulation,
 * and optional storage backend configuration.
 */
export type DeltaStrategyParams = {
  clientId: string;
  config?: DatabaseConfig | S3Config | FileConfig | DynamoDBConfig;
};

/**
 * Type guard to check if config is DatabaseConfig
 */
export const isDatabaseConfig = (config: DatabaseConfig | S3Config | FileConfig | DynamoDBConfig): config is DatabaseConfig => {
  return 'type' in config && ['sqlite', 'postgresql', 'mysql'].includes((config as DatabaseConfig).type);
};

/**
 * Type guard to check if config is S3Config
 */
export const isS3Config = (config: DatabaseConfig | S3Config | FileConfig | DynamoDBConfig): config is S3Config => {
  return 'bucketName' in config;
};

/**
 * Type guard to check if config is DynamoDBConfig
 */
export const isDynamoDBConfig = (config: DatabaseConfig | S3Config | FileConfig | DynamoDBConfig): config is DynamoDBConfig => {
  return 'personCurrentStateTableName' in config && 'personHistoryTableName' in config;
};

