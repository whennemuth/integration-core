/**
 * RunnerParams.ts - Configuration types and constants for DeltaStorageRunner
 *
 * This module defines the type system and global constants used by the test harness
 * to configure and run delta storage tests across different storage backends
 * (file system, S3, and database storage).
 */

/**
 * Global storage path constant
 * This path serves dual purpose:
 * 1) File system path for "file" delta storage
 * 2) Cache location where MockDataSource deposits mocked raw source data for ALL storage types
 */
export const storagePath = './test/test-harness/storage';

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
 * S3 configuration for cloud-based delta storage
 * Configures AWS S3 bucket settings for storing delta data in the cloud.
 */
export type S3Config = {
  bucketName: string;
  keyPrefix?: string;
  region?: string;
};

/**
 * Parameters for configuring a delta storage test run
 * Contains test configuration including client ID, data size, failure simulation,
 * and optional storage backend configuration.
 */
export type RunnerStrategyParms = {
  clientId: string;
  populationSize: number;
  simulatedPushFailureIndexes?: number[];
  config?: DatabaseConfig | S3Config;
};

/**
 * A named parameter set for test execution
 * Combines an ID, description, and test parameters into a reusable configuration.
 */
export type RunnerParameterSet = {
  id: number;
  description: string;
  parameterSet: RunnerStrategyParms;
};

/**
 * Main configuration structure for the test runner
 * Defines which parameter set is active and contains all available parameter sets.
 */
export type RunnerConfig = {
  activeParameterSetId: number;
  parameterSets: RunnerParameterSet[];
};

/**
 * Type guard to check if config is DatabaseConfig
 */
export const isDatabaseConfig = (config: DatabaseConfig | S3Config): config is DatabaseConfig => {
  return 'type' in config && ['sqlite', 'postgresql', 'mysql'].includes((config as DatabaseConfig).type);
};

/**
 * Type guard to check if config is S3Config
 */
export const isS3Config = (config: DatabaseConfig | S3Config): config is S3Config => {
  return 'bucketName' in config;
};

