
export * from '../src/DataSource';
export * from '../src/DataMapper';
export * from '../src/DataTarget';
export * from '../src/DeltaTypes';
export { EndToEnd } from '../src/EndToEnd';
export * from '../src/InputParser';
export * from '../src/InputTypes';
export * from '../src/InputUtils';
export * from '../src/InputValidation';

// Delta Strategy Params exports
export * from '../src/delta-strategy/DeltaStrategyParams';
export * from '../src/delta-strategy/DeltaStrategy';

// Delta Storage exports
export { StreamProvider, NDJSONStreamProcessor } from '../src/delta-strategy/file/storage/StreamProvider';
export { FileSystemStreamProvider } from '../src/delta-strategy/file/storage/FileSystemStreamProvider';
export { S3StreamProvider } from '../src/delta-strategy/file/storage/S3StreamProvider';
export { FileSystemDeltaStorage } from '../src/delta-strategy/file/storage/FileStorage';
export { S3BucketDeltaStorage } from '../src/delta-strategy/file/storage/S3Storage';

// Database Delta Storage exports
export { PostgreSQLDeltaStorage } from '../src/delta-strategy/db/storage/PostgreSQLDeltaStorage';
export { DatabaseProvider } from '../src/delta-strategy/db/storage/DatabaseProvider';
export { FieldSetEntity } from '../src/delta-strategy/db/storage/entities/FieldSetEntity';
export { DeltaHistoryEntity } from '../src/delta-strategy/db/storage/entities/DeltaHistoryEntity';

// Delta Strategy Runner exports
export { DeltaStrategyForDatabase } from '../src/delta-strategy/db/RunnerStrategyForDatabase';
export { DeltaStrategyForFileSystem } from '../src/delta-strategy/file/RunnerStrategyForFile';
export { DeltaStrategyForS3Bucket } from '../src/delta-strategy/file/RunnerStrategyForFile';

// Re-export for backward compatibility
export { FileSystemDeltaStorage as FileStorage } from '../src/delta-strategy/file/storage/FileStorage';
export { PostgreSQLDeltaStorage as DatabaseStorage } from '../src/delta-strategy/db/storage/PostgreSQLDeltaStorage';