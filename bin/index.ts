
export * from '../src/DataSource';
export * from '../src/DataTarget';
export * from '../src/DeltaTypes';
export * from '../src/InputParser';
export * from '../src/InputTypes';
export * from '../src/InputUtils';
export * from '../src/InputValidation';

// Delta Storage exports
export { StreamProvider, NDJSONStreamProcessor } from '../src/delta-storage/file/StreamProvider';
export { FileSystemStreamProvider } from '../src/delta-storage/file/FileSystemStreamProvider';
export { S3StreamProvider } from '../src/delta-storage/file/S3StreamProvider';
export { FileSystemDeltaStorage } from '../src/delta-storage/file/FileStorage';
export { S3BucketDeltaStorage } from '../src/delta-storage/file/S3Storage';

// Database Delta Storage exports
export { PostgreSQLDeltaStorage } from '../src/delta-storage/db/PostgreSQLDeltaStorage';
export { DatabaseProvider } from '../src/delta-storage/db/DatabaseProvider';
export { FieldSetEntity } from '../src/delta-storage/db/entities/FieldSetEntity';
export { DeltaHistoryEntity } from '../src/delta-storage/db/entities/DeltaHistoryEntity';

// Re-export for backward compatibility
export { FileSystemDeltaStorage as FileStorage } from '../src/delta-storage/file/FileStorage';
export { PostgreSQLDeltaStorage as DatabaseStorage } from '../src/delta-storage/db/PostgreSQLDeltaStorage';