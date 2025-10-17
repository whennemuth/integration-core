import { DataSource, DataSourceOptions } from 'typeorm';
import { DeltaHistoryEntity } from './entities/DeltaHistoryEntity';
import { EntityFactory } from './entities/EntityFactory';

/**
 * Database connection provider that abstracts database operations.
 * Supports both PostgreSQL for production and SQLite for testing.
 * Uses dynamic entity creation for client-specific tables.
 */
export class DatabaseProvider {
  private dataSource: DataSource | null = null;

  constructor(
    private config: DatabaseConfig
  ) {}

  /**
   * Initialize the database connection
   */
  async initialize(): Promise<void> {
    if (this.dataSource?.isInitialized) {
      return;
    }

    const options: DataSourceOptions = {
      ...this.config.options,
      entities: [DeltaHistoryEntity], // Start with only static entities
      synchronize: this.config.autoSync || false, // Only for development/testing
      logging: this.config.logging || false,
    } as DataSourceOptions;

    this.dataSource = new DataSource(options);
    await this.dataSource.initialize();

    // Run migrations if specified
    if (this.config.runMigrations) {
      await this.dataSource.runMigrations();
    }
  }

  /**
   * Get the initialized DataSource
   */
  getDataSource(): DataSource {
    if (!this.dataSource?.isInitialized) {
      throw new Error('Database not initialized. Call initialize() first.');
    }
    return this.dataSource;
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    if (this.dataSource?.isInitialized) {
      await this.dataSource.destroy();
      this.dataSource = null;
    }
  }

  /**
   * Check if database is connected
   */
  isConnected(): boolean {
    return this.dataSource?.isInitialized || false;
  }

  /**
   * Ensure client-specific tables exist for both current and previous data
   */
  async ensureClientTables(clientId: string): Promise<void> {
    if (!this.dataSource?.isInitialized) {
      throw new Error('Database not initialized. Call initialize() first.');
    }

    const tableNames = EntityFactory.getClientTableNames(clientId);
    const queryRunner = this.dataSource.createQueryRunner();

    try {
      // Create current data table if it doesn't exist
      await this.createTableIfNotExists(queryRunner, tableNames.current);
      
      // Create previous data table if it doesn't exist
      await this.createTableIfNotExists(queryRunner, tableNames.previous);
    } finally {
      await queryRunner.release();
    }
  }

  /**
   * Create a client table if it doesn't exist
   */
  private async createTableIfNotExists(queryRunner: any, tableName: string): Promise<void> {
    const hasTable = await queryRunner.hasTable(tableName);
    
    if (!hasTable) {
      // Create table with the same structure as FieldSetEntity
      const sql = this.getCreateTableSQL(tableName);
      await queryRunner.query(sql);
    }
  }

  /**
   * Get the SQL for creating a client-specific field set table
   */
  private getCreateTableSQL(tableName: string): string {
    const isPostgreSQL = this.config.options.type === 'postgres';
    
    if (isPostgreSQL) {
      return `
        CREATE TABLE "${tableName}" (
          "primaryKey" VARCHAR(255) PRIMARY KEY,
          "hash" VARCHAR(64) NOT NULL,
          "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `;
    } else {
      // SQLite
      return `
        CREATE TABLE "${tableName}" (
          "primaryKey" VARCHAR(255) PRIMARY KEY,
          "hash" VARCHAR(64) NOT NULL,
          "createdAt" DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `;
    }
  }

  /**
   * Create a PostgreSQL configuration
   */
  static createPostgreSQLConfig(options: {
    host?: string;
    port?: number;
    username?: string;
    password?: string;
    database?: string;
    ssl?: boolean;
    autoSync?: boolean;
    logging?: boolean;
    runMigrations?: boolean;
  }): DatabaseConfig {
    return {
      options: {
        type: 'postgres',
        host: options.host || 'localhost',
        port: options.port || 5432,
        username: options.username || 'postgres',
        password: options.password || 'password',
        database: options.database || 'delta_storage',
        ssl: options.ssl || false,
      },
      autoSync: options.autoSync || false,
      logging: options.logging || false,
      runMigrations: options.runMigrations || false,
    };
  }

  /**
   * Create an in-memory SQLite configuration for testing
   */
  static createInMemorySQLiteConfig(): DatabaseConfig {
    return {
      options: {
        type: 'better-sqlite3',
        database: ':memory:',
      },
      autoSync: true, // Always sync schema for in-memory testing
      logging: false,
      runMigrations: false,
    };
  }

  /**
   * Create a file-based SQLite configuration
   */
  static createSQLiteConfig(filePath: string, options?: {
    autoSync?: boolean;
    logging?: boolean;
  }): DatabaseConfig {
    return {
      options: {
        type: 'better-sqlite3',
        database: filePath,
      },
      autoSync: options?.autoSync || false,
      logging: options?.logging || false,
      runMigrations: false,
    };
  }
}

export interface DatabaseConfig {
  options: Partial<DataSourceOptions>;
  autoSync?: boolean;
  logging?: boolean;
  runMigrations?: boolean;
}