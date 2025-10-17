import { Repository, EntityManager } from 'typeorm';
import { DatabaseDeltaStorage, DeltaResult } from '../../../DeltaTypes';
import { FieldSet, Field } from '../../../InputTypes';
import { DatabaseProvider, DatabaseConfig } from './DatabaseProvider';
import { FieldSetEntity } from './entities/FieldSetEntity';
import { EntityFactory } from './entities/EntityFactory';
import { DeltaHistoryEntity } from './entities/DeltaHistoryEntity';

/**
 * Database-centric implementation of DeltaStorage that stores both current and previous data
 * and computes deltas using SQL operations for high performance and scalability.
 * 
 * Features:
 * - Stores data in normalized database tables with proper indexing
 * - Uses SQL outer joins for efficient delta computation
 * - Maintains audit trail of delta operations
 * - Supports both PostgreSQL (production) and SQLite (testing)
 * - Handles concurrent operations safely with database transactions
 */
export class PostgreSQLDeltaStorage implements DatabaseDeltaStorage {
  public readonly name = 'PostgreSQL Delta Storage';
  public readonly description = 'Database-centric delta storage using PostgreSQL with client-specific tables and SQL-based delta computation';

  private databaseProvider: DatabaseProvider;
  private deltaHistoryRepository!: Repository<DeltaHistoryEntity>;
  private initialized = false;

  /**
   * Creates a new PostgreSQLDeltaStorage instance
   * @param config Database configuration
   */
  constructor(config: DatabaseConfig) {
    this.databaseProvider = new DatabaseProvider(config);
  }

  /**
   * Initialize the database connection and repositories
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    await this.databaseProvider.initialize();
    const dataSource = this.databaseProvider.getDataSource();
    
    this.deltaHistoryRepository = dataSource.getRepository(DeltaHistoryEntity);
    
    this.initialized = true;
  }

  /**
   * Ensure the storage is initialized
   */
  private ensureInitialized(): void {
    if (!this.initialized) {
      throw new Error('PostgreSQLDeltaStorage not initialized. Call initialize() first.');
    }
  }

  /**
   * Stores the current input data, automatically promoting any existing current data to previous.
   * This operation is transactional to ensure data consistency.
   */
  async storeCurrentData(params: { clientId: string, data: FieldSet[], primaryKeyFields: Set<string> }): Promise<any> {
    const { clientId, data, primaryKeyFields } = params;
    if (!clientId) {
      throw new Error('clientId is required for storeCurrentData');
    }

    this.ensureInitialized();
    const startTime = Date.now();

    // Ensure client tables exist
    await this.databaseProvider.ensureClientTables(clientId);

    return await this.databaseProvider.getDataSource().transaction(async (manager: EntityManager) => {
      const tableNames = EntityFactory.getClientTableNames(clientId);
      
      // Step 1: Move existing current data to previous (clear previous first)
      await manager.query(`DELETE FROM "${tableNames.previous}"`);
      
      // Move current data to previous
      await manager.query(`
        INSERT INTO "${tableNames.previous}" (primaryKey, hash, createdAt)
        SELECT primaryKey, hash, createdAt FROM "${tableNames.current}"
      `);

      // Step 2: Replace current data with new data
      await manager.query(`DELETE FROM "${tableNames.current}"`);
      
      // Convert FieldSets to entities
      const entities = data
        .filter(fs => fs.hash) // Only store records with hashes
        .map(fs => FieldSetEntity.fromKeyAndHashFieldSet(fs, primaryKeyFields));

      if (entities.length > 0) {
        // Batch insert new current data
        for (const entity of entities) {
          await manager.query(`
            INSERT INTO "${tableNames.current}" (primaryKey, hash, createdAt)
            VALUES (?, ?, ?)
          `, [entity.primaryKey, entity.hash, new Date().toISOString()]);
        }
      }

      const processingTime = Date.now() - startTime;

      return {
        status: 'success',
        message: `Stored ${entities.length} field sets for client ${clientId}`,
        clientId,
        recordCount: entities.length,
        processingTime,
        storage: 'postgresql',
        timestamp: new Date().toISOString()
      };
    });
  }

  /**
   * Fetches previous input data, optionally limited to specific records for performance.
   * For database storage, this returns data from the previous table, with optional filtering
   * based on primary keys when limitTo is provided (useful for hash restoration operations).
   * 
   * Note: This method requires primary key fields to reconstruct FieldSets properly.
   * Since we can't determine primary key fields from stored data alone, this method
   * has limitations when limitTo is not provided or is empty.
   */
  async fetchPreviousData(params: { clientId: string, limitTo?: FieldSet[] }): Promise<FieldSet[]> {
    const { clientId, limitTo } = params;
    if (!clientId) {
      throw new Error('clientId is required for fetchPreviousData');
    }

    this.ensureInitialized();

    // Ensure client tables exist
    await this.databaseProvider.ensureClientTables(clientId);

    const dataSource = this.databaseProvider.getDataSource();
    const tableNames = EntityFactory.getClientTableNames(clientId);
    
    let query = `SELECT primaryKey, hash FROM "${tableNames.previous}"`;
    let queryParams: any[] = [];

    // If limitTo is provided, filter by primary keys for performance
    if (limitTo && limitTo.length > 0) {
      // Extract primary key values from limitTo field sets and build IN clause
      const primaryKeyValues = limitTo.map(fs => {
        // Generate primary key string from field values (same logic as FieldSetEntity)
        const primaryKeyArray = Array.from(fs.fieldValues.map((fv: Field) => Object.values(fv)[0]));
        return primaryKeyArray.join('|');
      });

      if (primaryKeyValues.length > 0) {
        const placeholders = primaryKeyValues.map(() => '?').join(', ');
        query += ` WHERE primaryKey IN (${placeholders})`;
        queryParams = primaryKeyValues;
      }
    }

    const results = await dataSource.query(query, queryParams);
    
    if (!results || results.length === 0) {
      return [];
    }

    // For database storage, we need to infer primary key field names from limitTo parameter
    // If limitTo is not provided, we'll return a minimal structure that works for hash restoration
    let primaryKeyFields: Set<string>;
    
    if (limitTo && limitTo.length > 0) {
      // Extract primary key field names from the first limitTo record
      const firstRecord = limitTo[0];
      const fieldNames = firstRecord.fieldValues.map((fv: Field) => Object.keys(fv)[0]);
      primaryKeyFields = new Set(fieldNames);
    } else {
      // If no limitTo provided, we can't reconstruct field names, so return empty field sets
      // This is acceptable since database storage typically uses limitTo for performance
      return results.map((record: any) => ({
        fieldValues: [], // Empty field values since we can't determine field names
        hash: record.hash
      }));
    }

    // Convert database results back to FieldSet format
    return results.map((record: any) => {
      const entity = new FieldSetEntity();
      entity.primaryKey = record.primaryKey;
      entity.hash = record.hash;
      
      // Use existing conversion method to get proper FieldSet format
      return entity.toKeyAndHashFieldSet(primaryKeyFields);
    });
  }

  /**
   * Computes and returns the delta using SQL operations.
   * Uses efficient outer joins to identify added, updated, and removed records.
   */
  async fetchDelta(params: { clientId: string, primaryKeyFields: Set<string> }): Promise<DeltaResult> {
    const { clientId, primaryKeyFields } = params;
    if (!clientId) {
      throw new Error('clientId is required for fetchDelta');
    }

    this.ensureInitialized();
    const startTime = Date.now();

    // Ensure client tables exist
    await this.databaseProvider.ensureClientTables(clientId);

    // Use raw SQL for optimal performance with three-way delta computation using primary key joins
    const dataSource = this.databaseProvider.getDataSource();
    const tableNames = EntityFactory.getClientTableNames(clientId);
    
    // Query for added records (primary key exists in current but not in previous)
    const addedEntities = await dataSource.query(`
      SELECT c.primaryKey, c.hash
      FROM "${tableNames.current}" c
      LEFT JOIN "${tableNames.previous}" p ON c.primaryKey = p.primaryKey
      WHERE p.primaryKey IS NULL
    `);

    // Query for updated records (primary key exists in both but with different hashes)
    const updatedEntities = await dataSource.query(`
      SELECT c.primaryKey, c.hash
      FROM "${tableNames.current}" c
      INNER JOIN "${tableNames.previous}" p ON c.primaryKey = p.primaryKey
      WHERE c.hash != p.hash
    `);

    // Query for removed records (primary key exists in previous but not in current)
    const removedEntities = await dataSource.query(`
      SELECT p.primaryKey, p.hash
      FROM "${tableNames.previous}" p
      LEFT JOIN "${tableNames.current}" c ON p.primaryKey = c.primaryKey
      WHERE c.primaryKey IS NULL
    `);

    // Convert query results back to FieldSet format using entity helper
    const added: FieldSet[] = addedEntities.map((record: any) => {
      const entity = new FieldSetEntity();
      entity.primaryKey = record.primaryKey;
      entity.hash = record.hash;
      return entity.toKeyAndHashFieldSet(primaryKeyFields);
    });

    const updated: FieldSet[] = updatedEntities.map((record: any) => {
      const entity = new FieldSetEntity();
      entity.primaryKey = record.primaryKey;
      entity.hash = record.hash;
      return entity.toKeyAndHashFieldSet(primaryKeyFields);
    });

    const removed: FieldSet[] = removedEntities.map((record: any) => {
      const entity = new FieldSetEntity();
      entity.primaryKey = record.primaryKey;
      entity.hash = record.hash;
      return entity.toKeyAndHashFieldSet(primaryKeyFields);
    });

    const deltaResult: DeltaResult = {
      added,
      updated,
      removed
    };

    // Record delta computation in history
    const processingTime = Date.now() - startTime;
    const historyEntry = DeltaHistoryEntity.fromDeltaResult(clientId, deltaResult, {
      computationTime: processingTime,
      totalCurrentRecords: await this.getRecordCount(clientId, 'current'),
      totalPreviousRecords: await this.getRecordCount(clientId, 'previous')
    });

    await this.deltaHistoryRepository.save(historyEntry);

    return deltaResult;
  }

  /**
   * Updates the previous data baseline after successful delta processing.
   * For database storage, this replaces both previous and current tables with the provided data
   * to ensure they remain in sync and prevent failed records from persisting.
   */
  async updatePreviousData(params: { clientId: string, newPreviousData: FieldSet[], primaryKeyFields?: Set<string>, failureCount?: number }): Promise<any> {
    const { clientId, newPreviousData, primaryKeyFields, failureCount = 0 } = params;
    if (!clientId) {
      throw new Error('clientId is required for updatePreviousData');
    }

    this.ensureInitialized();

    // Ensure client tables exist
    await this.databaseProvider.ensureClientTables(clientId);

    if (newPreviousData && newPreviousData.length > 0 && failureCount > 0) {
      // Replace previous data with the provided data
      return await this.databaseProvider.getDataSource().transaction(async (manager: EntityManager) => {
        const tableNames = EntityFactory.getClientTableNames(clientId);
        
        // Clear existing previous data
        await manager.query(`DELETE FROM "${tableNames.previous}"`);
        
        // Clear existing current data to keep tables in sync
        await manager.query(`DELETE FROM "${tableNames.current}"`);
        
        // Ensure primaryKeyFields is provided
        if (!primaryKeyFields) {
          throw new Error('primaryKeyFields is required when providing newPreviousData');
        }

        // Convert FieldSets to entities
        const entities = newPreviousData
          .filter(fs => fs.hash)
          .map(fs => FieldSetEntity.fromKeyAndHashFieldSet(fs, primaryKeyFields));

        // Store new previous data
        if (entities.length > 0) {
          // Batch insert new previous and current data
          for (const entity of entities) {
            await manager.query(`
              INSERT INTO "${tableNames.previous}" (primaryKey, hash, createdAt)
              VALUES (?, ?, ?)
            `, [entity.primaryKey, entity.hash, new Date().toISOString()]);
            await manager.query(`
              INSERT INTO "${tableNames.current}" (primaryKey, hash, createdAt)
              VALUES (?, ?, ?)
            `, [entity.primaryKey, entity.hash, new Date().toISOString()]);
          }
        }

        return {
          status: 'success',
          message: `Updated previous and current data for client ${clientId}`,
          action: 'replaced previous and current data with provided data',
          recordCount: entities.length,
          storage: 'postgresql',
          timestamp: new Date().toISOString()
        };
      });
    } else {
      // Promote current data to previous (standard delta cycle completion)
      return await this.databaseProvider.getDataSource().transaction(async (manager: EntityManager) => {
        const tableNames = EntityFactory.getClientTableNames(clientId);
        
        // Clear old previous data
        await manager.query(`DELETE FROM "${tableNames.previous}"`);
        
        // Promote current to previous
        const result = await manager.query(`
          INSERT INTO "${tableNames.previous}" (primaryKey, hash, createdAt)
          SELECT primaryKey, hash, createdAt FROM "${tableNames.current}"
        `);
        
        const recordCount = Array.isArray(result) ? result.length : (result.affectedRows || result.changes || 0);

        return {
          status: 'success',
          message: `Promoted current data to previous for client ${clientId}`,
          action: 'promoted current to previous baseline',
          recordCount: recordCount,
          storage: 'postgresql',
          timestamp: new Date().toISOString()
        };
      });
    }
  }

  /**
   * Get count of records for a client and data type
   */
  private async getRecordCount(clientId: string, dataType: 'current' | 'previous'): Promise<number> {
    const dataSource = this.databaseProvider.getDataSource();
    const tableNames = EntityFactory.getClientTableNames(clientId);
    const tableName = dataType === 'current' ? tableNames.current : tableNames.previous;
    
    const result = await dataSource.query(`SELECT COUNT(*) as count FROM "${tableName}"`);
    return parseInt(result[0]?.count || '0', 10);
  }

  /**
   * Clean up old data for a client
   */
  async cleanup(clientId: string): Promise<void> {
    this.ensureInitialized();
    
    await this.databaseProvider.getDataSource().transaction(async (manager: EntityManager) => {
      const tableNames = EntityFactory.getClientTableNames(clientId);
      
      // Clean up client-specific tables
      await manager.query(`DELETE FROM "${tableNames.current}"`);
      await manager.query(`DELETE FROM "${tableNames.previous}"`);
      
      // Clean up delta history
      const historyRepo = manager.getRepository(DeltaHistoryEntity);
      await historyRepo.delete({ clientId });
    });
  }

  /**
   * Get delta computation history for a client
   */
  async getDeltaHistory(clientId: string, limit: number = 10): Promise<DeltaHistoryEntity[]> {
    this.ensureInitialized();
    
    return await this.deltaHistoryRepository.find({
      where: { clientId },
      order: { createdAt: 'DESC' },
      take: limit
    });
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    await this.databaseProvider.close();
    this.initialized = false;
  }
}