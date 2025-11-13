import { Repository } from 'typeorm';
import { DatabaseDeltaStorage, DeltaResult } from '../../DeltaTypes';
import { FieldSet } from '../../InputTypes';
import { DatabaseProvider, DatabaseConfig } from './DatabaseProvider';
import { FieldSetEntity } from './entities/FieldSetEntity';
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
  public readonly description = 'Database-centric delta storage using PostgreSQL with SQL-based delta computation';

  private databaseProvider: DatabaseProvider;
  private fieldSetRepository!: Repository<FieldSetEntity>;
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
    
    this.fieldSetRepository = dataSource.getRepository(FieldSetEntity);
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
  async storeCurrentData(clientId: string, data: FieldSet[]): Promise<any> {
    if (!clientId) {
      throw new Error('clientId is required for storeCurrentData');
    }

    this.ensureInitialized();
    const startTime = Date.now();

    return await this.databaseProvider.getDataSource().transaction(async manager => {
      const fieldSetRepo = manager.getRepository(FieldSetEntity);

      // Step 1: Move existing current data to previous (delete old previous first)
      await fieldSetRepo.delete({ clientId, dataType: 'previous' });
      
      // Update current records to previous
      await fieldSetRepo.update(
        { clientId, dataType: 'current' },
        { dataType: 'previous' }
      );

      // Step 2: Store new current data
      const entities = data
        .filter(fs => fs.hash) // Only store records with hashes
        .map(fs => FieldSetEntity.fromFieldSet(clientId, fs, 'current'));

      if (entities.length > 0) {
        await fieldSetRepo.save(entities);
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
   * Computes and returns the delta using SQL operations.
   * Uses efficient outer joins to identify added, updated, and removed records.
   */
  async fetchDelta(clientId: string): Promise<DeltaResult> {
    if (!clientId) {
      throw new Error('clientId is required for fetchDelta');
    }

    this.ensureInitialized();
    const startTime = Date.now();

    // Use raw SQL for optimal performance with complex joins
    const dataSource = this.databaseProvider.getDataSource();
    
    // Query for added records (in current but not in previous)
    const addedRecords = await dataSource.query(`
      SELECT c.fieldValues, c.hash, c.validationMessages
      FROM field_sets c
      LEFT JOIN field_sets p ON c.hash = p.hash AND p.clientId = ? AND p.dataType = 'previous'
      WHERE c.clientId = ? AND c.dataType = 'current' AND p.hash IS NULL
    `, [clientId, clientId]);

    // Query for removed records (in previous but not in current)
    const removedRecords = await dataSource.query(`
      SELECT p.fieldValues, p.hash, p.validationMessages
      FROM field_sets p
      LEFT JOIN field_sets c ON p.hash = c.hash AND c.clientId = ? AND c.dataType = 'current'
      WHERE p.clientId = ? AND p.dataType = 'previous' AND c.hash IS NULL
    `, [clientId, clientId]);

    // For this implementation, we'll treat all hash changes as add/remove pairs
    // More sophisticated implementations could detect field-level updates
    const added: FieldSet[] = addedRecords.map((record: any) => ({
      fieldValues: record.fieldValues,
      hash: record.hash,
      validationMessages: record.validationMessages 
        ? new Map(Object.entries(record.validationMessages))
        : undefined
    }));

    const removed: FieldSet[] = removedRecords.map((record: any) => ({
      fieldValues: record.fieldValues,
      hash: record.hash,
      validationMessages: record.validationMessages 
        ? new Map(Object.entries(record.validationMessages))
        : undefined
    }));

    const deltaResult: DeltaResult = {
      added,
      removed
    };

    // Record delta computation in history
    const processingTime = Date.now() - startTime;
    const historyEntry = DeltaHistoryEntity.fromDeltaResult(clientId, deltaResult, {
      computationTime: processingTime,
      totalCurrentRecords: added.length + (await this.getRecordCount(clientId, 'current')) - added.length,
      totalPreviousRecords: removed.length + (await this.getRecordCount(clientId, 'previous')) - removed.length
    });

    await this.deltaHistoryRepository.save(historyEntry);

    return deltaResult;
  }

  /**
   * Updates the previous data baseline after successful delta processing.
   * In database storage, this typically means promoting current data to previous.
   */
  async updatePreviousData(clientId: string, newPreviousData?: FieldSet[]): Promise<any> {
    if (!clientId) {
      throw new Error('clientId is required for updatePreviousData');
    }

    this.ensureInitialized();

    if (newPreviousData && newPreviousData.length > 0) {
      // Replace previous data with the provided data
      return await this.databaseProvider.getDataSource().transaction(async manager => {
        const fieldSetRepo = manager.getRepository(FieldSetEntity);
        
        // Delete existing previous data
        await fieldSetRepo.delete({ clientId, dataType: 'previous' });
        
        // Store new previous data
        const entities = newPreviousData
          .filter(fs => fs.hash)
          .map(fs => FieldSetEntity.fromFieldSet(clientId, fs, 'previous'));

        if (entities.length > 0) {
          await fieldSetRepo.save(entities);
        }

        return {
          status: 'success',
          message: `Updated previous data for client ${clientId}`,
          action: 'replaced previous data with provided data',
          recordCount: entities.length,
          storage: 'postgresql',
          timestamp: new Date().toISOString()
        };
      });
    } else {
      // Promote current data to previous (standard delta cycle completion)
      return await this.databaseProvider.getDataSource().transaction(async manager => {
        const fieldSetRepo = manager.getRepository(FieldSetEntity);
        
        // Delete old previous data
        await fieldSetRepo.delete({ clientId, dataType: 'previous' });
        
        // Promote current to previous
        const result = await fieldSetRepo.update(
          { clientId, dataType: 'current' },
          { dataType: 'previous' }
        );

        return {
          status: 'success',
          message: `Promoted current data to previous for client ${clientId}`,
          action: 'promoted current to previous baseline',
          recordCount: result.affected || 0,
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
    return await this.fieldSetRepository.count({ where: { clientId, dataType } });
  }

  /**
   * Clean up old data for a client
   */
  async cleanup(clientId: string): Promise<void> {
    this.ensureInitialized();
    
    await this.databaseProvider.getDataSource().transaction(async manager => {
      const fieldSetRepo = manager.getRepository(FieldSetEntity);
      const historyRepo = manager.getRepository(DeltaHistoryEntity);
      
      await fieldSetRepo.delete({ clientId });
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