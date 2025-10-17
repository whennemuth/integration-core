import { DatabaseProvider } from "./storage/DatabaseProvider";
import { PostgreSQLDeltaStorage } from "./storage/PostgreSQLDeltaStorage";
import { DatabaseDeltaStorage, DeltaStorage } from "../../DeltaTypes";
import { InputUtilsDecorator } from "../../InputUtils";
import { DeltaStrategy } from '../DeltaStrategy';
import { DatabaseConfig, DeltaStrategyParams } from "../DeltaStrategyParams";

/**
 * Database storage strategy implementation
 */
export class DeltaStrategyForDatabase extends DeltaStrategy {
  private dbConfig: any;

  constructor(parms: DeltaStrategyParams) {
    super(parms);
    this.dbConfig = this.createDatabaseConfig(this.parms.config as DatabaseConfig);
  }
  
  public async computeDelta(computeParms: {
    storage: DeltaStorage, 
    currentFieldSets: any[], 
    inputUtils: InputUtilsDecorator, 
    clientId: string
  }): Promise<any> {
    const { storage, currentFieldSets, inputUtils, clientId } = computeParms;
    // Database storage: use built-in SQL-based delta computation
    const dbStorage = storage as DatabaseDeltaStorage;
    
    // Initialize database storage
    await (dbStorage as PostgreSQLDeltaStorage).initialize();
    
    // Store current data (automatically promotes previous data)
    const primaryKeyFields = inputUtils.getPrimaryKey();
    await dbStorage.storeCurrentData({ clientId, data: currentFieldSets, primaryKeyFields });
    
    // Fetch computed delta
    return await dbStorage.fetchDelta({ clientId, primaryKeyFields });
  }

  public get storage(): DeltaStorage {
    return new PostgreSQLDeltaStorage(this.dbConfig);
  }

  /**
   * Create a database configuration using DatabaseProvider static methods
   */
  private createDatabaseConfig(config: DatabaseConfig) {
    switch (config.type) {
      case 'sqlite':
        if (config.filename) {
          return DatabaseProvider.createSQLiteConfig(config.filename, {
            autoSync: config.synchronize,
            logging: config.logging
          });
        } else {
          return DatabaseProvider.createInMemorySQLiteConfig();
        }
      case 'postgresql':
        return DatabaseProvider.createPostgreSQLConfig({
          host: config.host,
          port: config.port,
          username: config.username,
          password: config.password,
          database: config.database,
          ssl: config.ssl,
          autoSync: config.synchronize,
          logging: config.logging,
          runMigrations: false
        });
      case 'mysql':
        throw new Error('MySQL support not yet implemented');
      default:
        throw new Error(`Unsupported database type: ${config.type}`);
    }
  }
}