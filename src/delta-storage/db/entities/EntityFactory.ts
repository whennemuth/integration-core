import { Entity, getMetadataArgsStorage } from 'typeorm';
import { FieldSetEntity } from './FieldSetEntity';

/**
 * Factory for creating client-specific entity classes with dynamic table names.
 * This allows us to have separate tables per client and data type without code duplication.
 */
export class EntityFactory {
  private static createdEntities = new Map<string, typeof FieldSetEntity>();

  /**
   * Creates or retrieves a FieldSetEntity class for a specific client and data type.
   * The class will be configured to use a table name like "client_{clientId}_{dataType}".
   */
  static getFieldSetEntity(clientId: string, dataType: 'current' | 'previous'): typeof FieldSetEntity {
    const tableName = FieldSetEntity.generateTableName(clientId, dataType);
    
    // Return cached entity class if already created
    if (this.createdEntities.has(tableName)) {
      return this.createdEntities.get(tableName)!;
    }

    // Create new entity class with dynamic table name
    @Entity(tableName)
    class DynamicFieldSetEntity extends FieldSetEntity {}

    // Cache the entity class
    this.createdEntities.set(tableName, DynamicFieldSetEntity);
    
    return DynamicFieldSetEntity;
  }

  /**
   * Gets all table names for a specific client (both current and previous).
   */
  static getClientTableNames(clientId: string): { current: string; previous: string } {
    return {
      current: FieldSetEntity.generateTableName(clientId, 'current'),
      previous: FieldSetEntity.generateTableName(clientId, 'previous')
    };
  }

  /**
   * Clears the entity cache (mainly for testing purposes).
   * Note: This doesn't clean up TypeORM metadata as it's readonly.
   * In production, entity classes should be created once and reused.
   */
  static clearCache(): void {
    this.createdEntities.clear();
  }

  /**
   * Gets all created table names (for cleanup and management purposes).
   */
  static getAllTableNames(): string[] {
    return Array.from(this.createdEntities.keys());
  }
}