import { Entity, PrimaryColumn, Column, CreateDateColumn } from 'typeorm';

/**
 * Entity representing a reduced FieldSet stored in the database for delta computation.
 * Stores primary key values (concatenated) and hash only - no full field data.
 * Corresponds to the output of InputUtilsDecorator.getKeyAndHashFieldSets().
 * 
 * This entity is designed to be reused with dynamic table names per client and data type:
 * - client_{clientId}_current (e.g., "client_123_current")
 * - client_{clientId}_previous (e.g., "client_123_previous")
 */
@Entity() // Table name will be set dynamically
export class FieldSetEntity {
  @PrimaryColumn('varchar', { length: 255 })
  primaryKey!: string; // Concatenated primary key values (e.g., "123|john@example.com")

  @Column('varchar', { length: 64 })
  hash!: string;

  @CreateDateColumn()
  createdAt!: Date;

  /**
   * Generate concatenated primary key value from field values
   */
  static generatePrimaryKeyValue(fieldValues: Record<string, any>[], primaryKeyFields: Set<string>): string {
    const primaryKeyValues = Array.from(primaryKeyFields).map((pkField: string) => {
      const field = fieldValues.find(fv => Object.keys(fv)[0] === pkField);
      return field ? Object.values(field)[0] : '';
    });
    return primaryKeyValues.join('|');
  }

  /**
   * Generate table name for client and data type
   * Sanitizes clientId to be SQL-safe by replacing special characters with underscores
   */
  static generateTableName(clientId: string, dataType: 'current' | 'previous'): string {
    // Replace any non-alphanumeric characters with underscores for SQL safety
    const sanitizedClientId = clientId.replace(/[^a-zA-Z0-9]/g, '_');
    return `client_${sanitizedClientId}_${dataType}`;
  }

  /**
   * Create a FieldSetEntity from a key and hash FieldSet
   */
  static fromKeyAndHashFieldSet(
    fieldSet: { fieldValues: Record<string, any>[]; hash?: string }, 
    primaryKeyFields: Set<string>
  ): FieldSetEntity {
    if (!fieldSet.hash) {
      throw new Error('FieldSet must have a hash to be stored in database');
    }

    const primaryKeyValue = FieldSetEntity.generatePrimaryKeyValue(fieldSet.fieldValues, primaryKeyFields);
    
    const entity = new FieldSetEntity();
    entity.primaryKey = primaryKeyValue;
    entity.hash = fieldSet.hash;

    return entity;
  }

  /**
   * Convert back to key and hash FieldSet format with reconstructed primary key fields
   */
  toKeyAndHashFieldSet(primaryKeyFields: Set<string>): { fieldValues: Record<string, any>[]; hash: string } {
    const primaryKeyValues = this.primaryKey.split('|');
    const primaryKeyFieldsArray = Array.from(primaryKeyFields);
    
    const fieldValues = primaryKeyFieldsArray.map((fieldName, index) => ({
      [fieldName]: primaryKeyValues[index] || ''
    }));

    return {
      fieldValues,
      hash: this.hash
    };
  }
}