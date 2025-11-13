import { Entity, PrimaryColumn, Column, Index, CreateDateColumn } from 'typeorm';

/**
 * Entity representing a FieldSet stored in the database for delta computation.
 * Stores both the hash and the actual field data for efficient delta operations.
 */
@Entity('field_sets')
@Index(['clientId', 'hash']) // Composite index for efficient delta queries
@Index(['clientId', 'dataType', 'createdAt']) // Index for cleanup operations
export class FieldSetEntity {
  @PrimaryColumn('varchar', { length: 255 })
  id!: string; // Composite key: clientId:hash:dataType

  @Column('varchar', { length: 100 })
  clientId!: string;

  @Column('varchar', { length: 64 })
  hash!: string;

  @Column('varchar', { length: 20, default: 'current' })
  dataType!: 'current' | 'previous'; // Tracks whether this is current or previous data

  @Column('text', { transformer: { to: JSON.stringify, from: JSON.parse } })
  fieldValues!: Record<string, any>[]; // Store the actual field values as JSON text

  @Column('text', { nullable: true, transformer: { 
    to: (value: Record<string, string> | null) => value ? JSON.stringify(value) : null, 
    from: (value: string | null) => value ? JSON.parse(value) : null 
  }})
  validationMessages!: Record<string, string> | null; // Store validation messages if any

  @CreateDateColumn()
  createdAt!: Date;

  /**
   * Generate a composite ID for the entity
   */
  static generateId(clientId: string, hash: string, dataType: 'current' | 'previous'): string {
    return `${clientId}:${hash}:${dataType}`;
  }

  /**
   * Create a FieldSetEntity from a FieldSet
   */
  static fromFieldSet(
    clientId: string, 
    fieldSet: { fieldValues: Record<string, any>[]; hash?: string; validationMessages?: Map<string, string> }, 
    dataType: 'current' | 'previous' = 'current'
  ): FieldSetEntity {
    if (!fieldSet.hash) {
      throw new Error('FieldSet must have a hash to be stored in database');
    }

    const entity = new FieldSetEntity();
    entity.id = FieldSetEntity.generateId(clientId, fieldSet.hash, dataType);
    entity.clientId = clientId;
    entity.hash = fieldSet.hash;
    entity.dataType = dataType;
    entity.fieldValues = fieldSet.fieldValues;
    entity.validationMessages = fieldSet.validationMessages 
      ? Object.fromEntries(fieldSet.validationMessages) 
      : null;

    return entity;
  }

  /**
   * Convert back to FieldSet format
   */
  toFieldSet(): { fieldValues: Record<string, any>[]; hash: string; validationMessages?: Map<string, string> } {
    return {
      fieldValues: this.fieldValues,
      hash: this.hash,
      validationMessages: this.validationMessages 
        ? new Map(Object.entries(this.validationMessages))
        : undefined
    };
  }
}