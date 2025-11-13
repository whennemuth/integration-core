import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, Index } from 'typeorm';

/**
 * Entity for tracking delta computation history and audit trails.
 * Provides visibility into when deltas were computed and what changed.
 */
@Entity('delta_history')
@Index(['clientId', 'createdAt']) // Index for querying client history
export class DeltaHistoryEntity {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column('varchar', { length: 100 })
  clientId!: string;

  @Column('int', { default: 0 })
  addedCount!: number;

  @Column('int', { default: 0 })
  updatedCount!: number;

  @Column('int', { default: 0 })
  removedCount!: number;

  @Column('text', { nullable: true, transformer: { 
    to: (value: any) => value ? JSON.stringify(value) : null, 
    from: (value: string | null) => value ? JSON.parse(value) : null 
  }})
  deltaMetadata!: {
    computationTime?: number; // Time taken to compute delta in ms
    totalCurrentRecords?: number;
    totalPreviousRecords?: number;
    processingNotes?: string[];
  } | null;

  @CreateDateColumn()
  createdAt!: Date;

  /**
   * Create a DeltaHistoryEntity from delta results
   */
  static fromDeltaResult(
    clientId: string,
    deltaResult: {
      added: any[];
      updated?: any[];
      removed: any[];
    },
    metadata?: {
      computationTime?: number;
      totalCurrentRecords?: number;
      totalPreviousRecords?: number;
      processingNotes?: string[];
    }
  ): DeltaHistoryEntity {
    const entity = new DeltaHistoryEntity();
    entity.clientId = clientId;
    entity.addedCount = deltaResult.added.length;
    entity.updatedCount = deltaResult.updated?.length || 0;
    entity.removedCount = deltaResult.removed.length;
    entity.deltaMetadata = metadata || null;

    return entity;
  }
}