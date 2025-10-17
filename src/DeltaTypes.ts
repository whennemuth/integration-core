import { FieldSet } from "./InputTypes";

/**
 * Base interface for delta storage implementations.
 * Contains common properties and operations shared by all storage types.
 */
export interface DeltaStorage {
  name: string;
  description: string;
  
  /**
   * Fetches the previous input data for delta computation or hash restoration.
   * @param params Parameters object containing clientId and optional limitTo
   * @returns Promise resolving to the previous field sets
   */
  fetchPreviousData(params: { clientId: string, limitTo?: FieldSet[] }): Promise<FieldSet[]>;
  
  /**
   * Use this to update the previous data baseline AFTER the computed delta 
   * has been pushed to the target.
   * @param params Parameters object containing clientId, newPreviousData, optional primaryKeyFields, and optional failureCount
   * @returns Promise that resolves when the update is complete
   */
  updatePreviousData(params: { 
    clientId: string, 
    newPreviousData: FieldSet[], 
    primaryKeyFields?: Set<string>, 
    failureCount?: number 
  }): Promise<any>;
}

/**
 * File-based delta storage (filesystem, S3, etc.) where current data comes from live DataSource
 * and only previous data needs to be stored for delta computation.
 * Now an alias for DeltaStorage since fetchPreviousData has been moved to the base interface.
 */
export type FileDeltaStorage = DeltaStorage;

/**
 * Database-centric delta storage where both current and previous data are stored,
 * and deltas are computed via database operations (e.g., outer joins on hash values).
 */
export interface DatabaseDeltaStorage extends DeltaStorage {
  /**
   * Stores the current input data for delta computations against prior input data.
   * @param params Parameters object containing clientId, data, and primaryKeyFields
   * @returns Promise that resolves when storage is complete
   */
  storeCurrentData(params: { clientId: string, data: FieldSet[], primaryKeyFields: Set<string> }): Promise<any>;
  
  /**
   * Computes and returns the delta using database operations.
   * Must be preceded by storeCurrentData to have both sets of data in place.
   * @param params Parameters object containing clientId and primaryKeyFields
   * @returns Promise resolving to the computed delta result
   */
  fetchDelta(params: { clientId: string, primaryKeyFields: Set<string> }): Promise<DeltaResult>;
}

export type FishingParms = { newOrUpdatedRecords: FieldSet[], removedOrUpdatedRecords: FieldSet[] };
export type DeltaParms = {
  // Clients must implement a function that "fishes" out the updated records from the addedRecords as 
  // this is where they will initially appear.
  fishOutTheUpdates: (parms: FishingParms) => DeltaResult;
  data?: { previous: FieldSet[], current: FieldSet[] };
}

/**
 * A "delta" is comprised of new records, changed records, and removed records as evidenced by where the hash
 * values of the FieldSets in each Input object either overlap or do not. Instances of this type compute the delta.
 */
export type DeltaEngine = {
  name: string;
  description: string;
  computeDelta: ( deltaParms: DeltaParms ) => Promise<DeltaResult>;
}

export type DeltaResult = {
  added: FieldSet[];
  updated?: FieldSet[];
  removed: FieldSet[];
}