import { FieldSet, Input } from "./InputTypes";

/**
 * Represents storage, like a database, file system or bucket storage, dynamodb, etc. where two sets of 
 * Input data objects can be stored for comparison with each other for the purposes of finding their "delta".
 */
export type DeltaStorage = {
  name: string;
  description: string;

  /**
   * Use this if the delta storage unit itself can produce the "delta", ie: a relational database where an 
   * outer join on the hash values is used. Must be preceded by storeCurrentInput to have both sets of data in place.
   * @returns 
   */
  fetchDeltaInput?: () => Promise<DeltaResult>;

  /**
   * Use this if the entire prior fetched input data is needed to compute the delta.
   * @returns 
   */
  fetchPreviousInput?: () => Promise<FieldSet[]>;

  /**
   *  Use this to store the current input data for delta computations against prior input data.
   * @param key 
   * @returns 
   */
  storeCurrentInput: (data: FieldSet[]) => Promise<void>;

  /**
   * Use this to swap or replace the prior input with the current input AFTER the computed delta 
   * has been pushed to the target.
   * @param task Indicates if table/file for current data at the delta storage location should be 
   * replaced or swap places with the table/file for prior data.
   * @returns 
   */
  rotate: (task: 'swap' | 'replace') => Promise<void>;
}
 
export type DeltaParms = {
  // Clients must implement a function that "fishes" out the updated records from the addedRecords as 
  // this is where they will initially appear.
  fishOutTheUpdates: (addedRecords: FieldSet[], removedRecords: FieldSet[]) => DeltaResult;
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