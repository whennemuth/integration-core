import { DeltaEngine, DeltaParms, DeltaResult, FishingParms } from "../DeltaTypes";
import { FieldSet } from "../InputTypes";

/**
 * A brute-force implementation of the DeltaEngine that computes the delta between two datasets by 
 * comparing values from the hashed result of each of their combined field sets (records).
 * Overlapping hashes indicate unchanged records, while non-overlapping hashes indicate new, changed, 
 * or removed records. This implementation is straightforward but reaches a performance considerations
 * threshold at around 200,000 records.
 */
export class BruteForceDeltaEngine implements DeltaEngine {
  name = "Brute Force Delta Engine";
  description = "A delta engine finds overlap between 2 datasets on the basis of HashSet methods.";

  public async computeDelta(deltaParms: DeltaParms): Promise<DeltaResult> {
    const { data, fishOutTheUpdates } = deltaParms;
    if ( ! data) {
      throw new Error("Previous and current input data must be provided.");
    }

    const { previous, current } = data;

    /**
     * Derive a set from the hashes of the records in each input for more performant lookups.
     * --------------------------------------------------------------------------------------
     *   Worst:  nested loops (O(n^2))
     *   Better: maps (O(n log n))
     *   Best:   sets (O(n))
     * -------------------------------------------------------------------------------------
     */
    const previousHashes = previous.map(record => record.hash).filter((hash): hash is string => hash !== undefined);
    const currentHashes = current.map(record => record.hash).filter((hash): hash is string => hash !== undefined);
    const previousHashSet = new Set(previousHashes);
    const currentHashSet = new Set(currentHashes);

    // Find new/missing efficiently
    const newHashes = currentHashes.filter(hash => !previousHashSet.has(hash));
    const missingHashes = previousHashes.filter(hash => !currentHashSet.has(hash));

    // Get a subset of the original records based on the added/removed hashes (should be small, so performance is ok)
    const newOrUpdatedRecords = current.filter(record => record.hash && newHashes.includes(record.hash));
    const removedOrUpdatedRecords = previous.filter(record => record.hash && missingHashes.includes(record.hash));

    return fishOutTheUpdates({ newOrUpdatedRecords, removedOrUpdatedRecords });
  }
}

/**
 * Use a comparison based on the primary key set (set of field names) which form the basis of finding
 * the "same" record between the following 2 types of provided record sets:
 *   1) Incoming records with hashes not found in the previous set of records.
 *      These comprise potentially new or merely updated records.
 *   2) Previous records with hashes not found in the incoming records. 
 *      These comprise potentially removed or merely updated records.
 * The comparison performed here determines which records are shared by both sets by primary key, and 
 * "fishes" those out of both sets into their own single set, and returns all 3 sets. 
 */
export const fishOutUpdatedRecordsByPK = (fishingParms:FishingParms, primaryKeySet: Set<string>): DeltaResult => {
  const { newOrUpdatedRecords, removedOrUpdatedRecords } = fishingParms;
  if (primaryKeySet.size === 0) {
    // If the primary key set is not provided, we cannot determine changed records, so we skip this step.
    return { added: newOrUpdatedRecords, updated: [], removed: removedOrUpdatedRecords };
  }
  const updatedRecords: FieldSet[] = [];
  const addedRecords: FieldSet[] = [];

  for (const newOrUpdatedRecord of newOrUpdatedRecords) {
    const matchingRemovedIndex = removedOrUpdatedRecords.findIndex(removedOrUpdatedRecord => {
      return Array.from(primaryKeySet.values()).every(keyname => {
        // Get the primary key field value from newOrUpdatedRecord
        const newRecordKeyField = newOrUpdatedRecord.fieldValues.find(fv => keyname in fv);
        const newRecordKeyValue = newRecordKeyField?.[keyname];
        
        // Get the primary key field value from removedOrUpdatedRecord  
        const removedRecordKeyField = removedOrUpdatedRecord.fieldValues.find(fv => keyname in fv);
        const removedRecordKeyValue = removedRecordKeyField?.[keyname];
        
        // Both values must exist and be equal
        return newRecordKeyValue !== undefined && 
               removedRecordKeyValue !== undefined && 
               newRecordKeyValue === removedRecordKeyValue;
      });
    });

    if (matchingRemovedIndex !== -1) {
      // Found a matching removed record, so this is a changed record
      updatedRecords.push(newOrUpdatedRecord);
      // Remove the matched removed record because it is now accounted for in the changed set
      removedOrUpdatedRecords.splice(matchingRemovedIndex, 1);
    } 
    else {
      addedRecords.push(newOrUpdatedRecord);
    }
  }

  return { added: addedRecords, updated: updatedRecords, removed: removedOrUpdatedRecords };
}
