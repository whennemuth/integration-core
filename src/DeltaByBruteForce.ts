import { DeltaEngine, DeltaParms, DeltaResult } from "./DeltaTypes";
import { FieldSet } from "./InputTypes";

/**
 * A brute-force implementation of the DeltaEngine that computes the delta between two datasets by 
 * comparing values from the hashed result of each of their combined field sets (records).
 * Overlapping hashes indicate unchanged records, while non-overlapping hashes indicate new, changed, 
 * or removed records. This implementation is straightforward but reaches a performance considerations
 * at around 200,000 records.
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
    const previousIds = previous.map(record => record.hash).filter((hash): hash is string => hash !== undefined);
    const currentIds = current.map(record => record.hash).filter((hash): hash is string => hash !== undefined);
    const previousSet = new Set(previousIds);
    const currentSet = new Set(currentIds);

    // Find added/removed efficiently 
    const addedHashes = currentIds.filter(id => !previousSet.has(id));
    const removedHashes = previousIds.filter(id => !currentSet.has(id));

    // Get a subset of the original records based on the added/removed hashes (should be small, so performance is ok)
    const addedRecords = current.filter(record => record.hash && addedHashes.includes(record.hash));
    const removedRecords = previous.filter(record => record.hash && removedHashes.includes(record.hash));

    return fishOutTheUpdates(addedRecords, removedRecords);
  }
}

/**
 * Use a comparison based on the primary key set (set of field names) which form the basis of finding
 * the "same" record between added and removed records to determine if it is truly new or removed or just 
 * "changed". This is less efficient, but the added/removed sets should be small enough to make 
 * this feasible. 
 */
export const fishOutUpdatedRecordsByPK = (added: FieldSet[], removed: FieldSet[], primaryKeySet: Set<string>): DeltaResult => {
  if (primaryKeySet.size === 0) {
    // If the primary key set is not provided, we cannot determine changed records, so we skip this step.
    return { added: added, updated: [], removed };
  }
  const changedRecords: FieldSet[] = [];
  const stillAddedRecords: FieldSet[] = [];
  for (const addedRecord of added) {
    const matchingRemovedIndex = removed.findIndex(removedRecord => {
      return Array.from(primaryKeySet.values()).every(keyname => {
        // Return true if a field in addedRecords can be found with a name that matches keyname 
        // and whose value is the same in removedRecord for a field of that same name.
        return addedRecord.fieldValues.some(fv => 
          removedRecord.fieldValues.some(rfv => 
            rfv[`${keyname}`] === fv[`${keyname}`]));
      });
    });

    if (matchingRemovedIndex !== -1) {
      // Found a matching removed record, so this is a changed record
      changedRecords.push(addedRecord);
      // Remove the matched removed record to prevent duplicate matches
      removed.splice(matchingRemovedIndex, 1);
    } 
    else {
      stillAddedRecords.push(addedRecord);
    }
  }

  return { added: stillAddedRecords, updated: changedRecords, removed };
}
