import { BatchPushResult } from "./DataTarget";
import { FieldDefinition, FieldSet, Input } from "./InputTypes";

export class InputUtilsDecorator implements Input {
  private primaryKeyFieldsNames: string[];
  
  constructor(private input: Input) { 
    this.primaryKeyFieldsNames = this.input.fieldDefinitions
      .filter(fd => fd.isPrimaryKey)
      .map(fd => fd.name);
  }
  
  public get fieldDefinitions(): FieldDefinition[] {
    return this.input.fieldDefinitions;
  }
  
  public get fieldSets(): FieldSet[] {
    return this.input.fieldSets;
  }
  
  public getPrimaryKey = (): Set<string> => {
    const pkFields = this.fieldDefinitions.filter(fd => fd.isPrimaryKey).map(fd => fd.name);
    return new Set(pkFields);
  }
  
  /**
   * A "reduced" version of the field sets containing only primary key fields and hash values.
   * @returns 
   */
  public getReducedFieldSets = (): FieldSet[] => {
    return this.fieldSets.map(fs => {
      const primaryKeyFields = fs.fieldValues.filter(fv => {
        const fieldName = Object.keys(fv)[0];
        return this.primaryKeyFieldsNames.includes(fieldName);
      });
      return { fieldValues: primaryKeyFields, hash: fs.hash, validationMessages: fs.validationMessages };
    });
  }

  /**
   * Restores previous hashes for records that failed to push to the data target, and also
   * for records with validation issues that lack hashes. This ensures that:
   * 1. Failed records maintain their old hashes so they continue to be detected as changed in the next run
   * 2. Invalid records are explicitly marked as unchanged (rather than ambiguously missing hashes)
   * 
   * @param currentInputFieldSets - The current reduced field sets to modify
   * @param previousInputFieldSets - The previous field sets with original hashes
   * @param pushResult - The batch push result containing failures
   */
  public restorePreviousHashesForFailures = (parms: {
    currentInputFieldSets: FieldSet[], 
    previousInputFieldSets: FieldSet[], 
    pushResult: BatchPushResult
  }): void => {
    const { currentInputFieldSets, previousInputFieldSets, pushResult } = parms;

    const primaryKeyFields = Array.from(this.getPrimaryKey());
    
    // Create a map of previous records by primary key for quick lookup
    const previousRecordsByPK = new Map<string, FieldSet>();
    previousInputFieldSets.forEach(prevRecord => {
      const pkValues = primaryKeyFields.map((pkField: string) => {
        const field = prevRecord.fieldValues.find(fv => Object.keys(fv)[0] === pkField);
        return field ? Object.values(field)[0] : '';
      });
      const pkKey = pkValues.join('|');
      previousRecordsByPK.set(pkKey, prevRecord);
    });
    
    // Helper function to restore hash for a record matching a primary key
    const restoreHashForPrimaryKey = (pkKey: string) => {
      const previousRecord = previousRecordsByPK.get(pkKey);
      if (previousRecord) {
        const currentRecordIndex = currentInputFieldSets.findIndex(currentRecord => {
          const currentPkValues = primaryKeyFields.map((pkField: string) => {
            const field = currentRecord.fieldValues.find(fv => Object.keys(fv)[0] === pkField);
            return field ? Object.values(field)[0] : '';
          });
          const currentPkKey = currentPkValues.join('|');
          return currentPkKey === pkKey;
        });
        
        if (currentRecordIndex >= 0) {
          currentInputFieldSets[currentRecordIndex].hash = previousRecord.hash;
        }
      }
    };

    // Restore hashes for push failures
    if (pushResult.failures && pushResult.failures.length > 0) {
      pushResult.failures.forEach(failure => {
        const failurePkValues = failure.primaryKey.map(pkField => Object.values(pkField)[0]);
        const failurePkKey = failurePkValues.join('|');
        restoreHashForPrimaryKey(failurePkKey);
      });
    }

    // Restore hashes for records with validation issues (missing hashes)
    currentInputFieldSets.forEach((currentRecord, index) => {
      if (!currentRecord.hash && currentRecord.validationMessages && currentRecord.validationMessages.size > 0) {
        const currentPkValues = primaryKeyFields.map((pkField: string) => {
          const field = currentRecord.fieldValues.find(fv => Object.keys(fv)[0] === pkField);
          return field ? Object.values(field)[0] : '';
        });
        const currentPkKey = currentPkValues.join('|');
        restoreHashForPrimaryKey(currentPkKey);
      }
    });
  }
}