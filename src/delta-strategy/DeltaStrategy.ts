import { DeltaStorage } from "../DeltaTypes";
import { FieldSet } from "../InputTypes";
import { InputUtilsDecorator } from "../InputUtils";
import { DeltaStrategyParams } from "./DeltaStrategyParams";

/**
 * Abstract strategy class for delta storage operations
 * Encapsulates storage configuration, creation, and delta computation logic
 */
export abstract class DeltaStrategy {

  constructor(public parms: DeltaStrategyParams) { }

  /**
   * Abstract method to compute delta based on storage type
   */
  public abstract computeDelta(computeParms: {
    storage: DeltaStorage, 
    currentFieldSets: any[], 
    inputUtils: InputUtilsDecorator, 
    clientId: string
  }): Promise<any>;

  /**
   * Abstract method to create storage instance
   */
  public abstract get storage(): DeltaStorage;

  /**
   * Builds a limitTo array containing records that need previous hash data:
   * - Records that failed to push (from pushResult.failures)
   * - Records with validation errors (from currentKeyAndHashFieldSets)
   * This enables efficient database queries by limiting the fetch to only necessary records.
   */
  public static buildLimitToArray(currentKeyAndHashFieldSets: FieldSet[], pushResult: any): FieldSet[] {
    const limitTo: FieldSet[] = [];
    
    // Add records that failed to push
    if (pushResult.failures && pushResult.failures.length > 0) {
      pushResult.failures.forEach((failure: any) => {
        // Convert failure.primaryKey (Field[]) to FieldSet format
        const fieldValues = failure.primaryKey; // This is already Field[] format
        limitTo.push({ fieldValues, hash: '' }); // Hash doesn't matter for lookup
      });
    }
    
    // Add records with validation errors (missing hashes or validation messages)
    currentKeyAndHashFieldSets.forEach(fs => {
      if (!fs.hash || (fs.validationMessages && fs.validationMessages.size > 0)) {
        limitTo.push(fs);
      }
    });
    
    return limitTo;
  }

}