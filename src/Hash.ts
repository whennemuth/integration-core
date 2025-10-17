
import { createHash } from 'crypto';
import { FieldSet } from './InputTypes';

/**
 * Generate a SHA-256 hash of the concatenated field values in a FieldSet.
 * @param fieldSet The FieldSet containing fieldValues to hash.
 * @param sort Whether to sort the fieldValues by field name before hashing. 
 * Defaults to false, assuming that fieldValues are already in a consistent natural order.
 * @returns A hexadecimal string representing the SHA-256 hash.
 */
export const hash = (fieldSet: FieldSet, sort: boolean = false): string => {
  const hash = createHash('sha256');
  if (sort) {
    fieldSet.fieldValues.sort((a, b) => {
      const aKey = Object.keys(a)[0];
      const bKey = Object.keys(b)[0];
      return aKey.localeCompare(bKey);
    });
  }

  const concatenatedValues = fieldSet.fieldValues
    .map(f => Object.values(f)[0]) // Get the value of each field
    .map(v => v === undefined ? '' : Array.isArray(v) ? v.join(',') : v.toString()) // Convert to string
    .join('|'); // Concatenate with a delimiter

  hash.update(concatenatedValues);
  return hash.digest('hex');
};