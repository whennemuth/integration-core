
import { createHash } from 'crypto';
import { FieldSet, FieldValue } from './InputTypes';

/**
 * Recursively serialize a FieldValue to a consistent string representation
 * @param value The FieldValue to serialize
 * @param depth Current recursion depth to prevent infinite loops
 * @param maxDepth Maximum allowed recursion depth
 */
const serializeValue = (value: FieldValue, depth: number = 0, maxDepth: number = 10): string => {
  if (depth > maxDepth) {
    throw new Error(`Maximum recursion depth (${maxDepth}) exceeded during value serialization`);
  }
  
  if (value === undefined || value === null) {
    return '';
  }
  
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value.toString();
  }
  
  if (Array.isArray(value)) {
    return value.map(item => serializeValue(item, depth + 1, maxDepth)).join(',');
  }
  
  // Handle nested objects (FieldValue can be an object with FieldValue properties)
  if (typeof value === 'object' && value !== null) {
    const sortedKeys = Object.keys(value).sort();
    return sortedKeys
      .map(key => `${key}:${serializeValue((value as any)[key], depth + 1, maxDepth)}`)
      .join(';');
  }
  
  // Fallback for any other case
  return String(value);
};

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
    .map(v => serializeValue(v)) // Convert to string recursively
    .join('|'); // Concatenate with a delimiter

  hash.update(concatenatedValues);
  return hash.digest('hex');
};