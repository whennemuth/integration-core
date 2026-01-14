import { Field, FieldDefinition, FieldSet, FieldValidator, FieldValue } from "./InputTypes";

/**
 * Apply the configured validation in the field definition to the field value to determine its validity.
 */
export class BasicFieldValidator extends FieldValidator {
  private _validationMessage: string | undefined;

  constructor(fldDef: FieldDefinition, field: Field) {
    super(fldDef, field);
  }

  /**
   * Recursively validate nested FieldValue structures
   * @param value The FieldValue to validate
   * @param fieldName The name of the field being validated (for error messages)
   * @param depth Current recursion depth
   * @param maxDepth Maximum allowed recursion depth
   * @returns true if valid, false otherwise
   */
  private validateNestedValue(value: FieldValue, fieldName: string, depth: number = 0, maxDepth: number = 10): boolean {
    if (depth > maxDepth) {
      this._validationMessage = `Maximum nesting depth (${maxDepth}) exceeded for field ${fieldName}`;
      return false;
    }
    
    if (value === undefined || value === null) {
      return true; // null/undefined are handled by required field validation
    }
    
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return true; // Primitive types are valid
    }
    
    if (Array.isArray(value)) {
      // Validate each array element recursively
      for (let i = 0; i < value.length; i++) {
        if (!this.validateNestedValue(value[i], `${fieldName}[${i}]`, depth + 1, maxDepth)) {
          return false;
        }
      }
      return true;
    }
    
    if (typeof value === 'object') {
      // Validate nested object properties recursively
      for (const [key, nestedValue] of Object.entries(value)) {
        if (!this.validateNestedValue(nestedValue as FieldValue, `${fieldName}.${key}`, depth + 1, maxDepth)) {
          return false;
        }
      }
      return true;
    }
    
    this._validationMessage = `Unsupported value type for field ${fieldName}: ${typeof value}`;
    return false;
  }

  public isValid(row?: Field[]): boolean {
    const { field, fldDef: { name, restrictions = [], required, defaultValue, type, options } } = this;
    const fldValue = field[name];
    const isEmpty = fldValue === undefined || fldValue === null || fldValue === '';

    // First, validate the nested structure
    if (!this.validateNestedValue(fldValue, name)) {
      return false;
    }

    if( ! required && isEmpty) {
      return true; // If not required and field is empty, it's valid
    }
    if (required && isEmpty) {
      if(defaultValue !== undefined) {
        return true; // If required but has a default value, it's valid
      }
      this._validationMessage = `${name} is required`;
      return false;
    }

    // Skip primitive type checking for complex nested values
    if (typeof fldValue === 'object' && (Array.isArray(fldValue) || fldValue !== null)) {
      // For nested objects/arrays, we've already validated structure above
      // and don't need to check primitive types
    } else {
      if (type === 'number' && typeof fldValue !== 'number') {
        this._validationMessage = `Expected a number`;
        return false;
      }
      if (type === 'string' && typeof fldValue !== 'string') {
        this._validationMessage = `Expected a string`;
        return false;
      }
      if (type === 'boolean' && typeof fldValue !== 'boolean') {
        this._validationMessage = `Expected a boolean`;
        return false;
      }
      if (type === 'date' && fldValue !== undefined && fldValue !== null) {
        const isValidDate = (fldValue as any instanceof Date) || 
          (typeof fldValue === 'string' && !isNaN(Date.parse(fldValue)));
        if (!isValidDate) {
          this._validationMessage = `Expected a date.`;
          return false;
        }
      }
    }

    if (type === 'email') {
      const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (typeof fldValue !== 'string' || !emailPattern.test(fldValue)) {
        this._validationMessage = `Invalid email format: ${fldValue}`;
        return false;
      }
    }
    if (type === 'url') {
      try {
        new URL(fldValue as string);
      } 
      catch {
        this._validationMessage = `Invalid URL format: ${fldValue}`;
        return false;
      }
    }
    if (type === 'select' || type === 'multiselect') {

      /**
       * Determine if all fieldValues are in options considering case sensitivity if configured.
       * Comparisons are made between the string representations of the values.
       * @param fieldValues 
       * @returns 
       */
      const inList = (fieldValues: any[]): boolean => {
        return fieldValues.every(fv => options!.values.some(v => options!.matchCase ? 
          v.toString() === fv.toString() : 
          v.toString().toLowerCase() === fv.toString().toLowerCase()));
      }

      if (Array.isArray(fldValue)) {
        if (options && ! inList(fldValue)) {
          this._validationMessage = `One or more values not in options: ${fldValue}`;
          return false;
        }
      } 
      else {
        if (options && ! inList([fldValue])) {
          this._validationMessage = `Value not in options: ${fldValue}`;
          return false;
        }
      }
    }


    // Now validate against the specific restriction rules
    for (const restriction of restrictions) {
      const { minLength, maxLength, pattern, min, max, custom = [] } = restriction;
      if (minLength !== undefined && typeof fldValue === 'string') {
        if (fldValue.length < minLength) {
          this._validationMessage = `Minimum length is ${minLength}: ${fldValue}`;
          return false;
        }
      }
      if (maxLength !== undefined && typeof fldValue === 'string') {
        if (fldValue.length > maxLength) {
          this._validationMessage = `Maximum length is ${maxLength}: ${fldValue}`;
          return false;
        }
      }
      if (pattern !== undefined) {
        const fv = `${fldValue}`
        const regex = new RegExp(pattern);
        if (!regex.test(fv)) {
          this._validationMessage = `Value does not match pattern ${pattern}: ${fldValue}`;
          return false;
        }
      }
      if (min !== undefined && typeof fldValue === 'number') {
        if (fldValue < min) {
          this._validationMessage = `Minimum value is ${min}: ${fldValue}`;
          return false;
        }
      }
      if (max !== undefined && typeof fldValue === 'number') {
        if (fldValue > max) {
          this._validationMessage = `Maximum value is ${max}: ${fldValue}`;
          return false;
        }
      }
      
      for (const customValidator of custom) {
        if ( ! customValidator(fldValue, row || [])) {
          this._validationMessage = `Custom validation failed: ${fldValue}`;
          return false;
        }
      }
    }
    return true;
  }

  public get validationMessage(): string | undefined {
    return this._validationMessage;
  }

}

/**
 * Apply the configured validations in the field definitions to the row values to determine the row's validity.
 */
export class RowValidator {
  private _validationMessages: Map<string, string> | undefined = undefined;

  constructor(
    private getFieldValidator: (fieldDef: FieldDefinition, field: Field) => FieldValidator, 
    private fieldDefinitions: FieldDefinition[], 
    private row: FieldSet) {}

  public isValid(): boolean {
    const { fieldDefinitions, row } = this;
    this._validationMessages = new Map();
    for (let i = 0; i < fieldDefinitions.length; i++) {
      const fieldDef = fieldDefinitions[i];
      const blankField = { [fieldDef.name]: undefined };
      const field = row.fieldValues.find(f => Object.keys(f)[0] === fieldDef.name) || blankField;
      const fieldValidator = this.getFieldValidator(fieldDef, field);
      if (fieldValidator.isValid(row.fieldValues)) {
        continue;
      }
      const { validationMessage } = fieldValidator;
      if( ! validationMessage) {
        continue; // Should be impossible to reach here, but just in case.
      }
      this._validationMessages.set(fieldDef.name, validationMessage);
    }
    row.validationMessages = this._validationMessages;
    return this._validationMessages.size === 0;
  }

  public get validationMessages(): Map<string, string> {
    if(this._validationMessages === undefined) {
      this.isValid();
    }
    return this._validationMessages!;
  }
}