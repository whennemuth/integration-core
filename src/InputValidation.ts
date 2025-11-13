import { Field, FieldDefinition, FieldSet, FieldValidator } from "./InputTypes";

/**
 * Apply the configured validation in the field definition to the field value to determine its validity.
 */
export class BasicFieldValidator extends FieldValidator {
  private _validationMessage: string | undefined;

  constructor(fldDef: FieldDefinition, field: Field) {
    super(fldDef, field);
  }

  /**
   * Validates the field against the defined validators.
   * @param row - Optional row context for custom validators
   * @returns True if the field is valid, false otherwise
   */
  public isValid(row?: Array<Field>): boolean {
    const { field, fldDef: { name, restrictions = [], required, defaultValue, type, options } } = this;
    const fldValue = field[name];
    const isEmpty = fldValue === undefined || fldValue === null || fldValue === '';

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
    if (type === 'date' && !(fldValue instanceof Date) && isNaN(Date.parse(fldValue as string))) {
      this._validationMessage = `Expected a date.`;
      return false;
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
        if ( ! customValidator(fldValue, row)) {
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