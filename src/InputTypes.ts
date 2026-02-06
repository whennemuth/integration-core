import { PushResult } from "./DataTarget";

/**
 * Definition of a field within an input schema. Used for field validation.
 * 
 * NOTE: field validation can only apply to the top level fields in the Field object.
 * This is because FieldValue supports nested structure, but FieldDefinition does not.
 * However, introducing nested FieldDefinitions would significantly complicate things and
 * require overhaul - Users would need to define complete nested schemas, which could 
 * become verbose and error-prone. For now, leave as is, and a case can be made for
 * removing field validation and use of FieldDefinition entirely.
 */
export type FieldDefinition = {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date' | 'email' | 'url' | 'select' | 'multiselect' | 'object' | 'array';
  isPrimaryKey?: boolean;
  label?: string;
  required: boolean;
  defaultValue?: string | number | boolean;
  restrictions?: FieldRestrictions[];
  options?: { matchCase: boolean; values: (string | number)[]; };
  description?: string;
};

export type FieldRestrictions = {
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  min?: number;
  max?: number;
  // An array of custom validation functions that return true if the value is valid
  custom?: Array<(value: FieldValue, row?: Array<Field>) => boolean>;
};

export type FieldValue = string | number | boolean | Array<string | number | FieldValue> | { [key: string]: FieldValue } | undefined;

export type Field = { 
  [key: string]: FieldValue; 
};

export type FieldSet = {
  fieldValues: Field[];
  validationMessages?: Map<string, string>;  
  hash?: string;
  pushResult?: PushResult;
}

export type Input = {
  fieldDefinitions: FieldDefinition[];
  fieldSets: FieldSet[];
};


/**
 * Base class for field validators to enforce a common constructor signature via a factory method.
 */
export abstract class FieldValidator {
  protected constructor(protected fldDef: FieldDefinition, protected field: Field) {}
  
  abstract isValid(row?: Array<Field>): boolean;
  abstract readonly validationMessage: string | undefined;

  // Static factory method enforced by abstract class
  static getInstance<T extends FieldValidator>(
    this: new (fldDef: FieldDefinition, field: Field) => T,
    fldDef: FieldDefinition, field: Field
  ): T {
    return new this(fldDef, field);
  }
}


