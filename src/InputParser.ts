import { hash } from "./Hash";
import { Field, FieldDefinition, FieldSet, FieldValidator, Input } from "./InputTypes";
import { RowValidator } from "./InputValidation";

/**
 * Represents a full set of input data with field definitions and validation.
 */
export class InputParser {
  private validRows: FieldSet[] | undefined = undefined;
  private invalidRows: FieldSet[] | undefined = undefined;

  constructor(
    private parms: {
      fieldValidator?: (fieldDef: FieldDefinition, field: Field) => FieldValidator, 
      _input?: Input
    }) {
      const { _input } = this.parms;
      if(_input) {
        this.parse(_input);
      }
    }

  /**
   * Validate the input data and compute hashes for valid rows.
   */
  public parse = (input?: Input): Input => {
    let { fieldValidator, _input } = this.parms;
    const { fieldDefinitions, fieldSets } = _input || input || {};
    if( ! fieldDefinitions || ! fieldSets) {
      throw new Error("Input data must be provided to parse.");
    }
    this.validRows = [];
    this.invalidRows = [];

    if( ! fieldValidator) {
      // Dummy field validator that always returns valid
      fieldValidator = (fieldDef: FieldDefinition, field: Field): FieldValidator => 
        (class extends FieldValidator {
          constructor(fldDef: FieldDefinition, field: Field) { super(fldDef, field); }
          isValid(): boolean { return true; }
          get validationMessage(): string | undefined { return undefined; }
        }).getInstance(fieldDef, field);
    }

    for (let i = 0; i < fieldSets.length; i++) {
      const row = fieldSets[i];
      const rowValidator = new RowValidator(fieldValidator, fieldDefinitions, row);
      if (rowValidator.isValid()) {
        row.hash = hash(row);
        this.validRows?.push(row);
      }
      else {
        row.validationMessages = rowValidator.validationMessages;
        this.invalidRows.push(row);
      }
    }
    return _input || input!;
  }

  public set input (input: Input) {
    this.input = input;
    this.parse(input);
  }

  public hasInvalidRows = (): boolean => {
    return this.getInvalidRows().length > 0;
  }

  public getInvalidRows = (): FieldSet[] => {
    if(this.invalidRows === undefined) {
      this.parse(this.input!);
    }
    return this.invalidRows!;
  }

  public getValidRows = (): FieldSet[] => {
    if(this.invalidRows === undefined) {
      this.parse(this.input!);
    }
    return this.validRows!;
  }
}