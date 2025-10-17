import { Field, FieldDefinition, FieldValidator, Input } from "../src/InputTypes";
import { InputParser } from "../src/InputParser";
import { BasicFieldValidator } from "../src/InputValidation";

const getFieldValidator = (fieldDef: FieldDefinition, field: Field): FieldValidator => {
  return new BasicFieldValidator(fieldDef, field);
}

describe('Input Parser', () => {

  it('should provide validationMessages for invalid rows, and hash those that are not invalid', () => {
    // Arrange
    const input = {
      fieldDefinitions: [
        { name: 'id', type: 'number', required: true },
        { name: 'name', type: 'string', required: true, restrictions: [ { minLength: 4 } ] },
        { name: 'email', type: 'email', required: false }
      ],
      fieldSets: [
        { fieldValues: [ { id: 1 }, { name: 'Alice' }, { email: 'alice@example.com' } ] },
        { fieldValues: [ { id: 2 }, { name: 'Bob' }, { email: 'bob@example.com' } ] },
        { fieldValues: [ { id: 3 }, { name: 'Charlie' }, { email: 'invalid-email' } ] }
      ]
    } satisfies Input;
    const parser = new InputParser({ fieldValidator: getFieldValidator, _input: input });

    // Act
    const hasInvalid = parser.hasInvalidRows();
    const validRows = parser.getValidRows();
    const invalidRows = parser.getInvalidRows();

    // Assert
    expect(validRows.length).toBe(1);
    expect(validRows[0].hash).not.toBeUndefined();
    expect(validRows[0].validationMessages?.entries.length).toBe(0);

    expect(hasInvalid).toBe(true);
    expect(invalidRows.length).toBe(2);
    expect(invalidRows[0].validationMessages?.get('name')).toBe('Minimum length is 4: Bob');
    expect(invalidRows[0].hash).toBeUndefined();
    expect(invalidRows[1].validationMessages?.get('email')).toBe('Invalid email format: invalid-email');
    expect(invalidRows[1].hash).toBeUndefined();
  });

});