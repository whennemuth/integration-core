import { Field, FieldDefinition, FieldValidator } from "../src/InputTypes";
import { BasicFieldValidator, RowValidator } from "../src/InputValidation";

const clone = <T>(obj: T): T => {
  return JSON.parse(JSON.stringify(obj)) as T;
}

const getFieldValidator = (fieldDef: FieldDefinition, field: Field): FieldValidator => {
  return new BasicFieldValidator(fieldDef, field);
}

describe('Input Validation', () => {

  it('should validate missing fields properly', () => {
    // Arrange
    const fieldDefinitions = [
      { name: 'username', type: 'string', required: true },
      { name: 'age', type: 'number', required: false },
      { name: 'email', type: 'email', required: true }    
    ] satisfies FieldDefinition[];
    const validRow = { 'fieldValues': [ { 'username': 'user1' }, { 'age': 25 }, { 'email': 'user1@example.com' } ] };
    const invalidRow1 = { 'fieldValues': [ { 'username': 'user2' }, { 'age': 30 } ] }; // Missing email
    const invalidRow2 = { 'fieldValues': [ { 'username': 'user1' }, { 'age': 40 }, { 'email': '' } ] }; // Empty email
    const invalidRow3 = { 'fieldValues': [ { 'username': 'user1' }, { 'age': 40 }, { 'email': undefined } ] }; // Undefined email
    const rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, validRow);
    const rowValidatorInvalid1 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow1);
    const rowValidatorInvalid2 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow2);
    const rowValidatorInvalid3 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow3);

    // Act
    const isValid = rowValidator.isValid();
    const isInvalid1 = ! rowValidatorInvalid1.isValid();
    const isInvalid2 = ! rowValidatorInvalid2.isValid();
    const isInvalid3 = ! rowValidatorInvalid3.isValid();

    // Assert
    expect(isValid).toBe(true);
    expect(isInvalid1).toBe(true);
    expect(isInvalid2).toBe(true);
    expect(isInvalid3).toBe(true);
    expect(rowValidator.validationMessages.size).toBe(0);
    expect(rowValidatorInvalid1.validationMessages?.get('email')).toContain('email is required');
    expect(rowValidatorInvalid2.validationMessages?.get('email')).toContain('email is required');
    expect(rowValidatorInvalid3.validationMessages?.get('email')).toContain('email is required');
  });

  it('should validate field sizes properly', () => {
    // Arrange
    const fieldDefinitions = [
      { name: 'username', type: 'string', required: true,
        restrictions: [ { minLength: 3 }, { maxLength: 10 }, { pattern: '^[a-zA-Z0-9_]+$' } ]
      },
      { name: 'age', type: 'number', required: false, restrictions: [ { min: 0 }, { max: 120 } ] },
      { name: 'email', type: 'email', required: true, restrictions: [ { maxLength: 20, minLength: 14 } ] }    
    ] satisfies FieldDefinition[];

    const validRow = { 'fieldValues': [ { 'username': 'user_1' }, { 'age': 25 }, { 'email': 'user_1@example.com' } ] };
    const invalidRow = { 'fieldValues': [ { 'username': 'u' }, { 'age': 150 }, { 'email': 'too@short.com' } ] };

    const rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, validRow);
    const rowValidatorInvalid = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow);

    // Act
    const isValid = rowValidator.isValid();
    const isInvalid = ! rowValidatorInvalid.isValid();
    const messages = rowValidatorInvalid.validationMessages;

    // Assert
    expect(isValid).toBe(true);
    expect(isInvalid).toBe(true);
    expect(messages.size).toBe(3);
    expect(messages.get('username')).toBe('Minimum length is 3: u');
    expect(messages.get('age')).toBe('Maximum value is 120: 150');
    expect(messages.get('email')).toBe('Minimum length is 14: too@short.com');
  });

  it('should validate email properly', () => {
    // Arrange
    const fieldDefinitions = [
      { name: 'randomField', type: 'string', required: false },
      { name: 'email', type: 'email', required: false }    
    ] as FieldDefinition[];

    let validRow = { 'fieldValues': [ { 'randomField': 'someValue' }, { 'email': 'user1@example.com' } ] };
    let invalidRow = { 'fieldValues': [ { 'randomField': 'someValue' }, { 'email': 'invalidEmail' } ] };

    let rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, validRow);
    let rowValidatorInvalid = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow);

    // Act
    let isValid = rowValidator.isValid();
    let isInvalid = ! rowValidatorInvalid.isValid();
    let messages = rowValidatorInvalid.validationMessages;

    // Assert
    expect(isValid).toBe(true);
    expect(isInvalid).toBe(true);
    expect(messages.size).toBe(1);
    expect(messages.get('email')).toBe('Invalid email format: invalidEmail');

    // Add a validator that should invalidate both rows
    fieldDefinitions[1].restrictions = [ { maxLength: 5 } ];

    // Arrange
    rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, validRow);
    rowValidatorInvalid = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow);

    // Act
    isValid = rowValidator.isValid();
    isInvalid = ! rowValidatorInvalid.isValid();
    const messages1 = rowValidator.validationMessages;
    const messages2 = rowValidatorInvalid.validationMessages;

    // Assert
    expect(isValid).toBe(false);
    expect(isInvalid).toBe(true);
    expect(messages1.size).toBe(1);
    expect(messages2.size).toBe(1);
    expect(messages1.get('email')).toBe('Maximum length is 5: user1@example.com');
    expect(messages2.get('email')).toBe('Invalid email format: invalidEmail');
  });

  it('should validate single select from options properly', () => {
    // Arrange
    const fieldDefinitions = [
      { name: 'status', type: 'select', required: true,
        options: { matchCase: false, values: ['active', 'inactive', 'pending'] }
      },
      { name: 'code', type: 'select', required: false,
        options: { matchCase: true, values: [100, 200, 300] }
      }
    ] as FieldDefinition[];

    const validRow1 = { 'fieldValues': [ { 'status': 'active' }, { 'code': 100 } ] };
    const validRow2 = { 'fieldValues': [ { 'status': 'INACTIVE' }, { 'code': 200 } ] };
    const validRow3 = { 'fieldValues': [ { 'status': 'active' }, { 'code': '100' } ] }; // strings can compare to numbers
    const invalidRow1 = { 'fieldValues': [ { 'status': 'unknown' }, { 'code': 100 } ] }; // status not in options
    const invalidRow2 = { 'fieldValues': [ { 'status': 'active' }, { 'code': 400 } ] }; // code not in options
    const invalidRow3 = { 'fieldValues': [ { 'status': 'active' }, { 'code': true } ] }; // code wrong type
    const invalidRow4 = { 'fieldValues': [ { 'status': 'ACTIVE' }, { 'code': 100 } ] }; // Will not match case

    const rowValidator1 = new RowValidator(getFieldValidator, fieldDefinitions, validRow1);
    const rowValidator2 = new RowValidator(getFieldValidator, fieldDefinitions, validRow2);
    const rowValidator3 = new RowValidator(getFieldValidator, fieldDefinitions, validRow3);
    const rowValidatorInvalid1 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow1);
    const rowValidatorInvalid2 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow2);
    const rowValidatorInvalid3 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow3);
    // Create a validator that is case sensitive for status
    const fieldDefinitions2 = clone(fieldDefinitions);
    fieldDefinitions2[0].options!.matchCase = true;
    const rowValidatorInvalid4 = new RowValidator(getFieldValidator, fieldDefinitions2, invalidRow4);

    // Act
    const isValid1 = rowValidator1.isValid();
    const isValid2 = rowValidator2.isValid();
    const isValid3 = rowValidator3.isValid();
    const isInvalid1 = ! rowValidatorInvalid1.isValid();
    const isInvalid2 = ! rowValidatorInvalid2.isValid();
    const isInvalid3 = ! rowValidatorInvalid3.isValid();
    const isInvalid4 = ! rowValidatorInvalid4.isValid();

    // Assert
    expect(isValid1).toBe(true);
    expect(isValid2).toBe(true);
    expect(isValid3).toBe(true);
    expect(isInvalid1).toBe(true);
    expect(isInvalid2).toBe(true);
    expect(isInvalid3).toBe(true);
    expect(isInvalid4).toBe(true);
    expect(rowValidator1.validationMessages.size).toBe(0);
    expect(rowValidator2.validationMessages.size).toBe(0);
    expect(rowValidator3.validationMessages.size).toBe(0);
    expect(rowValidatorInvalid1.validationMessages?.get('status')).toBe('Value not in options: unknown');
    expect(rowValidatorInvalid2.validationMessages?.get('code')).toBe('Value not in options: 400');
    expect(rowValidatorInvalid3.validationMessages?.get('code')).toBe('Value not in options: true');
    expect(rowValidatorInvalid4.validationMessages?.get('status')).toBe('Value not in options: ACTIVE');
  });

  it('should validate multi select from options properly', () => {
    // Arrange
    const fieldDefinitions = [
      { name: 'tags', type: 'multiselect', required: true,
        options: { matchCase: false, values: ['red', 'green', 'blue'] }
      },
      { name: 'codes', type: 'multiselect', required: false,
        options: { matchCase: true, values: [100, 200, 300] }
      }
    ] as FieldDefinition[];

    const validRow1 = { 'fieldValues': [ { 'tags': ['red', 'blue'] }, { 'codes': [100, 200] } ] };
    const validRow2 = { 'fieldValues': [ { 'tags': ['GREEN'] }, { 'codes': [200] } ] };
    const validRow3 = { 'fieldValues': [ { 'tags': ['red'] }, { 'codes': ['100', 300] } ] }; // strings can compare to numbers
    const invalidRow1 = { 'fieldValues': [ { 'tags': 'yellow' }, { 'codes': 100 } ] }; // tags not in options
    const invalidRow2 = { 'fieldValues': [ { 'tags': ['red'] }, { 'codes': [400] } ] }; // codes not in options
    const invalidRow3 = { 'fieldValues': [ { 'tags': ['red', 'yellow'] }, { 'codes': [100] } ] }; // one tag not in options

    const rowValidator1 = new RowValidator(getFieldValidator, fieldDefinitions, validRow1);
    const rowValidator2 = new RowValidator(getFieldValidator, fieldDefinitions, validRow2);
    const rowValidator3 = new RowValidator(getFieldValidator, fieldDefinitions, validRow3);
    const rowValidatorInvalid1 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow1);
    const rowValidatorInvalid2 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow2);
    const rowValidatorInvalid3 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow3);
    // Create a validator that is case sensitive for tags
    const fieldDefinitions2 = clone(fieldDefinitions);
    fieldDefinitions2[0].options!.matchCase = true;
    const rowValidatorInvalid4 = new RowValidator(getFieldValidator, fieldDefinitions2, validRow2);

    // Act
    const isValid1 = rowValidator1.isValid();
    const isValid2 = rowValidator2.isValid();
    const isValid3 = rowValidator3.isValid();
    const isInvalid1 = ! rowValidatorInvalid1.isValid();
    const isInvalid2 = ! rowValidatorInvalid2.isValid();
    const isInvalid3 = ! rowValidatorInvalid3.isValid();
    const isInvalid4 = ! rowValidatorInvalid4.isValid();

    // Assert
    expect(isValid1).toBe(true);
    expect(isValid2).toBe(true);
    expect(isValid3).toBe(true);
    expect(isInvalid1).toBe(true);
    expect(isInvalid2).toBe(true);
    expect(isInvalid3).toBe(true);
    expect(rowValidator1.validationMessages.size).toBe(0);
    expect(rowValidator2.validationMessages.size).toBe(0);
    expect(rowValidator3.validationMessages.size).toBe(0);
    expect(rowValidatorInvalid1.validationMessages?.get('tags')).toBe('Value not in options: yellow');
    expect(rowValidatorInvalid2.validationMessages?.get('codes')).toBe('One or more values not in options: 400');
    expect(rowValidatorInvalid3.validationMessages?.get('tags')).toBe('One or more values not in options: red,yellow');
    expect(rowValidatorInvalid4.validationMessages?.get('tags')).toBe('One or more values not in options: GREEN');
  });

  it('should validate custom validators properly', () => {
    // Arrange
    const isEven = (value: any): boolean => {
      return typeof value === 'number' && value % 2 === 0;
    };
    const isGreaterThanField = (otherFieldName: string) => {
      return (value: any, row?: Array<{ [key: string]: any }>): boolean => {
        if (typeof value !== 'number' || ! row) {
          return false;
        }
        const otherField = row.find(f => Object.keys(f)[0] === otherFieldName);
        if ( ! otherField) {
          return false;
        }
        const otherValue = otherField[otherFieldName];
        return typeof otherValue === 'number' && value > otherValue;
      };
    };

    const fieldDefinitions = [
      { name: 'evenNumber', type: 'number', required: true,
        restrictions: [ { custom: [ isEven ] } ]
      },
      { name: 'greaterNumber', type: 'number', required: true,
        restrictions: [ { custom: [ isGreaterThanField('evenNumber') ] } ]
      }
    ] as FieldDefinition[];

    const validRow = { 'fieldValues': [ { 'evenNumber': 4 }, { 'greaterNumber': 10 } ] };
    const invalidRow1 = { 'fieldValues': [ { 'evenNumber': 3 }, { 'greaterNumber': 10 } ] }; // evenNumber not even
    const invalidRow2 = { 'fieldValues': [ { 'evenNumber': 4 }, { 'greaterNumber': 2 } ] }; // greaterNumber not greater
    const invalidRow3 = { 'fieldValues': [ { 'evenNumber': 3 }, { 'greaterNumber': 2 } ] }; // both invalid

    const rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, validRow);
    const rowValidatorInvalid1 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow1);
    const rowValidatorInvalid2 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow2);
    const rowValidatorInvalid3 = new RowValidator(getFieldValidator, fieldDefinitions, invalidRow3);

    // Act
    const isValid = rowValidator.isValid();
    const isInvalid1 = ! rowValidatorInvalid1.isValid();
    const isInvalid2 = ! rowValidatorInvalid2.isValid();
    const isInvalid3 = ! rowValidatorInvalid3.isValid();

    // Assert
    expect(isValid).toBe(true);
    expect(isInvalid1).toBe(true);
    expect(isInvalid2).toBe(true);
    expect(isInvalid3).toBe(true);
    expect(rowValidator.validationMessages.size).toBe(0);
    expect(rowValidatorInvalid1.validationMessages?.get('evenNumber')).toBe('Custom validation failed: 3');
    expect(rowValidatorInvalid2.validationMessages?.get('greaterNumber')).toBe('Custom validation failed: 2');
    expect(rowValidatorInvalid3.validationMessages?.get('evenNumber')).toBe('Custom validation failed: 3');
    expect(rowValidatorInvalid3.validationMessages?.get('greaterNumber')).toBe('Custom validation failed: 2');
  });
});