import { Field, FieldDefinition, FieldSet, FieldValidator, Input } from "../src/InputTypes";
import { InputParser } from "../src/InputParser";
import { BasicFieldValidator } from "../src/InputValidation";

const getFieldValidator = (fieldDef: FieldDefinition, field: Field): FieldValidator => {
  return new BasicFieldValidator(fieldDef, field);
}

/**
 * Helper function to create a field filter that excludes specified fields from hash computation
 */
const createExcludeFieldsFilter = (excludeFields: string[]) => {
  return (fieldSet: FieldSet): FieldSet => {
    if (excludeFields.length === 0) {
      return fieldSet;
    }
    const filteredFieldValues = fieldSet.fieldValues
      .map(field => {
        const filteredField: Field = {};
        Object.keys(field).forEach(key => {
          if (!excludeFields.includes(key)) {
            filteredField[key] = field[key];
          }
        });
        return filteredField;
      })
      .filter(field => Object.keys(field).length > 0); // Remove empty objects
    return { ...fieldSet, fieldValues: filteredFieldValues };
  };
};

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

  describe('fieldFilter functionality', () => {
    it('should include all fields in hash when no fieldFilter is provided', () => {
      // Arrange
      const input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'name', type: 'string', required: true },
          { name: 'timestamp', type: 'string', required: false }
        ],
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-01' } ] }
        ]
      } satisfies Input;
      const parser = new InputParser({ fieldValidator: getFieldValidator, _input: input });

      // Act
      const validRows = parser.getValidRows();
      const hash1 = validRows[0].hash;

      // Create identical input with different timestamp
      const input2 = {
        fieldDefinitions: input.fieldDefinitions,
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-02' } ] }
        ]
      } satisfies Input;
      const parser2 = new InputParser({ fieldValidator: getFieldValidator, _input: input2 });
      const validRows2 = parser2.getValidRows();
      const hash2 = validRows2[0].hash;

      // Assert - hashes should be different because timestamp is included
      expect(hash1).toBeDefined();
      expect(hash2).toBeDefined();
      expect(hash1).not.toBe(hash2);
    });

    it('should exclude specified fields from hash computation', () => {
      // Arrange
      const input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'name', type: 'string', required: true },
          { name: 'timestamp', type: 'string', required: false }
        ],
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-01' } ] }
        ]
      } satisfies Input;
      const parser = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input,
        fieldFilter: createExcludeFieldsFilter(['timestamp'])
      });

      // Act
      const validRows = parser.getValidRows();
      const hash1 = validRows[0].hash;

      // Create identical input with different timestamp (should produce same hash)
      const input2 = {
        fieldDefinitions: input.fieldDefinitions,
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-02' } ] }
        ]
      } satisfies Input;
      const parser2 = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input2,
        fieldFilter: createExcludeFieldsFilter(['timestamp'])
      });
      const validRows2 = parser2.getValidRows();
      const hash2 = validRows2[0].hash;

      // Assert - hashes should be identical because timestamp is excluded
      expect(hash1).toBeDefined();
      expect(hash2).toBeDefined();
      expect(hash1).toBe(hash2);
    });

    it('should produce different hashes when non-excluded fields differ', () => {
      // Arrange
      const input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'name', type: 'string', required: true },
          { name: 'timestamp', type: 'string', required: false }
        ],
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-01' } ] }
        ]
      } satisfies Input;
      const parser = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input,
        fieldFilter: createExcludeFieldsFilter(['timestamp'])
      });

      // Act
      const validRows = parser.getValidRows();
      const hash1 = validRows[0].hash;

      // Create input with different name (non-excluded field)
      const input2 = {
        fieldDefinitions: input.fieldDefinitions,
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Bob' }, { timestamp: '2024-01-01' } ] }
        ]
      } satisfies Input;
      const parser2 = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input2,
        fieldFilter: createExcludeFieldsFilter(['timestamp'])
      });
      const validRows2 = parser2.getValidRows();
      const hash2 = validRows2[0].hash;

      // Assert - hashes should be different because name is not excluded
      expect(hash1).toBeDefined();
      expect(hash2).toBeDefined();
      expect(hash1).not.toBe(hash2);
    });

    it('should exclude multiple fields from hash computation', () => {
      // Arrange
      const input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'name', type: 'string', required: true },
          { name: 'timestamp', type: 'string', required: false },
          { name: 'updatedBy', type: 'string', required: false }
        ],
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-01' }, { updatedBy: 'admin' } ] }
        ]
      } satisfies Input;
      const parser = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input,
        fieldFilter: createExcludeFieldsFilter(['timestamp', 'updatedBy'])
      });

      // Act
      const validRows = parser.getValidRows();
      const hash1 = validRows[0].hash;

      // Create input with different excluded fields
      const input2 = {
        fieldDefinitions: input.fieldDefinitions,
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-02-15' }, { updatedBy: 'user123' } ] }
        ]
      } satisfies Input;
      const parser2 = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input2,
        fieldFilter: createExcludeFieldsFilter(['timestamp', 'updatedBy'])
      });
      const validRows2 = parser2.getValidRows();
      const hash2 = validRows2[0].hash;

      // Assert - hashes should be identical because both excluded fields differ
      expect(hash1).toBeDefined();
      expect(hash2).toBeDefined();
      expect(hash1).toBe(hash2);
    });

    it('should handle rows with multiple field objects in fieldValues', () => {
      // Arrange
      const input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'name', type: 'string', required: true },
          { name: 'timestamp', type: 'string', required: false }
        ],
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-01' } ] }
        ]
      } satisfies Input;
      const parser = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input,
        fieldFilter: createExcludeFieldsFilter(['timestamp'])
      });

      // Act
      const validRows = parser.getValidRows();
      const hash1 = validRows[0].hash;

      // Create input with different timestamp across multiple field objects
      const input2 = {
        fieldDefinitions: input.fieldDefinitions,
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2025-12-31' } ] }
        ]
      } satisfies Input;
      const parser2 = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input2,
        fieldFilter: createExcludeFieldsFilter(['timestamp'])
      });
      const validRows2 = parser2.getValidRows();
      const hash2 = validRows2[0].hash;

      // Assert - hashes should be identical
      expect(hash1).toBeDefined();
      expect(hash2).toBeDefined();
      expect(hash1).toBe(hash2);
    });

    it('should handle empty exclude list in fieldFilter as including all fields', () => {
      // Arrange
      const input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'name', type: 'string', required: true }
        ],
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' } ] }
        ]
      } satisfies Input;
      const parser1 = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input,
        fieldFilter: createExcludeFieldsFilter([])
      });
      const parser2 = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input
      });

      // Act
      const hash1 = parser1.getValidRows()[0].hash;
      const hash2 = parser2.getValidRows()[0].hash;

      // Assert - hashes should be identical (empty array = no exclusions)
      expect(hash1).toBeDefined();
      expect(hash2).toBeDefined();
      expect(hash1).toBe(hash2);
    });

    it('should ensure hashable objects do not contain nested hashable/hash properties', () => {
      // Arrange
      const input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'name', type: 'string', required: true },
          { name: 'timestamp', type: 'string', required: false }
        ],
        fieldSets: [
          { fieldValues: [ { id: 1 }, { name: 'Alice' }, { timestamp: '2024-01-01' } ] }
        ]
      } satisfies Input;
      const parser = new InputParser({ 
        fieldValidator: getFieldValidator, 
        _input: input,
        fieldFilter: createExcludeFieldsFilter(['timestamp'])
      });

      // Act
      const validRows = parser.getValidRows();
      const row = validRows[0];

      // Assert - the row should have hashable and hash properties
      expect(row.hashable).toBeDefined();
      expect(row.hash).toBeDefined();

      // Assert - the hashable object should NOT contain hashable or hash properties
      // (this prevents circular references)
      expect(row.hashable!.hashable).toBeUndefined();
      expect(row.hashable!.hash).toBeUndefined();

      // Assert - the hashable object should only have fieldValues
      const hashableKeys = Object.keys(row.hashable!);
      expect(hashableKeys).toEqual(['fieldValues']);

      // Assert - we can safely stringify the hashable object (no circular reference)
      expect(() => JSON.stringify(row.hashable)).not.toThrow();
    });
  });

});