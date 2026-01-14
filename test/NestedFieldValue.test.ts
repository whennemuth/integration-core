import { hash } from "../src/Hash";
import { BasicFieldValidator, RowValidator } from "../src/InputValidation";
import { Field, FieldDefinition, FieldSet, FieldValidator, FieldValue } from "../src/InputTypes";

const clone = <T>(obj: T): T => {
  return JSON.parse(JSON.stringify(obj)) as T;
}

const getFieldValidator = (fieldDef: FieldDefinition, field: Field): FieldValidator => {
  return new BasicFieldValidator(fieldDef, field);
}

describe('Nested FieldValue Support', () => {

  describe('Hash Function with Nested Values', () => {
    it('should produce consistent hashes for identical nested structures', () => {
      // Arrange
      const nestedFieldSet1: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            address: {
              street: '123 Main St',
              city: 'Boston',
              coordinates: [42.3601, -71.0589]
            } as FieldValue
          }
        ]
      };
      
      const nestedFieldSet2: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            address: {
              street: '123 Main St',
              city: 'Boston',
              coordinates: [42.3601, -71.0589]
            } as FieldValue
          }
        ]
      };

      // Act
      const hash1 = hash(nestedFieldSet1);
      const hash2 = hash(nestedFieldSet2);

      // Assert
      expect(hash1).toBe(hash2);
      expect(hash1).toBeDefined();
      expect(typeof hash1).toBe('string');
    });

    it('should produce different hashes for different nested structures', () => {
      // Arrange
      const nestedFieldSet1: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            address: {
              street: '123 Main St',
              city: 'Boston'
            } as FieldValue
          }
        ]
      };
      
      const nestedFieldSet2: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            address: {
              street: '456 Oak Ave',
              city: 'Boston'
            } as FieldValue
          }
        ]
      };

      // Act
      const hash1 = hash(nestedFieldSet1);
      const hash2 = hash(nestedFieldSet2);

      // Assert
      expect(hash1).not.toBe(hash2);
    });

    it('should handle deeply nested arrays and objects', () => {
      // Arrange
      const deeplyNested: FieldSet = {
        fieldValues: [
          { id: 1 },
          {
            metadata: {
              tags: ['important', 'urgent'],
              properties: {
                priority: 'high',
                assignees: [
                  { name: 'Alice', role: 'developer' } as FieldValue,
                  { name: 'Bob', role: 'tester' } as FieldValue
                ],
                settings: {
                  notifications: true,
                  reminders: [
                    { type: 'email', interval: 24 } as FieldValue,
                    { type: 'slack', interval: 1 } as FieldValue
                  ]
                } as FieldValue
              } as FieldValue
            } as FieldValue
          }
        ]
      };

      // Act & Assert - Should not throw and should produce valid hash
      const result = hash(deeplyNested);
      expect(result).toBeDefined();
      expect(typeof result).toBe('string');
      expect(result.length).toBeGreaterThan(0);
    });

    it('should handle circular reference protection', () => {
      // This test ensures we don't get infinite loops - we'll create something that exceeds maxDepth=10
      const fieldSetWithMaxDepth: FieldSet = {
        fieldValues: [
          { id: 1 },
          {
            // Create a structure that would exceed max depth (deeper than 10)
            level1: {
              level2: {
                level3: {
                  level4: {
                    level5: {
                      level6: {
                        level7: {
                          level8: {
                            level9: {
                              level10: {
                                level11: {
                                  level12: 'definitely too deep'
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          } as any // Use any to bypass TypeScript checking for this test
        ]
      };

      // Act & Assert
      expect(() => hash(fieldSetWithMaxDepth)).toThrow('Maximum recursion depth');
    });
  });

  describe('Validation with Nested Values', () => {
    it('should validate nested object structures', () => {
      // Arrange
      const fieldDefinitions: FieldDefinition[] = [
        { name: 'id', type: 'number', required: true, isPrimaryKey: true },
        { name: 'profile', type: 'string', required: false } // Use non-strict type for nested data
      ];

      const validRow: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            profile: {
              personal: {
                firstName: 'John',
                lastName: 'Doe',
                age: 30
              },
              contact: {
                email: 'john@example.com',
                phones: ['555-1234', '555-5678']
              }
            } as FieldValue
          }
        ]
      };

      const rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, validRow);

      // Act
      const isValid = rowValidator.isValid();

      // Assert
      expect(isValid).toBe(true);
      expect(rowValidator.validationMessages.size).toBe(0);
    });

    it('should validate nested arrays with mixed types', () => {
      // Arrange
      const fieldDefinitions: FieldDefinition[] = [
        { name: 'id', type: 'number', required: true, isPrimaryKey: true },
        { name: 'items', type: 'string', required: false }
      ];

      const validRow: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            items: [
              'simple string',
              42,
              true,
              {
                type: 'complex',
                data: ['nested', 'array'],
                metadata: {
                  created: '2024-01-01',
                  tags: ['tag1', 'tag2']
                }
              }
            ] as FieldValue
          }
        ]
      };

      const rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, validRow);

      // Act
      const isValid = rowValidator.isValid();

      // Assert
      expect(isValid).toBe(true);
    });

    it('should prevent infinite recursion during validation', () => {
      // Arrange
      const fieldDefinitions: FieldDefinition[] = [
        { name: 'id', type: 'number', required: true, isPrimaryKey: true },
        { name: 'nested', type: 'string', required: false }
      ];

      const tooDeepValue = {
        level1: {
          level2: {
            level3: {
              level4: {
                level5: {
                  level6: {
                    level7: {
                      level8: {
                        level9: {
                          level10: {
                            level11: 'too deep'
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      } as any;

      const tooDeepRow: FieldSet = {
        fieldValues: [
          { id: 1 },
          { nested: tooDeepValue }
        ]
      };

      const rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, tooDeepRow);

      // Act
      const isValid = rowValidator.isValid();

      // Assert
      expect(isValid).toBe(false);
      expect(rowValidator.validationMessages.get('nested')).toContain('Maximum nesting depth');
    });

    it('should handle empty nested structures', () => {
      // Arrange
      const fieldDefinitions: FieldDefinition[] = [
        { name: 'id', type: 'number', required: true, isPrimaryKey: true },
        { name: 'data', type: 'string', required: false }
      ];

      const rowWithEmptyNesting: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            data: {
              emptyObject: {},
              emptyArray: [],
              nullValue: null,
              undefinedValue: undefined
            } as any // Bypass TypeScript checking for this test case
          }
        ]
      };

      const rowValidator = new RowValidator(getFieldValidator, fieldDefinitions, rowWithEmptyNesting);

      // Act
      const isValid = rowValidator.isValid();

      // Assert
      expect(isValid).toBe(true);
    });
  });

  describe('Edge Cases and Error Handling', () => {
    it('should handle null and undefined at various nesting levels', () => {
      // Arrange
      const fieldSet: FieldSet = {
        fieldValues: [
          { id: 1 },
          { 
            data: {
              validField: 'hello',
              nullField: null,
              undefinedField: undefined,
              arrayWithNulls: ['item1', null, undefined, 'item2'],
              nestedWithNulls: {
                innerField: 'value',
                innerNull: null
              }
            } as any // Bypass TypeScript checking for this test case
          }
        ]
      };

      // Act & Assert - Should not throw
      const result = hash(fieldSet);
      expect(result).toBeDefined();
    });

    it('should maintain hash consistency regardless of object property order', () => {
      // Arrange
      const fieldSet1: FieldSet = {
        fieldValues: [
          { 
            data: {
              a: 'value1',
              b: 'value2',
              c: { x: 1, y: 2 }
            } as FieldValue
          }
        ]
      };

      const fieldSet2: FieldSet = {
        fieldValues: [
          { 
            data: {
              c: { y: 2, x: 1 } as FieldValue, // Different property order
              b: 'value2',
              a: 'value1'
            } as FieldValue
          }
        ]
      };

      // Act
      const hash1 = hash(fieldSet1);
      const hash2 = hash(fieldSet2);

      // Assert
      expect(hash1).toBe(hash2);
    });
  });
});