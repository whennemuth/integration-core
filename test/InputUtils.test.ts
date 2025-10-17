import { InputUtilsDecorator } from '../src/InputUtils';
import { BatchPushResult, BatchStatus, CrudOperation, Status } from '../src/DataTarget';
import { FieldDefinition, FieldSet, Input } from '../src/InputTypes';

describe('InputUtilsDecorator', () => {
  let inputUtils: InputUtilsDecorator;
  let mockInput: Input;

  beforeEach(() => {
    // Setup mock input with field definitions and field sets
    mockInput = {
      fieldDefinitions: [
        { name: 'id', type: 'number', isPrimaryKey: true, required: true },
        { name: 'email', type: 'email', isPrimaryKey: true, required: true },
        { name: 'name', type: 'string', required: true },
        { name: 'age', type: 'number', required: false },
        { name: 'active', type: 'boolean', required: true }
      ],
      fieldSets: [
        {
          fieldValues: [
            { id: 1 },
            { email: 'john@example.com' },
            { name: 'John Doe' },
            { age: 30 },
            { active: true }
          ],
          hash: 'hash1'
        },
        {
          fieldValues: [
            { id: 2 },
            { email: 'jane@example.com' },
            { name: 'Jane Smith' },
            { age: 25 },
            { active: false }
          ],
          hash: 'hash2'
        },
        {
          fieldValues: [
            { id: 3 },
            { email: 'bob@example.com' },
            { name: 'Bob Wilson' },
            { age: 35 },
            { active: true }
          ],
          hash: 'hash3'
        }
      ]
    };

    inputUtils = new InputUtilsDecorator(mockInput);
  });

  describe('getPrimaryKey', () => {
    it('should return a set containing all primary key field names of a composite key', () => {
      const result = inputUtils.getPrimaryKey();
      
      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(2);
      expect(result.has('id')).toBe(true);
      expect(result.has('email')).toBe(true);
      expect(result.has('name')).toBe(false);
      expect(result.has('age')).toBe(false);
      expect(result.has('active')).toBe(false);
    });

    it('should return empty set when no primary keys are defined', () => {
      const inputWithoutPK: Input = {
        fieldDefinitions: [
          { name: 'name', type: 'string', required: true },
          { name: 'age', type: 'number', required: false }
        ],
        fieldSets: []
      };

      const utilsWithoutPK = new InputUtilsDecorator(inputWithoutPK);
      const result = utilsWithoutPK.getPrimaryKey();

      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(0);
    });

    it('should return set with single primary key when only one is defined', () => {
      const inputWithSinglePK: Input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', isPrimaryKey: true, required: true },
          { name: 'name', type: 'string', required: true }
        ],
        fieldSets: []
      };

      const utilsWithSinglePK = new InputUtilsDecorator(inputWithSinglePK);
      const result = utilsWithSinglePK.getPrimaryKey();

      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(1);
      expect(result.has('id')).toBe(true);
    });
  });

  describe('getKeyAndHashFieldSets', () => {
    it('should return field sets containing only primary key fields and hash', () => {
      const result = inputUtils.getKeyAndHashFieldSets();

      expect(result).toHaveLength(3);
      
      // Check first record
      expect(result[0].fieldValues).toHaveLength(2);
      expect(result[0].fieldValues).toContainEqual({ id: 1 });
      expect(result[0].fieldValues).toContainEqual({ email: 'john@example.com' });
      expect(result[0].hash).toBe('hash1');

      // Check second record  
      expect(result[1].fieldValues).toHaveLength(2);
      expect(result[1].fieldValues).toContainEqual({ id: 2 });
      expect(result[1].fieldValues).toContainEqual({ email: 'jane@example.com' });
      expect(result[1].hash).toBe('hash2');

      // Check third record
      expect(result[2].fieldValues).toHaveLength(2);
      expect(result[2].fieldValues).toContainEqual({ id: 3 });
      expect(result[2].fieldValues).toContainEqual({ email: 'bob@example.com' });
      expect(result[2].hash).toBe('hash3');
    });

    it('should exclude non-primary key fields from reduced field sets', () => {
      const result = inputUtils.getKeyAndHashFieldSets();

      result.forEach(fieldSet => {
        fieldSet.fieldValues.forEach(field => {
          const fieldName = Object.keys(field)[0];
          expect(['id', 'email']).toContain(fieldName);
          expect(['name', 'age', 'active']).not.toContain(fieldName);
        });
      });
    });

    it('should preserve validation messages in reduced field sets', () => {
      // Add validation messages to original field sets
      mockInput.fieldSets[0].validationMessages = new Map([['id', 'Invalid ID']]);
      mockInput.fieldSets[1].validationMessages = new Map([['email', 'Invalid email format']]);

      const result = inputUtils.getKeyAndHashFieldSets();

      expect(result[0].validationMessages).toEqual(new Map([['id', 'Invalid ID']]));
      expect(result[1].validationMessages).toEqual(new Map([['email', 'Invalid email format']]));
      expect(result[2].validationMessages).toBeUndefined();
    });

    it('should return empty array when no field sets exist', () => {
      const emptyInput: Input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', isPrimaryKey: true, required: true }
        ],
        fieldSets: []
      };

      const emptyUtils = new InputUtilsDecorator(emptyInput);
      const result = emptyUtils.getKeyAndHashFieldSets();

      expect(result).toEqual([]);
    });

    it('should handle field sets with no primary key fields', () => {
      const inputWithoutPKValues: Input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', isPrimaryKey: true, required: true }
        ],
        fieldSets: [
          {
            fieldValues: [
              { name: 'John Doe' }, // No primary key fields in the data
              { age: 30 }
            ],
            hash: 'hash1'
          }
        ]
      };

      const utilsWithoutPKValues = new InputUtilsDecorator(inputWithoutPKValues);
      const result = utilsWithoutPKValues.getKeyAndHashFieldSets();

      expect(result).toHaveLength(1);
      expect(result[0].fieldValues).toEqual([]);
      expect(result[0].hash).toBe('hash1');
    });
  });

  describe('restorePreviousHashesForFailures', () => {
    let currentKeyAndHashFieldSets: FieldSet[];
    let previousKeyAndHashFieldSets: FieldSet[];

    beforeEach(() => {
      currentKeyAndHashFieldSets = [
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          hash: 'newHash1'
        },
        {
          fieldValues: [{ id: 2 }, { email: 'jane@example.com' }],
          hash: 'newHash2'
        },
        {
          fieldValues: [{ id: 3 }, { email: 'bob@example.com' }],
          hash: 'newHash3'
        }
      ];

      previousKeyAndHashFieldSets = [
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          hash: 'oldHash1'
        },
        {
          fieldValues: [{ id: 2 }, { email: 'jane@example.com' }],
          hash: 'oldHash2'
        },
        {
          fieldValues: [{ id: 3 }, { email: 'bob@example.com' }],
          hash: 'oldHash3'
        }
      ];
    });

    it('should restore previous hashes for failed records', () => {
      const pushResult: BatchPushResult = {
        status: BatchStatus.PARTIAL,
        failures: [
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 1 }, { email: 'john@example.com' }],
            crud: CrudOperation.UPDATE,
            message: 'Update failed'
          },
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 3 }, { email: 'bob@example.com' }],
            crud: CrudOperation.CREATE,
            message: 'Create failed'
          }
        ],
        successes: [
          {
            status: Status.SUCCESS,
            primaryKey: [{ id: 2 }, { email: 'jane@example.com' }],
            crud: CrudOperation.UPDATE
          }
        ]
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // Failed records should have their previous hashes restored
      expect(currentKeyAndHashFieldSets[0].hash).toBe('oldHash1'); // id: 1 failed
      expect(currentKeyAndHashFieldSets[1].hash).toBe('newHash2'); // id: 2 succeeded, keeps new hash
      expect(currentKeyAndHashFieldSets[2].hash).toBe('oldHash3'); // id: 3 failed
    });

    it('should do nothing when there are no failures', () => {
      const pushResult: BatchPushResult = {
        status: BatchStatus.SUCCESS,
        failures: [],
        successes: [
          {
            status: Status.SUCCESS,
            primaryKey: [{ id: 1 }, { email: 'john@example.com' }],
            crud: CrudOperation.UPDATE
          }
        ]
      };

      const originalHashes = currentKeyAndHashFieldSets.map(fs => fs.hash);

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // All hashes should remain unchanged
      currentKeyAndHashFieldSets.forEach((fs, index) => {
        expect(fs.hash).toBe(originalHashes[index]);
      });
    });

    it('should handle empty or undefined failures array', () => {
      const pushResult: BatchPushResult = {
        status: BatchStatus.SUCCESS,
        failures: undefined as any,
        successes: []
      };

      const originalHashes = currentKeyAndHashFieldSets.map(fs => fs.hash);

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // All hashes should remain unchanged
      currentKeyAndHashFieldSets.forEach((fs, index) => {
        expect(fs.hash).toBe(originalHashes[index]);
      });
    });

    it('should handle failures for records not found in previous input', () => {
      const pushResult: BatchPushResult = {
        status: BatchStatus.PARTIAL,
        failures: [
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 999 }, { email: 'nonexistent@example.com' }],
            crud: CrudOperation.CREATE,
            message: 'Create failed'
          }
        ],
        successes: []
      };

      const originalHashes = currentKeyAndHashFieldSets.map(fs => fs.hash);

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // All hashes should remain unchanged
      currentKeyAndHashFieldSets.forEach((fs, index) => {
        expect(fs.hash).toBe(originalHashes[index]);
      });
    });

    it('should handle push failures for records not found in previous input', () => {
      const currentWithNewFailedRecord = [
        {
          fieldValues: [{ id: 999 }, { email: 'new-failed-record@example.com' }],
          hash: 'newHash999' // New record that failed to push
        },
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          hash: 'newHash1'
        }
      ];

      const pushResult: BatchPushResult = {
        status: BatchStatus.PARTIAL,
        failures: [
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 999 }, { email: 'new-failed-record@example.com' }],
            crud: CrudOperation.CREATE,
            message: 'Create failed'
          }
        ],
        successes: []
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: currentWithNewFailedRecord,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // New failed record should be removed from the set (no previous record to restore from)
      expect(currentWithNewFailedRecord).toHaveLength(1);
      expect(currentWithNewFailedRecord[0].hash).toBe('newHash1');
    });

    it('should handle failures for records not found in current input', () => {
      const pushResult: BatchPushResult = {
        status: BatchStatus.PARTIAL,
        failures: [
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 1 }, { email: 'john@example.com' }],
            crud: CrudOperation.DELETE,
            message: 'Delete failed'
          }
        ],
        successes: []
      };

      // Remove the first record from current input to simulate it not being found
      const reducedCurrentInput = currentKeyAndHashFieldSets.slice(1);
      const originalHashes = reducedCurrentInput.map(fs => fs.hash);

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: reducedCurrentInput,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // Remaining hashes should be unchanged since the failed record isn't in current input
      reducedCurrentInput.forEach((fs, index) => {
        expect(fs.hash).toBe(originalHashes[index]);
      });
    });

    it('should handle composite primary keys correctly', () => {
      // Test with our existing composite key setup (id + email)
      const pushResult: BatchPushResult = {
        status: BatchStatus.PARTIAL,
        failures: [
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 2 }, { email: 'jane@example.com' }],
            crud: CrudOperation.UPDATE,
            message: 'Update failed'
          }
        ],
        successes: []
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // Only the record with matching composite key should be updated
      expect(currentKeyAndHashFieldSets[0].hash).toBe('newHash1'); // Different record
      expect(currentKeyAndHashFieldSets[1].hash).toBe('oldHash2'); // Matching composite key - restored
      expect(currentKeyAndHashFieldSets[2].hash).toBe('newHash3'); // Different record
    });

    it('should handle single primary key fields', () => {
      // Create InputUtils with single primary key for this test
      const singlePKInput: Input = {
        fieldDefinitions: [
          { name: 'id', type: 'number', isPrimaryKey: true, required: true },
          { name: 'name', type: 'string', required: true }
        ],
        fieldSets: []
      };

      const singlePKUtils = new InputUtilsDecorator(singlePKInput);

      const singlePKCurrent = [
        { fieldValues: [{ id: 1 }], hash: 'newHash1' },
        { fieldValues: [{ id: 2 }], hash: 'newHash2' }
      ];

      const singlePKPrevious = [
        { fieldValues: [{ id: 1 }], hash: 'oldHash1' },
        { fieldValues: [{ id: 2 }], hash: 'oldHash2' }
      ];

      const pushResult: BatchPushResult = {
        status: BatchStatus.PARTIAL,
        failures: [
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 1 }],
            crud: CrudOperation.UPDATE,
            message: 'Update failed'
          }
        ],
        successes: []
      };

      singlePKUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: singlePKCurrent,
        previousKeyAndHashFieldSets: singlePKPrevious,
        pushResult
      });

      expect(singlePKCurrent[0].hash).toBe('oldHash1'); // Failed record restored
      expect(singlePKCurrent[1].hash).toBe('newHash2'); // Successful record unchanged
    });

    it('should restore previous hashes for records with validation issues (missing hashes)', () => {
      const currentWithValidationIssues = [
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          hash: 'newHash1' // Valid record with hash
        },
        {
          fieldValues: [{ id: 2 }, { email: 'jane@example.com' }],
          validationMessages: new Map([['email', 'Invalid email format']]),
          // Missing hash due to validation failure
        },
        {
          fieldValues: [{ id: 3 }, { email: 'bob@example.com' }],
          hash: 'newHash3' // Valid record with hash
        }
      ];

      const pushResult: BatchPushResult = {
        status: BatchStatus.SUCCESS,
        failures: [],
        successes: []
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: currentWithValidationIssues,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // Valid records should keep their new hashes
      expect(currentWithValidationIssues[0].hash).toBe('newHash1');
      expect(currentWithValidationIssues[2].hash).toBe('newHash3');
      
      // Invalid record should have previous hash restored
      expect(currentWithValidationIssues[1].hash).toBe('oldHash2');
    });

    it('should restore hashes for both push failures and validation issues', () => {
      const currentWithMixedIssues = [
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          hash: 'newHash1' // Will be a push failure
        },
        {
          fieldValues: [{ id: 2 }, { email: 'jane@example.com' }],
          validationMessages: new Map([['email', 'Invalid email format']]),
          // Missing hash due to validation failure
        },
        {
          fieldValues: [{ id: 3 }, { email: 'bob@example.com' }],
          hash: 'newHash3' // Valid and successful
        }
      ];

      const pushResult: BatchPushResult = {
        status: BatchStatus.PARTIAL,
        failures: [
          {
            status: Status.FAILURE,
            primaryKey: [{ id: 1 }, { email: 'john@example.com' }],
            crud: CrudOperation.UPDATE,
            message: 'Update failed'
          }
        ],
        successes: [
          {
            status: Status.SUCCESS,
            primaryKey: [{ id: 3 }, { email: 'bob@example.com' }],
            crud: CrudOperation.UPDATE
          }
        ]
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: currentWithMixedIssues,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // Push failure should restore hash
      expect(currentWithMixedIssues[0].hash).toBe('oldHash1');
      
      // Validation failure should restore hash
      expect(currentWithMixedIssues[1].hash).toBe('oldHash2');
      
      // Successful record should keep new hash
      expect(currentWithMixedIssues[2].hash).toBe('newHash3');
    });

    it('should not restore hashes for records with validation messages that already have hashes', () => {
      const currentWithHashedValidationIssues = [
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          hash: 'newHash1',
          validationMessages: new Map([['name', 'Warning: name is suspicious']]) // Has hash despite validation message
        },
        {
          fieldValues: [{ id: 2 }, { email: 'jane@example.com' }],
          hash: 'newHash2'
        }
      ];

      const pushResult: BatchPushResult = {
        status: BatchStatus.SUCCESS,
        failures: [],
        successes: []
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: currentWithHashedValidationIssues,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // Records with existing hashes should not be modified, even with validation messages
      expect(currentWithHashedValidationIssues[0].hash).toBe('newHash1');
      expect(currentWithHashedValidationIssues[1].hash).toBe('newHash2');
    });

    it('should handle records with validation issues not found in previous input', () => {
      const currentWithNewInvalidRecord = [
        {
          fieldValues: [{ id: 999 }, { email: 'new-invalid-email' }],
          validationMessages: new Map([['email', 'Invalid email format']]),
          // Missing hash and not in previous input
        },
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          hash: 'newHash1'
        }
      ];

      const pushResult: BatchPushResult = {
        status: BatchStatus.SUCCESS,
        failures: [],
        successes: []
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: currentWithNewInvalidRecord,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // New invalid record should be removed from the set (no previous record to restore from)
      expect(currentWithNewInvalidRecord).toHaveLength(1);
      expect(currentWithNewInvalidRecord[0].hash).toBe('newHash1');
    });

    it('should handle empty validation messages map correctly', () => {
      const currentWithEmptyValidationMessages = [
        {
          fieldValues: [{ id: 1 }, { email: 'john@example.com' }],
          validationMessages: new Map(), // Empty map should not trigger restoration
          // Missing hash but empty validation messages
        },
        {
          fieldValues: [{ id: 2 }, { email: 'jane@example.com' }],
          hash: 'newHash2'
        }
      ];

      const pushResult: BatchPushResult = {
        status: BatchStatus.SUCCESS,
        failures: [],
        successes: []
      };

      inputUtils.restorePreviousHashesForFailures({
        currentKeyAndHashFieldSets: currentWithEmptyValidationMessages,
        previousKeyAndHashFieldSets,
        pushResult
      });

      // Empty validation messages should not trigger hash restoration
      expect(currentWithEmptyValidationMessages[0].hash).toBeUndefined();
      expect(currentWithEmptyValidationMessages[1].hash).toBe('newHash2');
    });
  });

  describe('Input interface implementation', () => {
    it('should expose fieldDefinitions from wrapped input', () => {
      expect(inputUtils.fieldDefinitions).toBe(mockInput.fieldDefinitions);
      expect(inputUtils.fieldDefinitions).toHaveLength(5);
    });

    it('should expose fieldSets from wrapped input', () => {
      expect(inputUtils.fieldSets).toBe(mockInput.fieldSets);
      expect(inputUtils.fieldSets).toHaveLength(3);
    });
  });
});