import { hash } from "../src/Hash";

describe('Hash', () => {

  it('should produce consistent hashes for identical FieldSet instances', () => {
    // Arrange
    // ---------------------------------------------------------------------------------
    const row1 = { fieldValues: [ { id: 1 }, { name: 'Alice' }, { email: 'alice@example.com' } ] }; 
    const row2 = { fieldValues: [ { id: 1 }, { name: 'Alice' }, { email: 'alice@example.com' } ] };
    const row3 = { fieldValues: [ { id: 2 }, { name: 'Bob' }, { email: 'bob@example.com' } ] };
    const row4 = { fieldValues: [ { email: 'bob@example.com' }, { name: 'Bob' }, { id: 2 } ] };

    // Act
    // ---------------------------------------------------------------------------------
    const hash1 = hash(row1);
    const hash2 = hash(row2);
    const hash3 = hash(row3);
    const hash4 = hash(row4);
    const hash5 = hash(row3, true); // With explicit sorting
    const hash6 = hash(row4, true); // With explicit sorting

    // Assert
    // ---------------------------------------------------------------------------------
    // Records with identical values in the same order should produce identical hashes
    expect(hash1).toBe(hash2);
    // Records with different values should produce different hashes
    expect(hash1).not.toBe(hash3);
    // Without sorting (default), hashes between records with the same fields/values but in different orders should differ
    expect(hash3).not.toBe(hash4);
    // With explicit sorting, hashes should match regardless of field order
    expect(hash5).toBe(hash6);
  });

});