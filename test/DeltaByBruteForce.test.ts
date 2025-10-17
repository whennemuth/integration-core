import { DeltaParms, FishingParms } from "../src/DeltaTypes";
import { BruteForceDeltaEngine, fishOutUpdatedRecordsByPK } from "../src/delta-strategy/DeltaByBruteForce";
import { FieldSet } from "../src/InputTypes";

describe('DeltaByBruteForce', () => {

  it('should compute the delta between two data sets with single field primary key for determining updated fields', 
    async() => {
    // Arrange
    const singleFieldPK = new Set<string>(["id"]);
    const engine = new BruteForceDeltaEngine();
    const deltaParms = {
      fishOutTheUpdates: (parms:FishingParms) => {
        const { newOrUpdatedRecords, removedOrUpdatedRecords } = parms;
        return fishOutUpdatedRecordsByPK({ newOrUpdatedRecords, removedOrUpdatedRecords }, singleFieldPK);
      },
      data: {
        previous: [
          { hash: 'hash1', fieldValues: [ { id: 1 }, { name: 'Alice' } ] },
          { hash: 'hash2', fieldValues: [ { id: 2 }, { name: 'Bob' } ] },
          { hash: 'hash3', fieldValues: [ { id: 3 }, { name: 'Charlie' } ] }
        ] satisfies FieldSet[],
        current: [
          { hash: 'hash2', fieldValues: [ { id: 2 }, { name: 'Bob' } ] },
          { hash: 'hash4', fieldValues: [ { id: 3 }, { name: 'Charlie Updated' } ] },
          { hash: 'hash5', fieldValues: [ { id: 4 }, { name: 'Diana' } ] },
          { hash: 'hash6', fieldValues: [ { id: 5 }, { name: 'Jane' } ] }
        ] satisfies FieldSet[]
      }
    } satisfies DeltaParms;
  
    // Act
    const { added: newRecords, updated: changedRecords, removed: removedRecords } = 
      await engine.computeDelta(deltaParms);

    // Assert    
    expect(newRecords).toHaveLength(2);
    expect(newRecords.map(r => r.fieldValues.find(fv => 'id' in fv)!['id'])).toEqual(expect.arrayContaining([4, 5]));

    expect(changedRecords).toHaveLength(1);
    expect(changedRecords![0].fieldValues.find(fv => 'id' in fv)!['id']).toBe(3);

    expect(removedRecords).toHaveLength(1);
    expect(removedRecords[0].fieldValues.find(fv => 'id' in fv)!['id']).toBe(1);
  });

  it('should compute the delta between two data sets with composite primary key for determining updated fields', 
    async() => {
    // Arrange
    const compositePK = new Set<string>(['id', 'username']);
    const engine = new BruteForceDeltaEngine();
    const deltaParms = {
      fishOutTheUpdates: (parms:FishingParms) => {
        const { newOrUpdatedRecords, removedOrUpdatedRecords } = parms;
        return fishOutUpdatedRecordsByPK({ newOrUpdatedRecords, removedOrUpdatedRecords }, compositePK);
      },
      data: {
        previous: [
          { hash: 'hash0', fieldValues: [ { id: 6 }, { username: 'jane' }, { name: 'Jane' } ] },
          { hash: 'hash1', fieldValues: [ { id: 1 }, { username: 'alice' }, { name: 'Alice' } ] },
          { hash: 'hash2', fieldValues: [ { id: 2 }, { username: 'bob' }, { name: 'Bob' } ] },
          { hash: 'hash3', fieldValues: [ { id: 3 }, { username: 'charlie' }, { name: 'Charlie' } ] }
        ] satisfies FieldSet[],
        current: [
          { hash: 'hash2', fieldValues: [ { id: 2 }, { username: 'bob' }, { name: 'Bob' } ] },
          { hash: 'hash4', fieldValues: [ { id: 3 }, { username: 'charlie' }, { name: 'Charlie Updated' } ] },
          { hash: 'hash5', fieldValues: [ { id: 4 }, { username: 'diana' }, { name: 'Diana' } ] },
          { hash: 'hash6', fieldValues: [ { id: 5 }, { username: 'jane' }, { name: 'Jane' } ] },
          { hash: 'hash7', fieldValues: [ { id: 6 }, { username: 'bob' }, { name: 'Bobby' } ] }
        ] satisfies FieldSet[]
      }
    } satisfies DeltaParms;
  
    // Act
    const { added: newRecords, updated: changedRecords, removed: removedRecords } = 
      await engine.computeDelta(deltaParms);

    // Assert    
    expect(newRecords).toHaveLength(3);
    expect(newRecords.map(r => r.fieldValues.find(fv => 'id' in fv)!['id'])).toEqual(expect.arrayContaining([4, 5, 6]));
    expect(newRecords.map(r => r.fieldValues.find(fv => 'username' in fv)!['username'])).toEqual(expect.arrayContaining(['diana', 'jane', 'bob']));
    expect(newRecords.map(r => r.fieldValues.find(fv => 'name' in fv)!['name'])).toEqual(expect.arrayContaining(['Diana', 'Jane', 'Bobby']));

    expect(changedRecords).toHaveLength(1);
    expect(changedRecords![0].fieldValues.find(fv => 'id' in fv)!['id']).toBe(3);
    expect(changedRecords![0].fieldValues.find(fv => 'username' in fv)!['username']).toBe('charlie');
    expect(changedRecords![0].fieldValues.find(fv => 'name' in fv)!['name']).toBe('Charlie Updated');

    expect(removedRecords).toHaveLength(2);
    expect(removedRecords.map(r => r.fieldValues.find(fv => 'id' in fv)!['id'])).toEqual(expect.arrayContaining([1, 6]));
    expect(removedRecords.map(r => r.fieldValues.find(fv => 'username' in fv)!['username'])).toEqual(expect.arrayContaining(['alice', 'jane']));
    expect(removedRecords.map(r => r.fieldValues.find(fv => 'name' in fv)!['name'])).toEqual(expect.arrayContaining(['Alice', 'Jane']));
  });
});