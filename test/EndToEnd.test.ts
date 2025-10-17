import { DataSource } from '../src/DataSource';
import { BasicPushAllOperation, DataTarget, PushAllParms, PushOneParms } from '../src/DataTarget';
import { DeltaEngine, DeltaResult, DeltaStorage } from '../src/DeltaTypes';
import { BruteForceDeltaEngine, fishOutUpdatedRecordsByPK } from '../src/DeltaByBruteForce';
import { InputParser } from '../src/InputParser';
import { Field, FieldDefinition, FieldSet, FieldValidator, Input } from '../src/InputTypes';
import { BasicFieldValidator } from '../src/InputValidation';

const rawSourceData = [
  { id: 1, fname: 'Alice', mi: 'A', lname: 'Anderson', dob: '1990-01-01' },
  { id: 2, fname: 'Bob', mi: 'B', lname: 'Brown', dob: '1992-02-02' },
  { id: 3, fname: 'Charlie', mi: 'C', lname: 'Clark', dob: '1994-03-03' },
  { id: 4, fname: 'Diana', mi: 'D', lname: 'Davis', dob: '1996-04-04' },
  { id: 5, fname: 'Ethan', mi: 'E', lname: 'Edwards', dob: '1998-05-05' },
];

const getMockDataSource = (): DataSource => {
  return {
    name: 'Mock Data Source',
    description: 'A data source for testing purposes',
    fetchRaw: async () => rawSourceData,
    convertRawToInput: (rawData: any): Input => {
      // Simulate parsing raw data into Input format
      const converted = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true },
          { name: 'fullname', type: 'string', required: true },
          { name: 'dob', type: 'date', required: true }
        ],
        fieldSets: rawData.map((item: any) => ({
          fieldValues: {
            id: item.id,
            fullname: `${item.fname} ${item.mi} ${item.lname}`,
            dob: new Date(item.dob).toISOString(),
          }
        }))
      } satisfies Input;

      return converted;
    }
  } satisfies DataSource;
};

const getMockDeltaStorage = (): DeltaStorage => {
  return {
    name: 'Mock Delta Storage',
    description: 'A delta storage for testing purposes',
    fetchPreviousInput: async (): Promise<FieldSet[]> => {
      // Simulate fetching previous field sets
      return [
        { fieldValues: [{ id: 1 }, { fullname: 'Alice A Anderson' }, { dob: '1990-01-01T00:00:00.000Z' }] },
        { fieldValues: [{ id: 2 }, { fullname: 'Bob B Brown' }, { dob: '1992-02-02T00:00:00.000Z' }] }
      ] satisfies FieldSet[];
    },
    storeCurrentInput: async (data: FieldSet[]): Promise<any> => {
      // An implementation should attempt to store at the very least, the hashes of the field sets, 
      // and the primary key field(s).
      for (const fs of data) {
        if ( ! fs.hash) {
          console.log(`No hash found for FieldSet: ${JSON.stringify(fs)}`);
          continue;
        }
        if ( ! ((fs.validationMessages || new Map()).size === 0)) { 
          console.log(`FieldSet with hash ${fs.hash} has validation messages, skipping storage.`);
          continue;
        }
        if ( ! fs.fieldValues || fs.fieldValues.length === 0) {
          console.log(`No field values found for FieldSet ${JSON.stringify(fs)}, skipping storage.`);
          continue;
        }
      }
      // Some random output - could be anything.
      return { status: 'success', recordCount: data.length };
    },
    rotate: async (key1: string, key2?: string): Promise<void> => {
      // Simulate swapping inputs
      console.log(`Swapping inputs for keys: ${key1} and ${key2 || key1}`);
      // Implementation for swapping inputs goes here
    },
    fetchDeltaInput: async (): Promise<DeltaResult> => {
      // Simulate delta storage itself returning the delta result
      const added: FieldSet[] = [
        { fieldValues: [{ id: 1 }, { fullname: 'Alice A Anderson' }, { dob: '1990-01-01T00:00:00.000Z' }] },
        { fieldValues: [{ id: 2 }, { fullname: 'Bob B Brown' }, { dob: '1992-02-02T00:00:00.000Z' }] },
        { fieldValues: [{ id: 3 }, { fullname: 'Charlie C Clark' }, { dob: '1994-03-03T00:00:00.000Z' }] }
      ] satisfies FieldSet[];
      return { added, removed: [] } satisfies DeltaResult;
      // return { added, updated: [], removed: [] } satisfies DeltaResult;
    }
  }
};

const getMockDataTarget = (): DataTarget => {
  return {
    name: 'Mock Data Target',
    description: 'A data target for testing purposes',
    pushAll: async (parms: PushAllParms): Promise<any> => {
      // Simulate converting Input format to raw data suitable for the target
      let pushed:number = 0;
      const succeededIds: number[] = [];
      const failedIds: number[] = [];
      const pusher = BasicPushAllOperation({ all: parms, pusher: (pushOneParms: PushOneParms): any => {
        const { data, crud } = pushOneParms;
        console.log(`Pushing one record with CRUD operation '${crud}': ${JSON.stringify(data)}`);
        // Simulate failure for every third record
        pushed++;
        const idFld = data.fieldValues.find((fv: any) => fv.id);
        if (pushed % 3 === 0) {
          if (idFld) failedIds.push(idFld.id as number);
          return { status: 'failed', reason: 'simulated failure' };
        }
        if (idFld) succeededIds.push(idFld.id as number);
        return { status: 'completed' };
      }});

      await pusher.push();

      return { status: 'completed', succeeded: succeededIds, failed: failedIds };
    },
    pushOne: async (parms: PushOneParms) => {
      const { data, crud } = parms;
      console.log(`Pushing one record with CRUD operation '${crud}': ${JSON.stringify(data)}`);
      // Simulate always succeeding
      return { status: 'success' };
      // return { status: 'failed', reason: 'network error' };
    }
  }
}

describe('EndToEnd', () => {

  const getDeltaResult = async (bruteForce: boolean): Promise<DeltaResult> => {

    // Create a mock data source
    const dataSource: DataSource = getMockDataSource();

    // Fetch raw data from the data source
    const rawData: any = await dataSource.fetchRaw();

    // Create field validator factory function
    const fieldValidator = (fieldDef: FieldDefinition, field: Field): FieldValidator => 
      BasicFieldValidator.getInstance(fieldDef, field);

    // Convert raw data to unparsed Input format
    const currentUnparsedInput: Input = dataSource.convertRawToInput(rawData);

    // Parse the Input to validate and hash records
    const currentInput: Input = new InputParser({ fieldValidator, _input: currentUnparsedInput }).parse();

    // Create a mock delta storage
    const deltaStorage: DeltaStorage = getMockDeltaStorage();

    // Create a mock data target
    const dataTarget: DataTarget = getMockDataTarget();

    let deltaEngine: DeltaEngine;
    let delta: DeltaResult;
    let rotateTask: 'swap' | 'replace' = 'swap';

    if(bruteForce) {
      // Fetch previous input from delta storage
      const previousInput: FieldSet[] = await deltaStorage.fetchPreviousInput!();

      // Create delta engine
      deltaEngine = new BruteForceDeltaEngine();

      // Define delta parameters
      const deltaParms = {
        data:{
          current: currentInput.fieldSets,
          previous: previousInput
        },
        fishOutTheUpdates: (current: FieldSet[], previous: FieldSet[]) => {
          const primaryKey = new Set<string>(['id']);
          return fishOutUpdatedRecordsByPK(current, previous, primaryKey);
        }
      }

      // Compute delta
      delta = await deltaEngine.computeDelta(deltaParms);

      // After computing delta via brute force, we need to replace prior input with current input (not swap)
      rotateTask = 'replace';
    }
    else {
      // Store current input in delta storage (becomes the new current, and the old current becomes previous)
      await deltaStorage.storeCurrentInput(currentInput.fieldSets);

      const deltaResult = await deltaStorage.fetchDeltaInput!();
      
      const { added, updated, removed } = deltaResult;

      if(updated) {
        delta = { added, updated, removed };
      }
      else {
        delta = fishOutUpdatedRecordsByPK(added, removed || [], new Set<string>(['id']));
      }
    }

    // Push delta to data target
    const pushResult = await dataTarget.pushAll!(delta as PushAllParms);

    console.log('Push Result:', JSON.stringify(pushResult, null, 2));

    // Store current input as previous for next run
    await deltaStorage.rotate(rotateTask);

    return delta;
  }

  it.skip('should compute a delta from previous and current inputs', async () => {

    const deltaResult = await getDeltaResult(true);

    const { added, updated, removed } = deltaResult;

    // Assert (TODO: These have not been finalized yet - they are just placeholders)
    expect(added).toHaveLength(2);
    expect(updated).toHaveLength(1);
    expect(removed).toHaveLength(1);
  });

  it.skip('should separate out updated records from the "new" records of a delta that has computed by delta storage (ie: database view)', async () => {

    const deltaResult = await getDeltaResult(false);

    const { added, updated, removed } = deltaResult;

    // Assert (TODO: These have not been finalized yet - they are just placeholders)
    expect(added).toHaveLength(2);
    expect(updated).toHaveLength(1);
    expect(removed).toHaveLength(1);
  });
});