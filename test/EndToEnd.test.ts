import { DataSource } from '../src/DataSource';
import { BasicPushAllOperation, BatchPushResult, BatchStatus, DataTarget, PushAllParms, PushOneParms, PushResult, SinglePushResult, Status } from '../src/DataTarget';
import { BruteForceDeltaEngine, fishOutUpdatedRecordsByPK } from '../src/DeltaByBruteForce';
import { DeltaEngine, DeltaResult, FileDeltaStorage, DatabaseDeltaStorage, FishingParms } from '../src/DeltaTypes';
import { hash } from '../src/Hash';
import { InputParser } from '../src/InputParser';
import { Field, FieldDefinition, FieldSet, FieldValidator, Input } from '../src/InputTypes';
import { InputUtilsDecorator } from '../src/InputUtils';
import { BasicFieldValidator } from '../src/InputValidation';

const previousSourceData = [
  { id: 1, fname: 'Jane', mi: 'J', lname: 'Johnson', dob: '2000-06-06' },
  { id: 2, fname: 'Kyle', mi: 'K', lname: 'King', dob: '2002-07-07' },
  { id: 3, fname: 'Alice', mi: 'A', lname: 'Anderson', dob: '1990-01-01' },
  { id: 4, fname: 'Bob', mi: 'B', lname: 'Brown', dob: '1992-02-02' },
  { id: 5, fname: 'Charlie', mi: 'C', lname: 'Clark', dob: '1994-03-03' },
  { id: 6, fname: 'Diana', mi: 'D', lname: 'Davis', dob: '1996-04-04' }
];

const newSourceData = [
  previousSourceData[2], // Alice - unchanged
  previousSourceData[3], // Bob - unchanged
  { id: 5, fname: 'Charlie (UPDATED)', mi: 'C', lname: 'Clark', dob: '1994-03-03' }, // Charlie - updated
  { id: 6, fname: 'Diana (UPDATED)', mi: 'D', lname: 'Davis', dob: '1996-04-04' }, // Diana - updated
  { id: 7, fname: 'Ethan', mi: 'E', lname: 'Edwards', dob: '1998-05-05' }, // Ethan - new
];

const clientId = 'test-client-001';

enum TestScenario {
  BRUTE_FORCE = 'brute-force',
  DATABASE_BASIC = 'database-basic',
  DATABASE_WITH_UPDATES = 'database-with-updates'
}

/**
 * Mock a data source, such as a database or API from which a new set of input data can be fetched.
 * @returns 
 */
const getMockDataSource = (): DataSource => {
  return {
    name: 'Mock Data Source',
    description: 'A data source for testing purposes',
    fetchRaw: async () => newSourceData,
    convertRawToInput: (rawData: any): Input => {
      // Simulate parsing raw data into Input format
      const converted = {
        fieldDefinitions: [
          { name: 'id', type: 'number', required: true, isPrimaryKey: true },
          { name: 'fullname', type: 'string', required: true,  },
          { name: 'dob', type: 'date', required: true }
        ],
        fieldSets: rawData.map((item: any) => ({
          fieldValues: [
            { id: item.id },
            { fullname: `${item.fname} ${item.mi} ${item.lname}` },
            { dob: new Date(item.dob).toISOString() },
          ] satisfies Array<Field>
        }))
      } satisfies Input;

      return converted;
    }
  } satisfies DataSource;
};

/**
 * Mock file-based delta storage for BRUTE_FORCE scenarios
 */
const getMockFileDeltaStorage = (): FileDeltaStorage => {
  return {
    name: 'Mock File Delta Storage',
    description: 'A file-based delta storage for testing purposes',
    fetchPreviousData: async (clientId: string): Promise<FieldSet[]> => {
      if (!clientId) {
        throw new Error('clientId is required for fetchPreviousData');
      }
      console.log(`Fetching previous input for client: ${clientId}`);

      // Use the mocked set of previous input data
      const unhashed = getMockDataSource().convertRawToInput(previousSourceData).fieldSets;

      // Apply a hash to each FieldSet using Hash.ts (simulates the fetched data as already coming with hashes)
      return unhashed.map(fs => {
        return { ...fs, hash: hash(fs) } satisfies FieldSet;
      });
    },
    updatePreviousData: async (clientId: string, newPreviousData?: FieldSet[]): Promise<any> => {
      // Simulate updating previous data
      console.log(`Updating previous data for clientId: ${clientId}, recordCount: ${newPreviousData?.length || 0}`);
      return { status: 'success', message: `Updated previous data for client: ${clientId}` };
    }
  };
};

/**
 * Mock database-based delta storage for DATABASE_* scenarios
 */
const getMockDatabaseDeltaStorage = (testScenario: string): DatabaseDeltaStorage => {
  return {
    name: 'Mock Database Delta Storage',
    description: 'A database-based delta storage for testing purposes',
    storeCurrentData: async (clientId: string, data: FieldSet[]): Promise<any> => {
      // An implementation should attempt to store at the very least, the hashes of the field sets, 
      // and the primary key field(s).
      for (const fs of data) {
        if (!fs.hash) {
          console.log(`No hash found for FieldSet: ${JSON.stringify(fs)}`);
          continue;
        }
        if ((fs.validationMessages || new Map()).size > 0) { 
          console.log(`FieldSet with hash ${fs.hash} has validation messages, skipping storage.`);
          continue;
        }
        if (!fs.fieldValues || fs.fieldValues.length === 0) {
          console.log(`No field values found for FieldSet ${JSON.stringify(fs)}, skipping storage.`);
          continue;
        }
      }
      // Some random output - could be anything.
      return { status: 'success', message: `Delta storage complete for client: ${clientId}`, recordCount: data.length };
    },
    fetchDelta: async (clientId: string): Promise<DeltaResult> => {
      if (!clientId) {
        throw new Error('clientId is required for fetchDelta');
      }
      console.log(`Fetching delta input for client: ${clientId}, scenario: ${testScenario}`);
      
      // Return a DeltaResult in which Jane and Kyle are removed, Charlie and Diana are updated, and Ethan is added.
      switch(testScenario) {
        case TestScenario.DATABASE_BASIC:
          // updated records will appear in the 'added' array for this scenario.
          const added: FieldSet[] = getMockDataSource().convertRawToInput(newSourceData.filter(record => {
            return record.id === 5 // Charlie (UPDATED - will appear as added due to hash change)
              ||   record.id === 6 // Diana (UPDATED - will appear as added due to hash change)
              ||   record.id === 7; // Ethan (ADDED)
          })).fieldSets;
          const removed: FieldSet[] = getMockDataSource().convertRawToInput(previousSourceData.filter(record => {
            return record.id === 1 // Jane (REMOVED)
              ||   record.id === 2 // Kyle (REMOVED)
              ||   record.id === 5 // Charlie (UPDATED - will appear as removed due to hash miss in new data)
              ||   record.id === 6; // Diana (UPDATED - will appear as removed due to hash miss in new data)
          })).fieldSets;
          return { added, removed } satisfies DeltaResult;

        case TestScenario.DATABASE_WITH_UPDATES:
          // updated records will appear in the 'updated' array for this scenario.
          const added2: FieldSet[] = getMockDataSource().convertRawToInput(newSourceData.filter(record => {
            return record.id === 7; // Ethan (ADDED)
          })).fieldSets;
          const updated2: FieldSet[] = getMockDataSource().convertRawToInput(newSourceData.filter(record => {
            return record.id === 5 // Charlie (UPDATED)
              ||   record.id === 6 // Diana (UPDATED)
          })).fieldSets;
          const removed2: FieldSet[] = getMockDataSource().convertRawToInput(previousSourceData.filter(record => {
            return record.id === 1 // Jane (REMOVED)
              ||   record.id === 2; // Kyle (REMOVED)
          })).fieldSets;
          return { added: added2, updated: updated2, removed: removed2 } satisfies DeltaResult;

        default:
          throw new Error(`Test scenario '${testScenario}' not implemented in mock fetchDelta.`);
      }
    },
    fetchPreviousData: async (clientId: string, limitTo?: FieldSet[]): Promise<FieldSet[]> => {
      // Mock implementation - return empty array for simplicity
      // In real scenarios, this would fetch previous data from database
      console.log(`Fetching previous data for client: ${clientId}, limitTo records: ${limitTo?.length || 'all'}`);
      return [];
    },
    updatePreviousData: async (clientId: string): Promise<any> => {
      // Simulate swapping current to previous
      console.log(`Performing updatePreviousData with clientId: ${clientId}`);
      return { status: 'success', message: `Updated previous data for client: ${clientId}` };
    }
  };
};

/**
 * Mock a target system, such as a database or API to which the computed delta can be pushed.
 * @returns 
 */
const getMockDataTarget = (): DataTarget => {
  return {
    name: 'Mock Data Target',
    description: 'A data target for testing purposes',
    pushAll: async (parms: PushAllParms): Promise<BatchPushResult> => {
      // Simulate converting Input format to raw data suitable for the target
      let pushed:number = 0;
      const failures: SinglePushResult[] = [];
      const successes: SinglePushResult[] = [];
      const singlePusher: DataTarget = {
        name: 'Inner Mock Data Target',
        description: 'An inner mock data target for testing purposes',
        async pushOne(pushOneParms: PushOneParms): Promise<SinglePushResult> {
          const { data, crud } = pushOneParms;
          console.log(`Pushing one record with CRUD operation '${crud}': ${JSON.stringify(data)}`);
          // Simulate failure for every third record
          pushed++;
          const idFld = data.fieldValues.find((fv: any) => fv.id);
          if( ! idFld) {
            throw new Error('No id field found in record being pushed');
          }
          if (pushed % 3 === 0) {
            const result = { 
              status: Status.FAILURE, 
              message: 'simulated failure', 
              timestamp: new Date(), 
              primaryKey: [ idFld ],
              crud
            } satisfies SinglePushResult;
            failures.push(result);
            return result;
          }
          const result = {
            status: Status.SUCCESS, timestamp: new Date(), primaryKey: [ idFld ], crud
          } satisfies SinglePushResult
          successes.push(result);
          return result;
        },
      }
      const pusher = BasicPushAllOperation({ all: parms, pusher: singlePusher });

      return await pusher.push();
    },
    pushOne: async (parms: PushOneParms): Promise<SinglePushResult> => {
      const { data, crud } = parms;
      console.log(`Pushing one record with CRUD operation '${crud}': ${JSON.stringify(data)}`);
      // Simulate always succeeding
      return { 
        status: Status.SUCCESS, 
        timestamp: new Date(), 
        primaryKey: [ data.fieldValues.find((fv: any) => fv.id)! ]
      };
    }
  } satisfies DataTarget;
}

describe('EndToEnd', () => {

  const getDeltaResult = async (testScenario: TestScenario): Promise<{ deltaResult: DeltaResult, pushResult: PushResult }> => {
    const { BRUTE_FORCE } = TestScenario;

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
    const currentParsedInput: Input = new InputParser({ fieldValidator, _input: currentUnparsedInput }).parse();

    // Get an instance of InputUtilsDecorator for helper methods on the parsed input
    const inputUtils = new InputUtilsDecorator(currentParsedInput);

    // Create a mock delta storage
    const deltaStorage = testScenario === BRUTE_FORCE 
      ? getMockFileDeltaStorage()
      : getMockDatabaseDeltaStorage(testScenario);

    // Create a mock data target
    const dataTarget: DataTarget = getMockDataTarget();

    let deltaEngine: DeltaEngine;
    let delta: DeltaResult;

    if(testScenario === BRUTE_FORCE) {
      // File-based storage scenario
      const fileDeltaStorage = deltaStorage as FileDeltaStorage;

      // Fetch previous input from delta storage
      const previousInput: FieldSet[] = await fileDeltaStorage.fetchPreviousData(clientId);

      // Create delta engine
      deltaEngine = new BruteForceDeltaEngine();

      // Define delta parameters
      const deltaParms = {
        data:{
          current: currentParsedInput.fieldSets,
          previous: previousInput
        },
        fishOutTheUpdates: (parms:FishingParms) => {
          return fishOutUpdatedRecordsByPK(parms, inputUtils.getPrimaryKey());
        }
      }

      // Compute delta
      delta = await deltaEngine.computeDelta(deltaParms);

      // Push delta to data target
      const pushResult = await dataTarget.pushAll!(delta as PushAllParms);

      // Log push result
      console.log('Push Result:', JSON.stringify(pushResult, null, 2));

      // Store current input as previous for next run (pass the current data as the new previous)
      await fileDeltaStorage.updatePreviousData(clientId, currentParsedInput.fieldSets);

      return { deltaResult: delta, pushResult };
    }
    else {
      // Database-based storage scenario
      const dbDeltaStorage = deltaStorage as DatabaseDeltaStorage;

      // Store current input in delta storage (becomes the new current, and the old current becomes previous)
      const primaryKeyFields = inputUtils.getPrimaryKey();
      await dbDeltaStorage.storeCurrentData(clientId, currentParsedInput.fieldSets, primaryKeyFields);

      const deltaResult = await dbDeltaStorage.fetchDelta(clientId, primaryKeyFields);
      
      if(deltaResult.updated) {
        // Updated records are already separated out
        const { added, updated, removed } = deltaResult; 
        delta = { added, updated, removed };
      }
      else {
        // Need to fish out updated records.
        const { added: newOrUpdatedRecords=[], removed: removedOrUpdatedRecords=[] } = deltaResult;
        const fishingParms: FishingParms = { newOrUpdatedRecords, removedOrUpdatedRecords };
        delta = fishOutUpdatedRecordsByPK(fishingParms, inputUtils.getPrimaryKey());
      }

      // Push delta to data target
      const pushResult = await dataTarget.pushAll!(delta as PushAllParms);

      console.log('Push Result:', JSON.stringify(pushResult, null, 2));

      // Store current input as previous for next run
      await dbDeltaStorage.updatePreviousData(clientId, []);

      return { deltaResult: delta, pushResult };
    }
  }

  const defaultDeltaResultAssertions = (deltaResult: DeltaResult) => {
    return {
      assert: () => {
        const { added, updated, removed } = deltaResult;

        /** ----------- VERIFY ADDED RECORDS ----------- */
        expect(added).toHaveLength(1);
        // expect the added record to be Ethan
        expect(added[0].fieldValues.find(fv => fv.id)?.id).toBe(7);
        expect(added[0].fieldValues.find(fv => fv.fullname)?.fullname).toBe('Ethan E Edwards');

        /** ----------- VERIFY UPDATED RECORDS ----------- */
        expect(updated).toHaveLength(2);
        // expect one of the updated records to be Charlie
        const updatedIds = updated!.map(fs => fs.fieldValues.find(fv => fv.id)?.id);
        expect(updatedIds).toContain(5);
        const charlieRecord = updated!.find(fs => fs.fieldValues.find(fv => fv.id)?.id === 5);
        expect(charlieRecord?.fieldValues.find(fv => fv.fullname)?.fullname).toBe('Charlie (UPDATED) C Clark');
        // expect one of the updated records to be Diana
        expect(updatedIds).toContain(6);
        const dianaRecord = updated!.find(fs => fs.fieldValues.find(fv => fv.id)?.id === 6);
        expect(dianaRecord?.fieldValues.find(fv => fv.fullname)?.fullname).toBe('Diana (UPDATED) D Davis');

        /** ----------- VERIFY REMOVED RECORDS ----------- */
        expect(removed).toHaveLength(2);
        // expect one of the removed records to be Jane
        const removedIds = removed.map(fs => fs.fieldValues.find(fv => fv.id)?.id);
        expect(removedIds).toContain(1);
        const janeRecord = removed.find(fs => fs.fieldValues.find(fv => fv.id)?.id === 1);
        expect(janeRecord?.fieldValues.find(fv => fv.fullname)?.fullname).toBe('Jane J Johnson');
        // expect one of the removed records to be Kyle
        expect(removedIds).toContain(2);
        const kyleRecord = removed.find(fs => fs.fieldValues.find(fv => fv.id)?.id === 2);
        expect(kyleRecord?.fieldValues.find(fv => fv.fullname)?.fullname).toBe('Kyle K King');
      }
    }
  }

  const defaultVerifyPushResult = (pushResult: PushResult) => {
    return {
      assert: () => {
        const batchPushResult = pushResult as BatchPushResult;
        expect(batchPushResult.status).toBe(BatchStatus.PARTIAL);
        expect(batchPushResult.failures).toHaveLength(1);
        expect(batchPushResult.failures![0].primaryKey[0].id).toBe(6); // Diana should have failed to push
        expect(batchPushResult.successes).toHaveLength(4); // Ethan, Charlie, Jane, Kyle should have succeeded
      }
    }
  }

  it('should compute a delta from previous and current inputs', async () => {

    const { deltaResult, pushResult } = await getDeltaResult(TestScenario.BRUTE_FORCE);

    defaultDeltaResultAssertions(deltaResult).assert();

    defaultVerifyPushResult(pushResult).assert();
  });

  it(`should "fish" out updated records from the "new" records of a delta that was computed 
    by delta storage (ie: database view)`, async () => {

    const { deltaResult, pushResult } = await getDeltaResult(TestScenario.DATABASE_BASIC);

    defaultDeltaResultAssertions(deltaResult).assert();

    defaultVerifyPushResult(pushResult).assert();
  });

  it(`should simply report output of records of a delta that was computed by delta storage that 
    includes added/updated separation (ie: database view)`, async () => {

    const { deltaResult, pushResult } = await getDeltaResult(TestScenario.DATABASE_WITH_UPDATES);

    defaultDeltaResultAssertions(deltaResult).assert();

    defaultVerifyPushResult(pushResult).assert();
  });
});