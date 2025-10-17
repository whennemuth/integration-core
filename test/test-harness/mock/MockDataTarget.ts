import { BasicPushAllOperation, BatchPushResult, DataTarget, PushAllParms, PushOneParms, SinglePushResult, Status } from "../../../src/DataTarget";
import { Field } from "../../../src/InputTypes";
import { RandomData } from "./RandomData";

export class MockDataTarget implements DataTarget {
  private firstNamesToFail: string[];

  constructor(parms: { simulatedPushFailureIndexes: number[] }) {
    this.firstNamesToFail = parms.simulatedPushFailureIndexes.map(i => `FirstName${i}`);
  }

  public get name(): string {
    return "Mock Data Target";
  }

  public get description(): string {
    return "A mock data target for pushing test data";
  }

  public pushOne = async (parms: PushOneParms): Promise<SinglePushResult> => {
    const { crud, data: { fieldValues, hash, validationMessages }} = parms;
    const { fieldDefinitions } = RandomData;

    // Get the primary key field(s)
    const primaryKey: Field[] = fieldValues.map((field:Field) => {
      const fieldDef = fieldDefinitions.find(fd => fd.name === Object.keys(field)[0]);
      if(fieldDef && fieldDef.isPrimaryKey) {
        return field;
      }
      return undefined;
    }).filter(f => f !== undefined) as Field[];
    console.log(`Mock push of record with primary key ${JSON.stringify(primaryKey)} and hash ${hash} using CRUD operation ${crud}.`);

    // Simulated push operation to a data target
    const simulatePush = async (failThisOne: boolean): Promise<SinglePushResult> => {
      if(failThisOne) {
        throw new Error(`Simulated push failure for record with primary key ${JSON.stringify(primaryKey)}`);
      }
      const status = (validationMessages && validationMessages.size > 0) ? Status.FAILURE : Status.SUCCESS;
      return { status, primaryKey, crud } satisfies SinglePushResult;
    }
    
    try {
      /**
       * Determine if this record should fail being pushed based on the specified field (probably fullname)
       * starting with one of the firstNamesToFail values
       */
      const failThisOne = fieldValues.some((fv:Field) => {
        const fldname = Object.keys(fv)[0];
        const fldValue = fv[fldname];
        if(fldname !== RandomData.fullNameFieldName) {
          return false;
        }
        return this.firstNamesToFail.find(fn => {
          return `${fldValue}`.startsWith(fn);
        })
      });
      
      return await simulatePush(failThisOne);
    }
    catch(e) {
      return { 
        status:Status.FAILURE, 
        primaryKey, 
        message: (e as Error).message, crud 
      } satisfies SinglePushResult;
    }
  }

  public pushAll = async (parms: PushAllParms): Promise<BatchPushResult> => {
    const pushResult: BatchPushResult = await BasicPushAllOperation({ all: parms, pusher: this }).push();
    console.log(`Mock pushAll result: ${JSON.stringify(pushResult, null, 2)}`);
    return pushResult;
  }
}