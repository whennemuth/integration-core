import { Field, FieldSet } from "./InputTypes";

/**
 * Represents a target where delta results can be pushed to, like an api endpoint, 
 * database, ftp drop point, etc.
 */

/**
 * Parameters that separate into type - add, update or delete - for bulk push operations to a data target.
 */
export type PushAllParms = {
  added: FieldSet[];
  updated: FieldSet[];
  removed: FieldSet[];
}

/** What kind of CRUD operation is being performed by the push */
export enum CrudOperation {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete'
}

/** A single record push operation's parameters */
export type PushOneParms = {
  data: FieldSet;
  crud: CrudOperation;
}

export enum Status {
  SUCCESS = 'success',
  FAILURE = 'failure'
}

export enum BatchStatus {
  SUCCESS = Status.SUCCESS,
  FAILURE = Status.FAILURE,
  PARTIAL = 'partial'
}

/** Base result type - what was the result of a push to the data target */ 
export type BasePushResult = {
  message?: string;
  timestamp?: Date;
}

/** Single record push result */ 
export type SinglePushResult = BasePushResult & {
  status: Status;
  primaryKey: Field[]; // Support composite keys
  crud?: CrudOperation;
}

/** Batch operation push result */
export type BatchPushResult = BasePushResult & {
  status: BatchStatus;
  successes?: SinglePushResult[];
  failures: SinglePushResult[];
}

/** Union type for flexibility */ 
export type PushResult = SinglePushResult | BatchPushResult;

/**
 * Represents a target where delta results can be pushed to, like an api endpoint, 
 * database, ftp drop point, etc.
 */
export type DataTarget = {
  name: string;
  description: string;
  pushOne: (parms: PushOneParms) => Promise<SinglePushResult>;
  pushAll?: (parms: PushAllParms) => Promise<BatchPushResult>;
}

/**
 * Basic implementation of a pushAll operation that iteratively calls a provided pushOne function.
 * @param param0 
 * @returns 
 */
export const BasicPushAllOperation = (
  { all, pusher }: 
  { all: PushAllParms, pusher: DataTarget }
) => {
  const { added, updated, removed } = all;

  return { 
    push: async (): Promise<BatchPushResult> => {
      let batchStatus: BatchStatus = BatchStatus.SUCCESS;
      let pushResult: SinglePushResult;
      const failures = new Array<SinglePushResult>();
      const successes = new Array<SinglePushResult>();

      const doPush = async (record: FieldSet, crud: CrudOperation): Promise<void> => {
        if( ! pusher.pushOne) {
          throw new Error("DataTarget must implement pushOne for BasicPushAllOperation to work.");
        }
        pushResult = await pusher.pushOne({ data: record, crud });
        if(pushResult.status === Status.FAILURE) {
          batchStatus = BatchStatus.PARTIAL;
          failures.push(pushResult);
        } 
        else {
          successes.push(pushResult);
        }
      };

      for(const record of added) {
        await doPush(record, CrudOperation.CREATE);
      }
      for(const record of updated) {
        await doPush(record, CrudOperation.UPDATE);
      }
      for(const record of removed) {
        await doPush(record, CrudOperation.DELETE);
      }

      return { 
        status: batchStatus, 
        successes, 
        failures, 
        timestamp: new Date(), 
        message: batchStatus === BatchStatus.SUCCESS ? 
          "All records pushed successfully." : batchStatus === BatchStatus.PARTIAL ? 
          "Some records failed to push." : "All records failed to push." 
      } satisfies BatchPushResult;      
    }
  }
};

