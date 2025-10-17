import { FieldSet } from "./InputTypes";

export type PushAllParms = {
  added: FieldSet[];
  updated: FieldSet[];
  removed: FieldSet[];
}

export type PushOneParms = {
  data: FieldSet;
  crud: 'create' | 'update' | 'delete';
}

export type PushResult = {
  status: 'success' | 'failure';
  reason?: string;
}

/**
 * Represents a target where delta results can be pushed to, like an api endpoint, database, ftp drop point, etc.
 */
export type DataTarget = {
  name: string;
  description: string;
  pushOne?: (parms: PushOneParms) => Promise<any>;
  pushAll?: (parms: PushAllParms) => Promise<any>;
}

export const BasicPushAllOperation = ({ all, pusher }: { all: PushAllParms, pusher: (parms: PushOneParms) => any }) => {
  const { added, updated, removed } = all;
  return { push: async () => {
    for(const record of added) {
      await pusher({ data: record, crud: 'create' });
    }
    for(const record of updated) {
      await pusher({ data: record, crud: 'update' });
    }
    for(const record of removed) {
      await pusher({ data: record, crud: 'delete' });
    }
  }}
};
