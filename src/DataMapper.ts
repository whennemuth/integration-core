import { CrudOperation } from "./DataTarget";
import { Input } from "./InputTypes";

export type DataMapper = {
  
  map: (rawData: any, crudOperation?: CrudOperation) => Input;
};
