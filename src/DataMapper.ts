import { Input } from "./InputTypes";

export type DataMapper = {
  
  map: (rawData: any) => Input;
};
