import { InputParser } from "./InputParser";
import { Input } from "./InputTypes";

/**
 * Represents a data source from which data can be fetched and converted to output as an Input object.
 */
export type DataSource = {
  name: string;
  description: string;
  fetchRaw: () => Promise<any>;
  // Convert the raw data fetched into an Input object. 
  // This is also where only fields of interest are cherry picked out to form the Input.
  convertRawToInput: (rawData: any) => Input;
}