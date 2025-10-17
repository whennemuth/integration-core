import { Input } from "./InputTypes";

/**
 * Represents a data source from which data can be fetched and converted as an Input object.
 */
export type DataSource = {
  name: string;
  description: string;
  /** Fetch raw data from the data source */
  fetchRaw: () => Promise<any>;
  /**
   * Send the raw data fetched through a mapping process that converts field names and formats 
   * into a form compatible with the data target as an Input object.
   * This is also where only fields of interest are cherry picked out to form that Input.
   * @param rawData 
   * @returns 
   */
  convertRawToInput: (rawData: any) => Input;
}