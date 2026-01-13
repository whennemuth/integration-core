/**
 * Represents a data source from which data can be fetched.
 */
export type DataSource = {
  name: string;
  description: string;
  /** Fetch raw data from the data source */
  fetchRaw: () => Promise<any>;
}