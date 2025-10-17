import { DataSource } from "../../../src/DataSource";
import { Field, Input } from "../../../src/InputTypes";
import { RandomData } from "./RandomData";

export class MockDataSource implements DataSource {
  private rawDataFile: string;

  constructor(private parms: { clientId: string, generator: RandomData, storagePath: string }) {
    this.rawDataFile = `${parms.storagePath}/${parms.clientId}/data-source.json`;
  }

  public get name(): string {
    return "Mock Data Source";
  }

  public get description(): string {
    return "A mock data source for generating test data";
  }

  /**
   * Save the raw data as a file.
   * @param rawData 
   */
  private cacheRawData = async (rawData: any[]): Promise<void> => {
    const fs = await import('fs/promises');
    const path = await import('path');
    const { rawDataFile } = this;
    
    try {
      // Ensure the directory exists
      const dir = path.dirname(rawDataFile);
      await fs.mkdir(dir, { recursive: true });
      
      // Write the raw data as JSON
      const jsonData = JSON.stringify(rawData, null, 2);
      await fs.writeFile(rawDataFile, jsonData, 'utf8');
      
    } catch (error) {
      console.error(`Failed to cache raw data to ${rawDataFile}:`, error);
      throw error;
    }
  }

  /**
   * Determine if the raw data cache file exists and load it.
   * @returns The cached raw data array, or undefined if file doesn't exist
   */
  private getCachedRawData = async (): Promise<any[] | undefined> => {
    const fs = await import('fs/promises');
    const { rawDataFile } = this;
    
    try {
      // Check if file exists and read it
      await fs.access(rawDataFile);
      const fileContent = await fs.readFile(rawDataFile, 'utf8');
      const rawData = JSON.parse(fileContent);
      
      // Validate it's an array
      if (Array.isArray(rawData)) {
        return rawData;
      } else {
        console.warn(`Cached raw data in ${rawDataFile} is not an array`);
        return undefined;
      }
      
    } catch (error) {
      // File doesn't exist or can't be read/parsed
      if ((error as any).code === 'ENOENT') {
        return undefined; // File doesn't exist
      } else {
        console.error(`Error reading cached raw data from ${rawDataFile}:`, error);
        return undefined;
      }
    }
  }

  private generateRaw = (): any => {
    const { generator, generator: { 
      generateRandomEmail, generateRandomDate, generateRandomYN, generateRandomUrl, generateRandomMultiselect 
    } } = this.parms;
    const records: any[] = [];

    for (let i = 0; i < generator.recordCount; i++) {
      const mi = String.fromCharCode(65 + Math.floor(Math.random() * 26));
      const name = { fn: `FirstName${i}`, mi: mi, ln: `LastName${i}` };
      const email = generateRandomEmail(name);
      const { roles, hobbies } = RandomData;

      records.push({
        firstName: name.fn,
        middleInitial: name.mi,
        lastName: name.ln,
        dob: generateRandomDate(18, 100),
        memberSince: generateRandomDate(0, 12),
        deactivated: generateRandomYN(),
        emailAddress: email,
        role: roles[Math.floor(Math.random() * roles.length)],
        website: generateRandomUrl(name),
        hobbies: generateRandomMultiselect(hobbies)
      });
    }
    return records;
  }

  public fetchRaw = async (): Promise<any> => {
    const { getCachedRawData, cacheRawData, generateRaw } = this;

    // Try to get cached data first
    let rawData = await getCachedRawData();
    
    if (!rawData) {
      // Generate new data if no cache exists
      rawData = generateRaw();
      
      // Cache the generated data for future use
      if (rawData) {
        await cacheRawData(rawData);
      }
    }
    
    return rawData;
  }

  public convertRawToInput = (rawData: any): Input => {
    const { fieldDefinitions } = RandomData;
    const getShortDate = (dte: Date | string): string => {
      if( dte instanceof Date ) {
        return dte.toISOString().split('T')[0];
      }
      else if(/^\d{4}\-\d{2}\-\d{2}/.test(dte)) {
        return dte.substring(0,10);
      }
      return dte;
    }
    const converted = {
      fieldDefinitions,
      fieldSets: rawData.map((rec: any) => ({
        fieldValues: [
          { fullname: `${rec.firstName} ${rec.middleInitial} ${rec.lastName}` },
          { dob: getShortDate(rec.dob) },
          { isActive: rec.deactivated === 'N' ? true : false },
          { signupDate: getShortDate(rec.memberSince) },
          { email: rec.emailAddress },
          { website: rec.website },
          { role: rec.role },
          { interests: rec.hobbies }
        ] satisfies Array<Field>
      }))
    } satisfies Input;
    return converted;
  };
}