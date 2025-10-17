import { FieldDefinition } from '../../../src/InputTypes';

/**
 * Test data generator for FileSystemDeltaStorage that creates random FieldSet data
 * based on provided FieldDefinitions and stores it using the delta storage system.
 */
export class RandomData {

  public static fullNameFieldName: string = 'fullname';
  public static roles : string[] = [ 'admin', 'user', 'guest' ];
  public static hobbies: string[] = [
    'sports', 'music', 'travel', 'reading', 'gaming', 'cooking', 'hiking', 'photography', 'gardening', 
    'crafting', 'dancing', 'writing', 'yoga', 'fishing',  'cycling', 'swimming', 'running', 'collecting', 
    'volunteering', 'blogging'
  ];
  public static fieldDefinitions: FieldDefinition[] = [
    { name: RandomData.fullNameFieldName, type: 'string', required: true },
    { name: 'dob', type: 'date', required: true, restrictions: [{ min: 0, max: 120 }], isPrimaryKey: true },
    { name: 'email', type: 'email', required: true , isPrimaryKey: true },
    { name: 'isActive', type: 'boolean', required: true },
    { name: 'signupDate', type: 'date', required: false },
    { name: 'website', type: 'url', required: false },
    { name: 'role', type: 'select', required: true, options: { matchCase: false, values: RandomData.roles} },
    { name: 'interests', type: 'multiselect', required: false, options: { matchCase: false, values: RandomData.hobbies } }
  ];

  /**
   * @param recordCount The number of random data records to generate
   */
  constructor(public readonly recordCount: number) { }

  /**
   * Generates a random boolean value
   * @returns A random boolean value as "Y" or "N", where "N" is 10x more likely
   */
  public generateRandomYN = (): string => {
    // Generate random number 0-10, where 0 = "Y", 1-10 = "N"
    // This makes "N" 10 times more likely than "Y"
    return Math.floor(Math.random() * 11) === 0 ? "Y" : "N";
  }

  /**
   * Generates a random date ISO formatted date value
   * @returns A random date value between 18 and 100 years ago
   */
  public generateRandomDate = (minYear:number, maxYear:number): string => {
    const now = new Date();
    const minYearsAgo = new Date(now.getFullYear() - minYear, now.getMonth(), now.getDate());
    const maxYearsAgo = new Date(now.getFullYear() - maxYear, now.getMonth(), now.getDate());
    const randomTime = maxYearsAgo.getTime() + Math.random() * (minYearsAgo.getTime() - maxYearsAgo.getTime());
    return new Date(randomTime).toISOString();
  }

  /**
   * Generates a random email address
   * @param name - The name components to use in the email
   * @returns A random valid email address
   */
  public generateRandomEmail = (name: { fn:string, mi:string, ln:string }): string => {
    const separators = ['-', '_', '.'];
    const separator = separators[Math.floor(Math.random() * 3)];
    const username = `${name.fn.toLowerCase()}${separator}${name.mi.toLowerCase()}${separator}${name.ln.toLowerCase()}`;
    return `${username}@bu.edu`;
  }

  /**
   * Generates a random URL
   * @param name - The name components to use in the URL
   * @returns A random valid URL
   */
  public generateRandomUrl = (name: { fn:string, mi:string, ln:string }): string | undefined => {
    const domains = [ 'com', 'org', 'net', 'edu' ];
    const email = this.generateRandomEmail(name);
    const domain = domains[Math.floor(Math.random() * domains.length)];
    return `http://www.${email.split('@')[0]}.${domain}`;
  }

  /**
   * Generates random values from multiselect options
   * @param pool - The pool of string values to select from
   * @returns An array of random values from the available options
   */
  public generateRandomMultiselect = (pool: string[], limit?: number): string[] => {
    if (pool.length === 0) return [];
    if (pool.length === 1) return [...pool]; // Return copy of single item
    
    // Determine the effective limit
    let effectiveLimit: number;
    if (limit !== undefined) {
      effectiveLimit = limit;
    } else {
      // If no limit provided, randomly assign one between 1 and pool.length - 1
      effectiveLimit = Math.floor(Math.random() * (pool.length - 1)) + 1;
    }
    
    // Ensure effective limit doesn't exceed pool size - 1
    effectiveLimit = Math.min(effectiveLimit, pool.length - 1);
    effectiveLimit = Math.max(1, effectiveLimit); // Ensure at least 1
    
    // Randomly select between 1 and effectiveLimit items
    const numSelections = Math.floor(Math.random() * effectiveLimit) + 1;
    
    // Shuffle pool and take first numSelections items
    const shuffled = [...pool].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, numSelections);
  }
}