/**
 * AWS region resolution utilities
 * Shared utilities for resolving AWS regions across different components
 */

export type RegionConfig = {
  region?: string;
};

/**
 * Resolves AWS region from multiple sources in priority order:
 * 1. Explicit region from config
 * 2. AWS_REGION environment variable
 * 3. REGION environment variable (custom fallback)
 * 4. undefined (let AWS SDK use its default resolution)
 */
export function resolveAwsRegion(config?: RegionConfig): string | undefined {
  // 1. Check explicit config first
  if (config?.region) {
    return config.region;
  }
  
  // 2. Check AWS_REGION environment variable
  if (process.env.AWS_REGION) {
    return process.env.AWS_REGION;
  }
  
  // 3. Check REGION environment variable (custom fallback)
  if (process.env.REGION) {
    return process.env.REGION;
  }
  
  // 4. Return undefined to let AWS SDK handle default resolution
  return undefined;
}


/**
 * Get the value of an a specified environment variable that has the specified prefix. If the environment 
 * variable with the prefix is not set, it will attempt to find an environment variable with the same 
 * name but without the prefix. Both will get set, favoring the prefixed version if it exists. This allows 
 * for flexible configuration where you can set either MY_PREFIX_VARIABLE or VARIABLE, and the function 
 * will ensure that both are available in process.env.
 * @param param0 
 * @returns 
 */
export const TestEnvironment = (prefix: string, custom?: (entry: {key:string, val?:string}) => string): { 
  getVar: (key: string) => string | undefined, 
  getVarOrEmptyString: (key: string) => string 
} => {
  if(prefix && !prefix.endsWith('_')) {
    prefix = `${prefix}_`;
  }

  const getVar = (key: string): string | undefined => {
    let val = process.env[prefix + key];
    if(custom) {
      val = custom({ key, val });
      if(val) {
        process.env[prefix + key] = val;
        process.env[key] = val;
      }
      return val;
    }    
    if(val) {
      // Set the non-prefixed version as well for convenience, favoring the prefixed version if both exist
      process.env[key] = val;
    }
    else {
      // Check for a non-prefixed version if the prefixed version is not set, and set the prefixed version to it if found
      val = process.env[key];
      if(val) {
        process.env[prefix + key] = val;
      }
    }

    return val;
  };

  return { 
    getVar,
    getVarOrEmptyString: (key: string): string => {
      return getVar(key) || '';
    }
  }
}