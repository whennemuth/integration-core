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