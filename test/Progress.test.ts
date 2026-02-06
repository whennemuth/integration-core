import { Progress, IProgress } from "../src/utils/Progress";

describe('Progress', () => {
  let mockTime: number;
  let mockGetTime: () => number;

  beforeEach(() => {
    mockTime = 1000000000; // Start at a fixed time
    mockGetTime = jest.fn(() => mockTime);
  });

  describe('interface implementation', () => {
    it('should implement IProgress interface', () => {
      const progress = new Progress({ total: 100, getTime: mockGetTime });
      
      // Verify all IProgress methods and properties exist
      expect(typeof progress.increment).toBe('function');
      expect(typeof progress.percentage).toBe('number');
      expect(typeof progress.percentCompleteMessage).toBe('string');
      expect(typeof progress.fullProgressMessage).toBe('string');
      expect(typeof progress.elapsedTime).toBe('string');
      expect(typeof progress.estimatedTotalTime).toBe('string');
      expect(typeof progress.estimatedTimeRemaining).toBe('string');
    });

    it('should work when assigned to IProgress type', () => {
      const progress: IProgress = new Progress({ 
        total: 100, 
        getTime: mockGetTime,
        startTime: new Date(mockTime)
      });
      
      progress.increment(10);
      expect(progress.percentage).toBe(10);
      expect(progress.percentCompleteMessage).toBe('10.00% complete (10 of 100)');
      expect(progress.elapsedTime).toBe('0 seconds');
      expect(progress.fullProgressMessage).toContain('"completed": 10');
    });
  });

  describe('constructor', () => {
    it('should initialize with default values', () => {
      const progress = new Progress({ total: 100 });
      
      expect(progress.percentage).toBe(0);
      expect(progress.percentCompleteMessage).toBe('0.00% complete (0 of 100)');
    });

    it('should initialize with custom values', () => {
      const startTime = new Date('2023-01-01T10:00:00.000Z');
      const progress = new Progress({
        total: 50,
        startTime,
        completed: 10,
        getTime: mockGetTime
      });

      expect(progress.percentage).toBe(20);
      expect(progress.percentCompleteMessage).toBe('20.00% complete (10 of 50)');
    });

    it('should initialize with logAfter parameter', () => {
      const progress = new Progress({ 
        total: 100, 
        logAfter: 10,
        getTime: mockGetTime 
      });
      
      expect(progress.percentage).toBe(0);
      expect(progress.percentCompleteMessage).toBe('0.00% complete (0 of 100)');
    });

    it('should handle zero total gracefully', () => {
      const progress = new Progress({ total: 0 });
      
      expect(progress.percentage).toBe(100);
      expect(progress.percentCompleteMessage).toBe('100.00% complete (0 of 0)');
    });
  });

  describe('increment', () => {
    it('should increment completed count by 1 by default', () => {
      const progress = new Progress({ total: 100 });
      
      progress.increment();
      
      expect(progress.percentage).toBe(1);
      expect(progress.percentCompleteMessage).toBe('1.00% complete (1 of 100)');
    });

    it('should increment completed count by specified amount', () => {
      const progress = new Progress({ total: 100 });
      
      progress.increment(5);
      
      expect(progress.percentage).toBe(5);
      expect(progress.percentCompleteMessage).toBe('5.00% complete (5 of 100)');
    });

    it('should allow incrementing beyond total', () => {
      const progress = new Progress({ total: 10 });
      
      progress.increment(15);
      
      expect(progress.percentage).toBe(150);
      expect(progress.percentCompleteMessage).toBe('150.00% complete (15 of 10)');
    });
  });

  describe('percentage', () => {
    it('should calculate percentage correctly for various completion states', () => {
      const progress = new Progress({ total: 200, completed: 50 });
      
      expect(progress.percentage).toBe(25);
      
      progress.increment(50);
      expect(progress.percentage).toBe(50);
      
      progress.increment(100);
      expect(progress.percentage).toBe(100);
    });

    it('should handle fractional percentages', () => {
      const progress = new Progress({ total: 3, completed: 1 });
      
      expect(progress.percentage).toBeCloseTo(33.33, 2);
    });
  });

  describe('percentCompleteMessage', () => {
    it('should format message correctly', () => {
      const progress = new Progress({ total: 1000, completed: 250 });
      
      expect(progress.percentCompleteMessage).toBe('25.00% complete (250 of 1000)');
    });
  });

  describe('elapsedTime', () => {
    it('should calculate elapsed time using mocked time function', () => {
      const startTime = new Date(mockTime - 5000); // Started 5 seconds ago
      const progress = new Progress({
        total: 100,
        startTime,
        getTime: mockGetTime
      });

      expect(progress.elapsedTime).toBe('5 seconds');
    });

    it('should update elapsed time as mock time advances', () => {
      const startTime = new Date(mockTime);
      const progress = new Progress({
        total: 100,
        startTime,
        getTime: mockGetTime
      });

      // Initially no time elapsed
      expect(progress.elapsedTime).toBe('0 seconds');

      // Advance mock time by 1 minute
      mockTime += 60000;
      expect(progress.elapsedTime).toBe('1 minute');

      // Advance mock time by another 2 minutes and 30 seconds
      mockTime += 150000;
      expect(progress.elapsedTime).toBe('3 minutes, 30 seconds');
    });

    it('should provide accurate estimates even with very small elapsed times', () => {
      const startTime = new Date(mockTime);
      const progress = new Progress({
        total: 100,
        startTime,
        completed: 0,
        getTime: mockGetTime
      });

      // After 500ms with 10 items completed
      mockTime += 500;
      progress.increment(10);

      // Should round display to 1 second but calculate with precise 500ms
      expect(progress.elapsedTime).toBe('1 second'); // Rounds 500ms to 1 second
      expect(progress.estimatedTotalTime).toBe('5 seconds'); // 500ms * 10 = 5000ms = 5 seconds
      expect(progress.estimatedTimeRemaining).toBe('5 seconds'); // 5s total - 0s rounded remaining = 5s

      // After 800ms with 20 items completed
      mockTime += 300; // Total 800ms
      progress.increment(10);

      expect(progress.elapsedTime).toBe('1 second'); // Rounds 800ms to 1 second  
      expect(progress.estimatedTotalTime).toBe('4 seconds'); // 800ms / 20 * 100 = 4000ms = 4 seconds
      expect(progress.estimatedTimeRemaining).toBe('3 seconds'); // 4000ms - 800ms = 3200ms = 3 seconds
    });
  });

  describe('estimatedTotalTime', () => {
    it('should return "Unknown" when no progress has been made', () => {
      const progress = new Progress({
        total: 100,
        completed: 0,
        getTime: mockGetTime
      });

      expect(progress.estimatedTotalTime).toBe('Unknown');
    });

    it('should calculate estimated total time based on current progress', () => {
      const startTime = new Date(mockTime - 10000); // Started 10 seconds ago
      const progress = new Progress({
        total: 100,
        startTime,
        completed: 25, // 25% complete in 10 seconds
        getTime: mockGetTime
      });

      // 25 items in 10 seconds = 400ms per item
      // 100 items would take 40 seconds total (rounded)
      expect(progress.estimatedTotalTime).toBe('40 seconds');
    });

    it('should update estimates as work progresses', () => {
      const startTime = new Date(mockTime);
      const progress = new Progress({
        total: 100,
        startTime,
        completed: 0,
        getTime: mockGetTime
      });

      // Complete 50 items in 30 seconds
      mockTime += 30000;
      progress.increment(50);

      // 50 items in 30 seconds = 600ms per item
      // 100 items would take 60 seconds total
      expect(progress.estimatedTotalTime).toBe('1 minute');
    });
  });

  describe('estimatedTimeRemaining', () => {
    it('should return "Unknown" when no progress has been made', () => {
      const progress = new Progress({
        total: 100,
        completed: 0,
        getTime: mockGetTime
      });

      expect(progress.estimatedTimeRemaining).toBe('Unknown');
    });

    it('should calculate remaining time based on current progress', () => {
      const startTime = new Date(mockTime - 20000); // Started 20 seconds ago
      const progress = new Progress({
        total: 100,
        startTime,
        completed: 50, // 50% complete in 20 seconds
        getTime: mockGetTime
      });

      // 50 items in 20 seconds = 400ms per item
      // Remaining 50 items would take another 20 seconds
      expect(progress.estimatedTimeRemaining).toBe('20 seconds');
    });

    it('should show decreasing time remaining as work progresses', () => {
      const startTime = new Date(mockTime);
      const progress = new Progress({
        total: 100,
        startTime,
        completed: 0,
        getTime: mockGetTime
      });

      // Complete 25 items in 10 seconds
      mockTime += 10000;
      progress.increment(25);

      // 25 items in 10 seconds = 400ms per item
      // Remaining 75 items would take 30 seconds
      expect(progress.estimatedTimeRemaining).toBe('30 seconds');

      // Complete another 25 items in another 10 seconds
      mockTime += 10000;
      progress.increment(25);

      // Now 50 items in 20 seconds = 400ms per item
      // Remaining 50 items would take 20 seconds
      expect(progress.estimatedTimeRemaining).toBe('20 seconds');
    });

    it('should handle completion correctly', () => {
      const startTime = new Date(mockTime - 30000); // Started 30 seconds ago
      const progress = new Progress({
        total: 100,
        startTime,
        completed: 100, // 100% complete
        getTime: mockGetTime
      });

      expect(progress.estimatedTimeRemaining).toBe('0 seconds');
    });
  });

  describe('fullProgressMessage', () => {
    it('should include all progress information in JSON format', () => {
      const startTime = new Date(mockTime - 15000); // Started 15 seconds ago
      const progress = new Progress({
        total: 200,
        startTime,
        completed: 75, // 37.5% complete
        getTime: mockGetTime
      });

      const message = progress.fullProgressMessage;
      
      expect(message).toContain('"completed": 75');
      expect(message).toContain('"total": 200');
      expect(message).toContain('"percentage": "37.50"');
      expect(message).toContain('"elapsedTime": "15 seconds"');
      expect(message).toContain('"estimatedTotalTime": "40 seconds"');
      expect(message).toContain('"estimatedTimeRemaining": "25 seconds"');
    });

    it('should handle edge cases in full progress message', () => {
      const progress = new Progress({
        total: 50,
        completed: 0,
        getTime: mockGetTime
      });

      const message = progress.fullProgressMessage;
      
      expect(message).toContain('"completed": 0');
      expect(message).toContain('"total": 50');
      expect(message).toContain('"percentage": "0.00"');
      expect(message).toContain('"estimatedTotalTime": "Unknown"');
      expect(message).toContain('"estimatedTimeRemaining": "Unknown"');
    });
  });

  describe('time estimation accuracy', () => {
    it('should provide consistent estimates across multiple increments', () => {
      const startTime = new Date(mockTime);
      const progress = new Progress({
        total: 1000,
        startTime,
        completed: 0,
        getTime: mockGetTime
      });

      // Process items at a steady rate: 10 items per second
      for (let i = 0; i < 5; i++) {
        mockTime += 1000; // Advance 1 second
        progress.increment(10); // Complete 10 items
      }

      // After 5 seconds, 50 items completed
      // Rate: 10 items/second, so 1000 items should take 100 seconds total (rounded)
      expect(progress.estimatedTotalTime).toBe('1 minute, 40 seconds');
      // Remaining: 950 items at 10 items/second = 95 seconds (rounded)
      expect(progress.estimatedTimeRemaining).toBe('1 minute, 35 seconds');
    });

    it('should handle variable processing rates', () => {
      const startTime = new Date(mockTime);
      const progress = new Progress({
        total: 100,
        startTime,
        completed: 0,
        getTime: mockGetTime
      });

      // Fast start: 20 items in 5 seconds
      mockTime += 5000;
      progress.increment(20);
      
      const firstEstimate = progress.estimatedTotalTime;
      expect(firstEstimate).toBe('25 seconds');

      // Slow down: 10 more items in 10 seconds
      mockTime += 10000;
      progress.increment(10);

      // Now 30 items in 15 seconds = 500ms per item
      // 100 items would take 50 seconds total (rounded)
      expect(progress.estimatedTotalTime).toBe('50 seconds');
    });
  });

  describe('logging functionality', () => {
    let consoleSpy: jest.SpyInstance;

    beforeEach(() => {
      consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    });

    afterEach(() => {
      consoleSpy.mockRestore();
    });

    it('should not log when logAfter is 0 (default)', () => {
      const progress = new Progress({ 
        total: 100,
        getTime: mockGetTime 
      });
      
      progress.increment(10);
      progress.increment(5);
      progress.increment(20);
      
      expect(consoleSpy).not.toHaveBeenCalled();
    });

    it('should not log when logAfter is not specified', () => {
      const progress = new Progress({ 
        total: 100,
        getTime: mockGetTime 
      });
      
      progress.increment(50);
      
      expect(consoleSpy).not.toHaveBeenCalled();
    });

    it('should log when increment reaches logAfter threshold', () => {
      const progress = new Progress({ 
        total: 100,
        logAfter: 10,
        getTime: mockGetTime 
      });
      
      progress.increment(5);
      expect(consoleSpy).not.toHaveBeenCalled();
      
      progress.increment(5); // Total: 10, should trigger log
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 10'));
    });

    it('should log when single increment exceeds logAfter threshold', () => {
      const progress = new Progress({ 
        total: 100,
        logAfter: 5,
        getTime: mockGetTime 
      });
      
      progress.increment(15); // Exceeds threshold, should log
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 15'));
    });

    it('should log multiple times as thresholds are reached', () => {
      const progress = new Progress({ 
        total: 100,
        logAfter: 10,
        getTime: mockGetTime 
      });
      
      // First threshold
      progress.increment(10);
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 10'));
      
      // Not enough to trigger next log
      progress.increment(5);
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      
      // Second threshold
      progress.increment(5);
      expect(consoleSpy).toHaveBeenCalledTimes(2);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 20'));
      
      // Third threshold with larger increment
      progress.increment(15);
      expect(consoleSpy).toHaveBeenCalledTimes(3);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 35'));
    });

    it('should respect initial completed value for logging threshold', () => {
      const progress = new Progress({ 
        total: 100,
        completed: 5,
        logAfter: 10,
        getTime: mockGetTime 
      });
      
      // Need 10 more from the initial 5 to reach first threshold (5 + 10 = 15)
      progress.increment(9);
      expect(consoleSpy).not.toHaveBeenCalled();
      
      progress.increment(1); // Total: 15, increment of 10 from initial, should log
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 15'));
    });

    it('should include timing information in logged messages', () => {
      const startTime = new Date(mockTime - 5000);
      const progress = new Progress({ 
        total: 100,
        startTime,
        logAfter: 25,
        getTime: mockGetTime 
      });
      
      mockTime += 10000; // Advance time
      progress.increment(25);
      
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      const loggedMessage = consoleSpy.mock.calls[0][0];
      expect(loggedMessage).toContain('"completed": 25');
      expect(loggedMessage).toContain('"total": 100');
      expect(loggedMessage).toContain('"percentage": "25.00"');
      expect(loggedMessage).toContain('"elapsedTime"');
      expect(loggedMessage).toContain('"estimatedTotalTime"');
      expect(loggedMessage).toContain('"estimatedTimeRemaining"');
    });

    it('should handle edge case where logAfter equals total', () => {
      const progress = new Progress({ 
        total: 50,
        logAfter: 50,
        getTime: mockGetTime 
      });
      
      progress.increment(49);
      expect(consoleSpy).not.toHaveBeenCalled();
      
      progress.increment(1); // Completes all work
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 50'));
    });

    it('should handle logAfter larger than total', () => {
      const progress = new Progress({ 
        total: 10,
        logAfter: 20,
        getTime: mockGetTime 
      });
      
      progress.increment(10); // Complete all work
      expect(consoleSpy).not.toHaveBeenCalled(); // Threshold not reached
      
      progress.increment(10); // Go beyond total
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('"completed": 20'));
    });
  });

  describe('edge cases', () => {
    it('should handle negative time differences gracefully', () => {
      // Create a start time in the future
      const futureStartTime = new Date(mockTime + 10000);
      const progress = new Progress({
        total: 100,
        startTime: futureStartTime,
        getTime: mockGetTime
      });

      // This creates a negative elapsed time, which should be handled gracefully
      const elapsedTime = progress.elapsedTime;
      expect(typeof elapsedTime).toBe('string');
    });

    it('should handle very large numbers', () => {
      const progress = new Progress({
        total: 1000000,
        completed: 500000,
        getTime: mockGetTime
      });

      expect(progress.percentage).toBe(50);
      expect(progress.percentCompleteMessage).toBe('50.00% complete (500000 of 1000000)');
    });

    it('should handle floating point precision issues', () => {
      const progress = new Progress({
        total: 3,
        completed: 1,
        getTime: mockGetTime
      });

      // 1/3 = 0.3333... which should be handled correctly
      expect(progress.percentage).toBeCloseTo(33.333333, 5);
      expect(progress.percentCompleteMessage).toBe('33.33% complete (1 of 3)');
    });
  });

  describe('real-world scenarios', () => {
    it('should simulate a file download progress', () => {
      const startTime = new Date(mockTime);
      const totalBytes = 1000000; // 1MB
      const progress = new Progress({
        total: totalBytes,
        startTime,
        completed: 0,
        getTime: mockGetTime
      });

      // Download 100KB every 2 seconds
      const chunkSize = 100000;
      const chunkTime = 2000;

      for (let downloaded = 0; downloaded < totalBytes; downloaded += chunkSize) {
        mockTime += chunkTime;
        progress.increment(Math.min(chunkSize, totalBytes - downloaded));

        if (downloaded + chunkSize >= totalBytes) {
          // Final chunk - should be 100% complete
          expect(progress.percentage).toBe(100);
          expect(progress.estimatedTimeRemaining).toBe('0 seconds');
        }
      }

      // Total time should be 20 seconds (10 chunks * 2 seconds each)
      expect(progress.elapsedTime).toBe('20 seconds');
    });

    it('should simulate batch processing with irregular timing and logging', () => {
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      
      const startTime = new Date(mockTime);
      const progress = new Progress({
        total: 500,
        startTime,
        completed: 0,
        logAfter: 100, // Log every 100 items
        getTime: mockGetTime
      });

      // Process different batch sizes with different timings
      const batches = [
        { items: 50, duration: 5000 },   // Fast batch
        { items: 100, duration: 15000 }, // Slow batch - should trigger log
        { items: 75, duration: 8000 },   // Medium batch
      ];

      for (const batch of batches) {
        mockTime += batch.duration;
        progress.increment(batch.items);
      }

      // Total: 225 items in 28 seconds
      expect(progress.percentage).toBe(45); // 225/500 = 45%
      expect(progress.elapsedTime).toBe('28 seconds');
      
      // Should have logged once when reaching 150 items (after second batch)
      expect(consoleSpy).toHaveBeenCalledTimes(1);
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('\"completed\": 150'));
      
      consoleSpy.mockRestore();
      
      // Estimation should be based on average rate
      // 225 items in 28 seconds ≈ 124.4ms per item
      // 500 items would take ≈ 62.2 seconds
      expect(progress.estimatedTotalTime).toMatch(/1 minute/);
    });
  });
});