import { Timer } from "./Timer";

/**
 * Interface defining the contract for progress tracking functionality
 */
export interface IProgress {
  /**
   * Increment the completed count by the specified amount
   * @param count Number of items to increment by (default: 1)
   */
  increment(count?: number): void;

  /**
   * Get the current percentage of completion (0-100)
   */
  readonly percentage: number;

  /**
   * Get a formatted message showing percentage and completion counts
   */
  readonly percentCompleteMessage: string;

  /**
   * Get a comprehensive progress message with all timing information in JSON format
   */
  readonly fullProgressMessage: string;

  /**
   * Get the elapsed time since progress tracking started
   */
  readonly elapsedTime: string;

  /**
   * Get the estimated total time to complete all work
   */
  readonly estimatedTotalTime: string;

  /**
   * Get the estimated time remaining to complete all work
   */
  readonly estimatedTimeRemaining: string;
}

/**
 * Progress class to track the progress of a batch operation, such as fetching or pushing data
 */
export class Progress implements IProgress {
  private total: number;
  private startTime: Date;
  private completed: number;
  private getTime: () => number;
  private logAfter: number;
  private lastLoggedCompleted: number;

  constructor(private params: { 
    total: number, 
    startTime?: Date, 
    completed?: number,
    getTime?: () => number,
    logAfter?: number }) 
  {

    const { total, startTime=new Date(), completed=0, getTime=() => new Date().getTime(), logAfter=0 } = params;
    this.total = total;
    this.startTime = startTime;
    this.completed = completed;
    this.getTime = getTime;
    this.logAfter = logAfter;
    this.lastLoggedCompleted = completed;
  }

  public increment(count: number = 1): void {
    this.completed += count;
    
    // Log progress if logAfter is enabled and threshold is met
    if (this.logAfter > 0 && this.completed - this.lastLoggedCompleted >= this.logAfter) {
      console.log(this.fullProgressMessage);
      this.lastLoggedCompleted = this.completed;
    }
  }

  public get percentage(): number {
    return this.total > 0 ? (this.completed / this.total) * 100 : 100;
  }

  public get percentCompleteMessage(): string {
    return `${this.percentage.toFixed(2)}% complete (${this.completed} of ${this.total})`;
  }

  public get fullProgressMessage(): string {
    const data = {
      completed: this.completed,
      total: this.total,
      percentage: this.percentage.toFixed(2),
      elapsedTime: new Timer().getDuration(this.elapsedMilliseconds),
      estimatedTotalTime: this.estimatedTotalTime,
      estimatedTimeRemaining: this.estimatedTimeRemaining
    };
    return `Progress: ${JSON.stringify(data, null, 2)}`;
  }

  private get elapsedMilliseconds(): number {
    const now = new Date();
    return this.getTime() - this.startTime.getTime();
  }

  public get elapsedTime(): string {
    const roundedMs = Math.round(this.elapsedMilliseconds / 1000) * 1000;
    return new Timer().getDuration(roundedMs);
  }

  public get estimatedTotalTime(): string {
    if (this.completed === 0) {
      return 'Unknown';
    }
    const estimatedTotalMils = (this.elapsedMilliseconds / this.completed) * this.total;
    const roundedMils = Math.round(estimatedTotalMils / 1000) * 1000; // Round to nearest second
    return new Timer().getDuration(roundedMils);
  }

  public get estimatedTimeRemaining(): string {
    if (this.completed === 0) {
      return 'Unknown';
    }
    const elapsedMils = this.elapsedMilliseconds;
    const estimatedTotalMils = (elapsedMils / this.completed) * this.total;
    const remainingMils = estimatedTotalMils - elapsedMils;
    const roundedMils = Math.round(remainingMils / 1000) * 1000; // Round to nearest second
    return new Timer().getDuration(roundedMils);
  }
}



if (require.main === module) {
  // Simple test of Progress class
  const totalItems = 100;
  const progress = new Progress({ total: totalItems, logAfter: 10 });
  
  console.log('Starting progress test...');
  for (let i = 0; i < totalItems; i++) {
    // Simulate work
    const delay = Math.random() * 100; // Random delay between 0-100ms
    const start = Date.now();
    while (Date.now() - start < delay) {
      // Busy wait to simulate work
    }
    progress.increment();
  } 
  console.log('Progress test completed.');
  console.log('Final progress message:', progress.fullProgressMessage);
}