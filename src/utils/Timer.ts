
const humanizeDuration = require("humanize-duration");

// What humanize-duration considers to be the number of milliseconds in each unit of time
const defaults = { 
  y: 31557600000, 
  mo: 2629800000, 
  w: 604800000, 
  d: 86400000, 
  h: 3600000, 
  m: 60000, 
  s: 1000, 
  ms: 1 
}

export const humanReadableFromMilliseconds = (duration:number, daysOnly:boolean=true): string => {
  const { d, y } = defaults;
  let units = ["y", "mo", "w", "d", "h", "m", "s"];
  if(daysOnly) {
    if(duration >= d && duration < y) {
      // favor expressing time in days even if it is greater than a week or a month, until you get to years.
      units = [ "d", "h", "m", "s", "ms" ];
    }
  }
  return humanizeDuration(duration, { units });
}

export const humanReadableFromSeconds = (duration:number, daysOnly:boolean=true): string => humanReadableFromMilliseconds(duration * 1000, daysOnly);

export const humanReadableFromMinutes = (duration:number, daysOnly:boolean=true): string => humanReadableFromSeconds(duration * 60, daysOnly);

export const humanReadableFromHours = (duration:number, daysOnly:boolean=true): string => humanReadableFromMinutes(duration * 60, daysOnly);

export const humanReadableFromDays = (duration:number, daysOnly:boolean=true): string => humanReadableFromHours(duration * 24, daysOnly);


class Timer {
  private startTime: number;
  private elapsedTime: number = 0;

  constructor() { /** Nothing yet */ }

  public start = (): void => {
    this.reset();
    this.startTime = Date.now();
  }

  public stop = (): void => {
    if(this.startTime) {
      this.elapsedTime = (Date.now() - this.startTime);
      this.startTime = 0;
    }
  }
  
  public reset = (): void => {
    this.startTime = 0;
    this.elapsedTime = 0;
  }

  public getElapsedMilliseconds = (): number => {
    return this.elapsedTime;
  }

  public getDuration = (milliseconds:number): string => {
    return humanReadableFromMilliseconds(milliseconds);
  }

  public logElapsed = (prependMsg:string): string => {
    const msg = `${prependMsg}: ${this.getDuration(this.elapsedTime)}`;
    console.log(msg);
    return msg;
  }
}

if (require.main === module) {
  // Simple test 1
  const timer = new Timer();
  console.log(timer.getDuration(3661000)); // 1 hour, 1 minute, 1 second

  // Simple test 2
  timer.start();
  setTimeout(() => {
    timer.stop();
    timer.logElapsed("Test Task");
  }, 3671);
}

export { Timer };