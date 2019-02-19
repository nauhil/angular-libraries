import {
  interval as observableInterval,
  BehaviorSubject,
  Observable,
  Subject,
  of,
  timer,
  merge
} from 'rxjs';
import { ActivityMonitor } from './activity-monitor.interface';
import { ACTIVITY_MONITOR } from './activity-monitor.token';
import { InactivityConfig } from './inactivity-config.interface';
import { INACTIVITY_CONFIG } from './inactivity-config.token';
import { Inject, Injectable, Optional } from '@angular/core';
import {
  switchMap,
  throttle,
  map,
  startWith,
  filter,
  takeUntil
} from 'rxjs/operators';

@Injectable({
  providedIn: 'root',
  deps: [INACTIVITY_CONFIG, ACTIVITY_MONITOR]
})
export class InactivityTimerService {
  private timeout$: Observable<Date | never>;
  private monitor$ = new BehaviorSubject<boolean>(true);
  private artificialActivity$ = new Subject<void>();

  constructor(
    @Inject(INACTIVITY_CONFIG) private config: InactivityConfig,
    @Optional()
    @Inject(ACTIVITY_MONITOR)
    private monitor: ActivityMonitor
  ) {
    // Merge all monitors together, and activate them via register()
    this.timeout$ = this.monitor$.pipe(
      filter(x => !!x),
      switchMap(m => {
        return merge(this.monitor.getMonitor(), this.artificialActivity$).pipe(
          throttle(() => observableInterval(500)), // Throttle - to stop spamming
          startWith(undefined), // Trigger observable immediately
          map(() => {
            const d = new Date();
            d.setMinutes(d.getMinutes() + config.inactivityTime);
            return d;
          }),
          takeUntil(this.monitor$.pipe(filter(x => !x)))
        );
      })
    );
  }

  /**
   * Emits Timeout objects describing current timeout state
   * @returns Observable<Timeout>
   */
  public getTimeOut(): Observable<Timeout> {
    return this.timeout$.pipe(
      switchMap((date: Date) => {
        return timer(0, 60000).pipe(
          switchMap(() => {
            const f = new Date();
            return of({
              showWarning: this.config.warningTime
                ? f.getTime() + this.config.warningTime > date.getTime()
                : false,
              timedOut: f.getTime() > date.getTime(),
              timeLeft: date.getTime() - f.getTime()
            });
          })
        );
      })
    );
  }

  /**
   * Starts listening for activity.
   * @param activate=false. If true will reset the activity timer as any other activity.
   * @returns void
   */
  public startMonitor(activate = false): void {
    this.monitor$.next(true);
    if (activate) {
      this.activate();
    }
  }

  /**
   * Stops listening for activity.
   * @returns void
   */
  public stopMonitor(): void {
    this.monitor$.next(false);
  }

  /**
   * Provides a programmatic way of simulating activity.
   * Will reset the activity timer as any other activity.
   */
  public activate(): void {
    this.artificialActivity$.next(undefined);
  }
}

export interface Timeout {
  showWarning: boolean;
  timedOut: boolean;
  timeLeft: number;
}
