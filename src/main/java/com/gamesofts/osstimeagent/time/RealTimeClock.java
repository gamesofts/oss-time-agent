package com.gamesofts.osstimeagent.time;

import java.util.concurrent.atomic.AtomicLong;

public final class RealTimeClock {
    private volatile long baseRealMillis;
    private volatile long baseNanoTime;
    private final AtomicLong lastReturnedMillis;

    public RealTimeClock() {
        long now = System.currentTimeMillis();
        this.baseRealMillis = now;
        this.baseNanoTime = System.nanoTime();
        this.lastReturnedMillis = new AtomicLong(now);
    }

    public void updateBaseTime(long realMillis) {
        this.baseRealMillis = realMillis;
        this.baseNanoTime = System.nanoTime();
        long prev;
        do {
            prev = lastReturnedMillis.get();
            if (realMillis <= prev) {
                return;
            }
        } while (!lastReturnedMillis.compareAndSet(prev, realMillis));
    }

    public void updateBaseTimeAuthoritative(long realMillis) {
        this.baseRealMillis = realMillis;
        this.baseNanoTime = System.nanoTime();
        this.lastReturnedMillis.set(realMillis);
    }

    public long currentTimeMillis() {
        long elapsedNanos = System.nanoTime() - baseNanoTime;
        long candidate = baseRealMillis + (elapsedNanos / 1000000L);
        for (;;) {
            long prev = lastReturnedMillis.get();
            if (candidate <= prev) {
                return prev;
            }
            if (lastReturnedMillis.compareAndSet(prev, candidate)) {
                return candidate;
            }
        }
    }

    public long currentTickOffsetMillis() {
        return currentTimeMillis() - System.currentTimeMillis();
    }
}
