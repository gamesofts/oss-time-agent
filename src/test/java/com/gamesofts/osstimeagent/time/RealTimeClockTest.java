package com.gamesofts.osstimeagent.time;

import org.junit.Assert;
import org.junit.Test;

public class RealTimeClockTest {
    @Test
    public void testMonotonicAfterBackwardUpdate() throws Exception {
        RealTimeClock clock = new RealTimeClock();
        long t1 = clock.currentTimeMillis();
        clock.updateBaseTime(t1 - 60000L);
        long t2 = clock.currentTimeMillis();
        Assert.assertTrue(t2 >= t1);
    }

    @Test
    public void testTickOffsetApproximatelyMatchesInjectedBase() {
        RealTimeClock clock = new RealTimeClock();
        long systemNow = System.currentTimeMillis();
        clock.updateBaseTime(systemNow + 5000L);
        long offset = clock.currentTickOffsetMillis();
        Assert.assertTrue(offset >= 4000L);
        Assert.assertTrue(offset <= 6000L);
    }

    @Test
    public void testAuthoritativeUpdateAllowsBackwardJump() {
        RealTimeClock clock = new RealTimeClock();
        long now = clock.currentTimeMillis();
        long target = now - 24L * 60L * 60L * 1000L;
        clock.updateBaseTimeAuthoritative(target);
        long actual = clock.currentTimeMillis();
        Assert.assertTrue(actual < now - (23L * 60L * 60L * 1000L));
        Assert.assertTrue(actual <= target + 1000L);
    }
}
