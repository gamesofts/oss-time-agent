package com.gamesofts.osstimeagent.bridge;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class OssTimeBridgeTest {
    @Test
    public void testResolveTickOffsetMillisFallsBackToSdkBeforeAuthoritativeSync() {
        OssTimeBridge.resetPreSyncStateForTest();

        Assert.assertEquals(1234L, OssTimeBridge.resolveTickOffsetMillis(1234L));
    }

    @Test
    public void testResignForRetrySyncsTickOffsetAndClearsHeaders() {
        OssTimeBridge.resetPreSyncStateForTest();

        FakeClientConfiguration cfg = new FakeClientConfiguration();
        cfg.tickOffset = 12345L;
        FakeServiceClient client = new FakeServiceClient(cfg);
        FakeRequestMessage req = new FakeRequestMessage();
        req.headers.put("Date", "old");
        req.headers.put("Authorization", "old");
        req.headers.put("x-oss-date", "old");
        req.headers.put("x-oss-content-sha256", "old");

        FakeSigner signer = new FakeSigner();
        FakeSigner handler = new FakeSigner();
        FakeExecutionContext ctx = new FakeExecutionContext();
        ctx.signer = signer;
        ctx.signerHandlers.add(handler);

        OssTimeBridge.resignForRetry(client, req, ctx, 1);

        Assert.assertEquals(12345L, signer.signerParams.tickOffset);
        Assert.assertEquals(12345L, handler.signerParams.tickOffset);
        Assert.assertEquals(1, signer.signCalls);
        Assert.assertEquals(1, handler.signCalls);
        Assert.assertFalse(req.headers.containsKey("Date"));
        Assert.assertFalse(req.headers.containsKey("Authorization"));
        Assert.assertEquals(Boolean.TRUE, req.signedMarker);
    }

    @Test
    public void testResolveTickOffsetMillisUsesDynamicOffsetAfterPreSyncSuccess() throws Exception {
        final com.gamesofts.osstimeagent.time.RealTimeClock clock = new com.gamesofts.osstimeagent.time.RealTimeClock();
        OssTimeBridge.installClock(clock);
        OssTimeBridge.installEndpointTimeSyncerForTest(new OssTimeBridge.EndpointTimeSyncer() {
            public com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult sync(URI endpoint, com.gamesofts.osstimeagent.time.RealTimeClock c) {
                long t = System.currentTimeMillis() + 5000L;
                c.updateBaseTimeAuthoritative(t);
                return com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult.success(t, "HEAD");
            }
        });
        OssTimeBridge.resetPreSyncStateForTest();

        FakeClientConfiguration cfg = new FakeClientConfiguration();
        FakeServiceClient client = new FakeServiceClient(cfg);
        FakeExecutionContext ctx = new FakeExecutionContext();
        ctx.signer = new FakeSigner();
        FakeRequestMessage req = new FakeRequestMessage();
        req.endpoint = new URI("https://oss-cn-shanghai.aliyuncs.com/");

        OssTimeBridge.beforeInitialSign(client, req, ctx);

        long expected = OssTimeBridge.currentTickOffsetMillis();
        long resolved = OssTimeBridge.resolveTickOffsetMillis(-777777L);

        Assert.assertTrue(Math.abs(resolved - expected) < 200L);
        Assert.assertTrue(resolved > 1000L);
        Assert.assertNotEquals(-777777L, resolved);
    }

    @Test
    public void testResignForRetryUsesDynamicOffsetAfterPreSyncSuccess() throws Exception {
        final long target = System.currentTimeMillis() + 8000L;
        final com.gamesofts.osstimeagent.time.RealTimeClock clock = new com.gamesofts.osstimeagent.time.RealTimeClock();
        OssTimeBridge.installClock(clock);
        OssTimeBridge.installEndpointTimeSyncerForTest(new OssTimeBridge.EndpointTimeSyncer() {
            public com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult sync(URI endpoint, com.gamesofts.osstimeagent.time.RealTimeClock c) {
                c.updateBaseTimeAuthoritative(target);
                return com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult.success(target, "HEAD");
            }
        });
        OssTimeBridge.resetPreSyncStateForTest();

        FakeClientConfiguration cfg = new FakeClientConfiguration();
        FakeServiceClient client = new FakeServiceClient(cfg);
        FakeExecutionContext ctx = new FakeExecutionContext();
        FakeSigner signer = new FakeSigner();
        ctx.signer = signer;
        FakeRequestMessage req = new FakeRequestMessage();
        req.endpoint = new URI("https://oss-cn-shanghai.aliyuncs.com/");

        OssTimeBridge.beforeInitialSign(client, req, ctx);

        cfg.tickOffset = 123L;
        req.headers.put("Date", "old");
        req.headers.put("Authorization", "old");

        OssTimeBridge.resignForRetry(client, req, ctx, 1);

        long expected = OssTimeBridge.currentTickOffsetMillis();
        Assert.assertTrue(Math.abs(signer.signerParams.tickOffset - expected) < 200L);
        Assert.assertNotEquals(123L, signer.signerParams.tickOffset);
    }

    @Test
    public void testBeforeInitialSignPreSyncOnlyOnceGloballyAfterSuccess() throws Exception {
        final AtomicInteger calls = new AtomicInteger();
        final com.gamesofts.osstimeagent.time.RealTimeClock clock = new com.gamesofts.osstimeagent.time.RealTimeClock();
        OssTimeBridge.installClock(clock);
        OssTimeBridge.installEndpointTimeSyncerForTest(new OssTimeBridge.EndpointTimeSyncer() {
            public com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult sync(URI endpoint, com.gamesofts.osstimeagent.time.RealTimeClock c) {
                calls.incrementAndGet();
                long t = System.currentTimeMillis() + 3000L;
                c.updateBaseTimeAuthoritative(t);
                return com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult.success(t, "HEAD");
            }
        });
        OssTimeBridge.resetPreSyncStateForTest();

        FakeClientConfiguration cfg = new FakeClientConfiguration();
        FakeServiceClient client = new FakeServiceClient(cfg);
        FakeExecutionContext ctx = new FakeExecutionContext();
        ctx.signer = new FakeSigner();
        ctx.signerHandlers.add(new FakeSigner());
        FakeRequestMessage req = new FakeRequestMessage();
        req.endpoint = new URI("https://oss-cn-hangzhou.aliyuncs.com/");

        OssTimeBridge.beforeInitialSign(client, req, ctx);
        req.endpoint = new URI("https://oss-cn-shanghai.aliyuncs.com/");
        OssTimeBridge.beforeInitialSign(client, req, ctx);
        OssTimeBridge.beforeInitialSign(client, req, ctx);

        Assert.assertEquals(1, calls.get());
        Assert.assertTrue(clock.currentTickOffsetMillis() > 0L);
        Assert.assertEquals(cfg.tickOffset, ctx.signer.signerParams.tickOffset);
        Assert.assertTrue(cfg.lastSetTickOffsetArg > System.currentTimeMillis());
    }

    @Test
    public void testBeforeInitialSignConcurrentPreSyncIsIdempotentGlobally() throws Exception {
        final AtomicInteger calls = new AtomicInteger();
        OssTimeBridge.installClock(new com.gamesofts.osstimeagent.time.RealTimeClock());
        OssTimeBridge.installEndpointTimeSyncerForTest(new OssTimeBridge.EndpointTimeSyncer() {
            public com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult sync(URI endpoint, com.gamesofts.osstimeagent.time.RealTimeClock c) throws Exception {
                calls.incrementAndGet();
                Thread.sleep(50L);
                long t = System.currentTimeMillis();
                c.updateBaseTimeAuthoritative(t);
                return com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult.success(t, "HEAD");
            }
        });
        OssTimeBridge.resetPreSyncStateForTest();

        final FakeClientConfiguration cfg = new FakeClientConfiguration();
        final FakeServiceClient client = new FakeServiceClient(cfg);
        final FakeExecutionContext ctx = new FakeExecutionContext();
        ctx.signer = new FakeSigner();
        final FakeRequestMessage req = new FakeRequestMessage();
        req.endpoint = new URI("https://oss-cn-shanghai.aliyuncs.com/");
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(4);

        int i;
        for (i = 0; i < 4; i++) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        start.await();
                        OssTimeBridge.beforeInitialSign(client, req, ctx);
                    } catch (Throwable ignore) {
                    } finally {
                        done.countDown();
                    }
                }
            }).start();
        }

        start.countDown();
        done.await();
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testBeforeInitialSignFailureCanRetryGlobally() throws Exception {
        final AtomicInteger calls = new AtomicInteger();
        OssTimeBridge.installClock(new com.gamesofts.osstimeagent.time.RealTimeClock());
        OssTimeBridge.installEndpointTimeSyncerForTest(new OssTimeBridge.EndpointTimeSyncer() {
            public com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult sync(URI endpoint, com.gamesofts.osstimeagent.time.RealTimeClock c) {
                calls.incrementAndGet();
                return com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult.failed();
            }
        });
        OssTimeBridge.resetPreSyncStateForTest();

        FakeRequestMessage req = new FakeRequestMessage();
        req.endpoint = new URI("https://oss-cn-qingdao.aliyuncs.com/");

        OssTimeBridge.beforeInitialSign(new Object(), req, new Object());
        OssTimeBridge.beforeInitialSign(new Object(), req, new Object());

        Assert.assertEquals(2, calls.get());
    }

    @Test
    public void testBeforeInitialSignPreSyncAppliesLargeBackwardTickOffsetToConfigAndSigner() throws Exception {
        final long now = System.currentTimeMillis();
        final long target = now - 24L * 60L * 60L * 1000L;
        OssTimeBridge.installClock(new com.gamesofts.osstimeagent.time.RealTimeClock());
        OssTimeBridge.installEndpointTimeSyncerForTest(new OssTimeBridge.EndpointTimeSyncer() {
            public com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult sync(URI endpoint, com.gamesofts.osstimeagent.time.RealTimeClock c) {
                c.updateBaseTimeAuthoritative(target);
                return com.gamesofts.osstimeagent.time.OssEndpointTimeSync.SyncResult.success(target, "HEAD");
            }
        });
        OssTimeBridge.resetPreSyncStateForTest();

        FakeClientConfiguration cfg = new FakeClientConfiguration();
        FakeServiceClient client = new FakeServiceClient(cfg);
        FakeExecutionContext ctx = new FakeExecutionContext();
        FakeSigner signer = new FakeSigner();
        FakeSigner handler = new FakeSigner();
        ctx.signer = signer;
        ctx.signerHandlers.add(handler);
        FakeRequestMessage req = new FakeRequestMessage();
        req.endpoint = new URI("https://oss-cn-beijing.aliyuncs.com/");

        OssTimeBridge.beforeInitialSign(client, req, ctx);

        Assert.assertTrue(cfg.tickOffset < -23L * 60L * 60L * 1000L);
        Assert.assertTrue(Math.abs(cfg.lastSetTickOffsetArg - target) < 2000L);
        Assert.assertEquals(cfg.tickOffset, signer.signerParams.tickOffset);
        Assert.assertEquals(cfg.tickOffset, handler.signerParams.tickOffset);
    }

    public static final class FakeServiceClient {
        private final FakeClientConfiguration config;
        public FakeServiceClient(FakeClientConfiguration config) { this.config = config; }
        public FakeClientConfiguration getClientConfiguration() { return config; }
    }

    public static final class FakeClientConfiguration {
        long tickOffset;
        long lastSetTickOffsetArg;
        public long getTickOffset() { return tickOffset; }
        public void setTickOffset(long serverTimeMillis) {
            this.lastSetTickOffsetArg = serverTimeMillis;
            this.tickOffset = serverTimeMillis - System.currentTimeMillis();
        }
    }

    public static final class FakeExecutionContext {
        FakeSigner signer;
        List signerHandlers = new ArrayList();
        public FakeSigner getSigner() { return signer; }
        public List getSignerHandlers() { return signerHandlers; }
    }

    public static final class FakeRequestMessage {
        Map headers = new HashMap();
        Boolean signedMarker;
        URI endpoint;
        public Map getHeaders() { return headers; }
        public boolean isUseUrlSignature() { return false; }
        public URI getEndpoint() { return endpoint; }
    }

    public static final class FakeSigner {
        public final FakeSignerParams signerParams = new FakeSignerParams();
        int signCalls;
        public void sign(Object request) {
            signCalls++;
            ((FakeRequestMessage) request).signedMarker = Boolean.TRUE;
        }
    }

    public static final class FakeSignerParams {
        long tickOffset;
        public void setTickOffset(long tickOffset) { this.tickOffset = tickOffset; }
    }

}
