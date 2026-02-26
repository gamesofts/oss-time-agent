package com.gamesofts.osstimeagent.time;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

public final class OssEndpointTimeSync {
    private static final int CONNECT_TIMEOUT_MS = 1000;
    private static final int READ_TIMEOUT_MS = 1000;

    public static final class SyncResult {
        private final boolean success;
        private final long estimatedServerMillis;
        private final String methodUsed;

        private SyncResult(boolean success, long estimatedServerMillis, String methodUsed) {
            this.success = success;
            this.estimatedServerMillis = estimatedServerMillis;
            this.methodUsed = methodUsed;
        }

        public static SyncResult success(long estimatedServerMillis, String methodUsed) {
            return new SyncResult(true, estimatedServerMillis, methodUsed);
        }

        public static SyncResult failed() {
            return new SyncResult(false, 0L, null);
        }

        public boolean isSuccess() {
            return success;
        }

        public long getEstimatedServerMillis() {
            return estimatedServerMillis;
        }

        public String getMethodUsed() {
            return methodUsed;
        }
    }

    public SyncResult sync(URI endpoint, RealTimeClock clock) throws IOException {
        if (endpoint == null || clock == null) {
            return SyncResult.failed();
        }
        URL baseUrl = endpoint.toURL();
        SyncResult head = trySync(baseUrl, "HEAD", clock);
        if (head.isSuccess()) {
            return head;
        }
        return trySync(baseUrl, "GET", clock);
    }

    private SyncResult trySync(URL endpointUrl, String method, RealTimeClock clock) throws IOException {
        HttpURLConnection conn = null;
        long t0 = System.currentTimeMillis();
        long t1 = t0;
        try {
            conn = (HttpURLConnection) endpointUrl.openConnection();
            conn.setRequestMethod(method);
            conn.setInstanceFollowRedirects(false);
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.connect();

            // Force headers to be available; any status code is acceptable if Date exists.
            conn.getResponseCode();
            t1 = System.currentTimeMillis();

            long serverMillis = conn.getHeaderFieldDate("Date", -1L);
            if (serverMillis <= 0L) {
                return SyncResult.failed();
            }
            long estimated = serverMillis + ((t1 - t0) / 2L);
            clock.updateBaseTimeAuthoritative(estimated);
            return SyncResult.success(estimated, method);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
