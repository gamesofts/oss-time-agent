package com.gamesofts.osstimeagent.time;

import com.gamesofts.osstimeagent.util.AgentLog;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public final class OssEndpointTimeSync {
    private static final int CONNECT_TIMEOUT_MS = 1000;
    private static final int READ_TIMEOUT_MS = 1000;
    private static final boolean PRESYNC_INSECURE_HTTPS = true;
    private static volatile ConnectionOpener connectionOpener = new DefaultConnectionOpener();
    private static volatile SSLSocketFactory insecureSslSocketFactory;
    private static final HostnameVerifier INSECURE_HOSTNAME_VERIFIER = new HostnameVerifier() {
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    };
    interface ConnectionOpener {
        HttpURLConnection open(URL url) throws IOException;
    }

    static final class DefaultConnectionOpener implements ConnectionOpener {
        public HttpURLConnection open(URL url) throws IOException {
            return (HttpURLConnection) url.openConnection();
        }
    }

    public static final class SyncResult {
        private final boolean success;
        private final long estimatedServerMillis;
        private final String methodUsed;
        private final boolean insecureHttpsUsed;
        private final String failureReason;

        private SyncResult(boolean success, long estimatedServerMillis, String methodUsed,
                           boolean insecureHttpsUsed, String failureReason) {
            this.success = success;
            this.estimatedServerMillis = estimatedServerMillis;
            this.methodUsed = methodUsed;
            this.insecureHttpsUsed = insecureHttpsUsed;
            this.failureReason = failureReason;
        }

        public static SyncResult success(long estimatedServerMillis, String methodUsed) {
            return new SyncResult(true, estimatedServerMillis, methodUsed, false, null);
        }

        public static SyncResult success(long estimatedServerMillis, String methodUsed, boolean insecureHttpsUsed) {
            return new SyncResult(true, estimatedServerMillis, methodUsed, insecureHttpsUsed, null);
        }

        public static SyncResult failed() {
            return new SyncResult(false, 0L, null, false, null);
        }

        public static SyncResult failed(String failureReason) {
            return new SyncResult(false, 0L, null, false, failureReason);
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

        public boolean isInsecureHttpsUsed() {
            return insecureHttpsUsed;
        }

        public String getFailureReason() {
            return failureReason;
        }
    }

    public OssEndpointTimeSync() {
    }

    public SyncResult sync(URI endpoint, RealTimeClock clock) throws IOException {
        if (endpoint == null || clock == null) {
            return SyncResult.failed();
        }
        URL baseUrl = endpoint.toURL();
        SyncResult head;
        try {
            head = trySync(baseUrl, "HEAD", clock);
        } catch (IOException e) {
            AgentLog.debug("OSS pre-sync HEAD failed: " + e.toString());
            head = SyncResult.failed("HEAD " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        if (head.isSuccess()) {
            return head;
        }
        try {
            SyncResult get = trySync(baseUrl, "GET", clock);
            if (get.isSuccess()) {
                return get;
            }
            String reason = mergeFailureReasons(head.getFailureReason(), get.getFailureReason());
            return reason == null ? get : SyncResult.failed(reason);
        } catch (IOException e) {
            AgentLog.debug("OSS pre-sync GET failed: " + e.toString());
            String tail = "GET " + e.getClass().getSimpleName() + ": " + e.getMessage();
            return SyncResult.failed(mergeFailureReasons(head.getFailureReason(), tail));
        }
    }

    private SyncResult trySync(URL endpointUrl, String method, RealTimeClock clock) throws IOException {
        HttpURLConnection conn = null;
        long t0 = System.currentTimeMillis();
        long t1 = t0;
        try {
            conn = openConnection(endpointUrl);
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
                return SyncResult.failed("missing Date header");
            }
            long estimated = serverMillis + ((t1 - t0) / 2L);
            clock.updateBaseTimeAuthoritative(estimated);
            return SyncResult.success(estimated, method, isInsecureHttpsApplied(conn));
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private HttpURLConnection openConnection(URL endpointUrl) throws IOException {
        HttpURLConnection conn = connectionOpener.open(endpointUrl);
        if (conn instanceof HttpsURLConnection && PRESYNC_INSECURE_HTTPS) {
            HttpsURLConnection https = (HttpsURLConnection) conn;
            try {
                https.setSSLSocketFactory(getOrCreateInsecureSslSocketFactory());
                https.setHostnameVerifier(INSECURE_HOSTNAME_VERIFIER);
            } catch (Exception e) {
                throw new IOException("failed to initialize insecure HTTPS for OSS pre-sync", e);
            }
        }
        return conn;
    }

    private boolean isInsecureHttpsApplied(HttpURLConnection conn) {
        return conn instanceof HttpsURLConnection && PRESYNC_INSECURE_HTTPS;
    }

    private static String mergeFailureReasons(String a, String b) {
        if (a == null || a.length() == 0) {
            return b;
        }
        if (b == null || b.length() == 0) {
            return a;
        }
        return a + "; " + b;
    }

    private static SSLSocketFactory getOrCreateInsecureSslSocketFactory() throws Exception {
        SSLSocketFactory f = insecureSslSocketFactory;
        if (f != null) {
            return f;
        }
        synchronized (OssEndpointTimeSync.class) {
            f = insecureSslSocketFactory;
            if (f != null) {
                return f;
            }
            TrustManager[] trustAll = new TrustManager[] {
                    new X509TrustManager() {
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {
                        }

                        public void checkServerTrusted(X509Certificate[] chain, String authType) {
                        }

                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
            };
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new SecureRandom());
            insecureSslSocketFactory = ctx.getSocketFactory();
            return insecureSslSocketFactory;
        }
    }

    static void installConnectionOpenerForTest(ConnectionOpener opener) {
        connectionOpener = opener == null ? new DefaultConnectionOpener() : opener;
    }

    static void resetConnectionOpenerForTest() {
        connectionOpener = new DefaultConnectionOpener();
    }
}
