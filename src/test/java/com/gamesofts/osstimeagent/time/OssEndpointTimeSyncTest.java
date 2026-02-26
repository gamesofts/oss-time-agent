package com.gamesofts.osstimeagent.time;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.security.Principal;
import java.security.cert.Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocketFactory;

public class OssEndpointTimeSyncTest {
    @After
    public void tearDown() {
        OssEndpointTimeSync.resetConnectionOpenerForTest();
    }

    @Test
    public void testSyncAccepts404WithDateHeader() throws Exception {
        final long serverTime = System.currentTimeMillis() + 5000L;
        TestHttpServer server = new TestHttpServer(new Responder[] {
                new Responder() {
                    public void respond(String method, OutputStream out) throws Exception {
                        writeResponse(out, 404, httpDate(serverTime));
                    }
                }
        });
        server.start();
        try {
            RealTimeClock clock = new RealTimeClock();
            OssEndpointTimeSync.SyncResult result = new OssEndpointTimeSync().sync(new URI(server.url()), clock);
            Assert.assertTrue(result.isSuccess());
            Assert.assertEquals("HEAD", result.getMethodUsed());
            Assert.assertTrue(result.getEstimatedServerMillis() > 0L);
            long offset = clock.currentTickOffsetMillis();
            Assert.assertTrue(offset > 2000L);
        } finally {
            server.close();
        }
    }

    @Test
    public void testSyncFailsWhenDateHeaderMissing() throws Exception {
        TestHttpServer server = new TestHttpServer(new Responder[] {
                new Responder() {
                    public void respond(String method, OutputStream out) throws Exception {
                        writeResponse(out, 404, null);
                    }
                },
                new Responder() {
                    public void respond(String method, OutputStream out) throws Exception {
                        writeResponse(out, 404, null);
                    }
                }
        });
        server.start();
        try {
            RealTimeClock clock = new RealTimeClock();
            OssEndpointTimeSync.SyncResult result = new OssEndpointTimeSync().sync(new URI(server.url()), clock);
            Assert.assertFalse(result.isSuccess());
        } finally {
            server.close();
        }
    }

    @Test
    public void testSyncFallsBackToGetWhenHeadHasNoDate() throws Exception {
        final AtomicInteger requestCount = new AtomicInteger();
        final long serverTime = System.currentTimeMillis() + 4000L;
        TestHttpServer server = new TestHttpServer(new Responder[] {
                new Responder() {
                    public void respond(String method, OutputStream out) throws Exception {
                        requestCount.incrementAndGet();
                        Assert.assertEquals("HEAD", method);
                        writeResponse(out, 405, null);
                    }
                },
                new Responder() {
                    public void respond(String method, OutputStream out) throws Exception {
                        requestCount.incrementAndGet();
                        Assert.assertEquals("GET", method);
                        writeResponse(out, 404, httpDate(serverTime));
                    }
                }
        });
        server.start();
        try {
            RealTimeClock clock = new RealTimeClock();
            OssEndpointTimeSync.SyncResult result = new OssEndpointTimeSync().sync(new URI(server.url()), clock);
            Assert.assertTrue(result.isSuccess());
            Assert.assertEquals("GET", result.getMethodUsed());
            Assert.assertEquals(2, requestCount.get());
        } finally {
            server.close();
        }
    }

    @Test
    public void testSyncUsesInsecureHttpsByDefault() throws Exception {
        final long serverTime = System.currentTimeMillis() + 5000L;
        final FakeHttpsURLConnection conn = new FakeHttpsURLConnection(new URL("https://example.com/"), serverTime, false);
        OssEndpointTimeSync.installConnectionOpenerForTest(new OssEndpointTimeSync.ConnectionOpener() {
            public java.net.HttpURLConnection open(URL url) {
                return conn;
            }
        });

        RealTimeClock clock = new RealTimeClock();
        OssEndpointTimeSync.SyncResult result = new OssEndpointTimeSync().sync(new URI("https://example.com/"), clock);

        Assert.assertTrue(result.isSuccess());
        Assert.assertEquals("HEAD", result.getMethodUsed());
        Assert.assertTrue(result.isInsecureHttpsUsed());
        Assert.assertTrue(conn.sslSocketFactorySet);
        Assert.assertTrue(conn.hostnameVerifierSet);
    }

    private static String httpDate(long millis) {
        SimpleDateFormat f = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        return f.format(new Date(millis));
    }

    private static void writeResponse(OutputStream out, int status, String dateHeader) throws Exception {
        String reason = status == 404 ? "Not Found" : (status == 405 ? "Method Not Allowed" : "OK");
        StringBuilder sb = new StringBuilder();
        sb.append("HTTP/1.1 ").append(status).append(' ').append(reason).append("\r\n");
        if (dateHeader != null) {
            sb.append("Date: ").append(dateHeader).append("\r\n");
        }
        sb.append("Content-Length: 0\r\n");
        sb.append("Connection: close\r\n");
        sb.append("\r\n");
        out.write(sb.toString().getBytes("ISO-8859-1"));
        out.flush();
    }

    interface Responder {
        void respond(String method, OutputStream out) throws Exception;
    }

    static final class TestHttpServer {
        private final Responder[] responders;
        private final AtomicInteger index = new AtomicInteger();
        private final CountDownLatch ready = new CountDownLatch(1);
        private ServerSocket serverSocket;
        private Thread thread;
        private volatile boolean closed;

        TestHttpServer(Responder[] responders) {
            this.responders = responders;
        }

        void start() throws Exception {
            serverSocket = new ServerSocket(0);
            thread = new Thread(new Runnable() {
                public void run() {
                    ready.countDown();
                    while (!closed) {
                        Socket socket = null;
                        try {
                            socket = serverSocket.accept();
                            handle(socket);
                        } catch (Throwable ignore) {
                            if (closed) {
                                return;
                            }
                        } finally {
                            if (socket != null) {
                                try {
                                    socket.close();
                                } catch (Throwable ignore) {
                                }
                            }
                        }
                    }
                }
            }, "oss-endpoint-time-sync-test");
            thread.setDaemon(true);
            thread.start();
            ready.await();
        }

        String url() {
            return "http://127.0.0.1:" + serverSocket.getLocalPort() + "/";
        }

        void close() throws Exception {
            closed = true;
            if (serverSocket != null) {
                serverSocket.close();
            }
            if (thread != null) {
                thread.join(1000L);
            }
        }

        private void handle(Socket socket) throws Exception {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), "ISO-8859-1"));
            String line = in.readLine();
            if (line == null) {
                return;
            }
            String method = line.split(" ")[0];
            while (true) {
                String h = in.readLine();
                if (h == null || h.length() == 0) {
                    break;
                }
            }
            int i = index.getAndIncrement();
            Responder responder = responders[i < responders.length ? i : responders.length - 1];
            responder.respond(method, socket.getOutputStream());
        }
    }

    static final class FakeHttpsURLConnection extends HttpsURLConnection {
        private final long serverTimeMillis;
        private final boolean failOnConnect;
        boolean sslSocketFactorySet;
        boolean hostnameVerifierSet;

        FakeHttpsURLConnection(URL u, long serverTimeMillis, boolean failOnConnect) {
            super(u);
            this.serverTimeMillis = serverTimeMillis;
            this.failOnConnect = failOnConnect;
        }

        public void connect() throws java.io.IOException {
            if (failOnConnect && !(sslSocketFactorySet && hostnameVerifierSet)) {
                throw new SSLHandshakeException("self signed certificate in certificate chain");
            }
            connected = true;
        }

        public void disconnect() {
            connected = false;
        }

        public boolean usingProxy() {
            return false;
        }

        public int getResponseCode() {
            return 403;
        }

        public long getHeaderFieldDate(String name, long Default) {
            if ("Date".equalsIgnoreCase(name)) {
                return serverTimeMillis;
            }
            return Default;
        }

        public void setSSLSocketFactory(SSLSocketFactory sf) {
            super.setSSLSocketFactory(sf);
            sslSocketFactorySet = (sf != null);
        }

        public void setHostnameVerifier(HostnameVerifier v) {
            super.setHostnameVerifier(v);
            hostnameVerifierSet = (v != null);
        }

        public String getCipherSuite() {
            return "TLS_FAKE";
        }

        public Certificate[] getLocalCertificates() {
            return null;
        }

        public Certificate[] getServerCertificates() {
            return null;
        }

        public Principal getPeerPrincipal() {
            return null;
        }

        public Principal getLocalPrincipal() {
            return null;
        }
    }
}
