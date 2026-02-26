package com.gamesofts.osstimeagent.bridge;

import com.gamesofts.osstimeagent.time.OssEndpointTimeSync;
import com.gamesofts.osstimeagent.time.RealTimeClock;
import com.gamesofts.osstimeagent.util.AgentLog;

import java.net.URI;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class OssTimeBridge {
    interface EndpointTimeSyncer {
        OssEndpointTimeSync.SyncResult sync(URI endpoint, RealTimeClock clock) throws Exception;
    }

    private static volatile RealTimeClock clock = new RealTimeClock();
    private static volatile boolean resignRetryWarned;
    private static volatile long lastConfigTickOffsetLogged = Long.MIN_VALUE;
    private static volatile EndpointTimeSyncer endpointTimeSyncer = new EndpointTimeSyncer() {
        public OssEndpointTimeSync.SyncResult sync(URI endpoint, RealTimeClock c) throws Exception {
            return new OssEndpointTimeSync().sync(endpoint, c);
        }
    };
    private static final AtomicBoolean authoritativeClockReady = new AtomicBoolean(false);
    private static final AtomicBoolean globalPreSyncSucceeded = new AtomicBoolean(false);
    private static final AtomicBoolean globalPreSyncInFlight = new AtomicBoolean(false);
    private static final Object NULL_REFLECTION = new Object();
    private static final Map signMethodCache = new ConcurrentHashMap();
    private static final Map signerParamsFieldCache = new ConcurrentHashMap();
    private static final Map signerParamsSetTickOffsetMethodCache = new ConcurrentHashMap();
    private static final Set unsupportedSignerParamsClasses =
            Collections.newSetFromMap(new ConcurrentHashMap());
    private static final ThreadLocal suppressSdkTickOffsetHook = new ThreadLocal();

    private OssTimeBridge() {
    }

    public static void installClock(RealTimeClock c) {
        if (c != null) {
            clock = c;
        }
    }

    public static long currentTimeMillis() {
        RealTimeClock c = clock;
        if (c == null) {
            return System.currentTimeMillis();
        }
        return c.currentTimeMillis();
    }

    public static long currentTickOffsetMillis() {
        RealTimeClock c = clock;
        if (c == null) {
            return 0L;
        }
        return c.currentTickOffsetMillis();
    }

    public static long resolveTickOffsetMillis(long sdkTickOffset) {
        if (!authoritativeClockReady.get()) {
            return sdkTickOffset;
        }
        return currentTickOffsetMillis();
    }

    public static void beforeInitialSign(Object serviceClient, Object requestMessage, Object executionContext) {
        if (requestMessage == null) {
            return;
        }
        URI endpoint;
        String endpointKey;
        try {
            endpoint = getRequestEndpoint(requestMessage);
            if (endpoint == null) {
                return;
            }
            endpointKey = endpointKey(endpoint);
            if (endpointKey == null || endpointKey.length() == 0) {
                return;
            }
        } catch (Throwable t) {
            AgentLog.debug("OSS pre-sync skipped: " + t.toString());
            return;
        }
        if (globalPreSyncSucceeded.get()) {
            return;
        }
        if (!globalPreSyncInFlight.compareAndSet(false, true)) {
            return;
        }

        try {
            EndpointTimeSyncer syncer = endpointTimeSyncer;
            RealTimeClock c = clock;
            OssEndpointTimeSync.SyncResult result = (syncer == null || c == null) ? null : syncer.sync(endpoint, c);
            if (result != null && result.isSuccess()) {
                long syncedNow = result.getEstimatedServerMillis();
                long tickOffset = currentTickOffsetMillis();
                boolean appliedToSdk = applyPreSyncTickOffset(serviceClient, executionContext, syncedNow, tickOffset);
                authoritativeClockReady.set(true);
                globalPreSyncSucceeded.set(true);
                if (appliedToSdk) {
                    onConfigTickOffsetUpdatedFromPreSync(tickOffset);
                }
                AgentLog.info("OSS endpoint pre-sync success: " + endpointKey
                        + ", tickOffset=" + tickOffset + "ms"
                        + ", time=" + syncedNow
                        + " (" + formatUtcTime(syncedNow) + ")"
                        + ", insecureHttps=" + result.isInsecureHttpsUsed()
                        + ", appliedToSdk=" + appliedToSdk
                        + (result.getMethodUsed() == null ? "" : ", method=" + result.getMethodUsed()));
            } else {
                String reason = result == null ? "sync result missing"
                        : (result.getFailureReason() == null ? "missing Date header or unsupported response"
                        : result.getFailureReason());
                logPreSyncFailure(endpointKey, reason, isPreSyncInsecureHttpsUsed(endpoint, result));
            }
        } catch (Throwable t) {
            logPreSyncFailure(endpointKey, t.toString(), false);
        } finally {
            globalPreSyncInFlight.set(false);
        }
    }

    public static void resignForRetry(Object serviceClient, Object requestMessage, Object executionContext, int retries) {
        if (retries <= 0 || serviceClient == null || requestMessage == null || executionContext == null) {
            return;
        }
        try {
            long currentOffset = getConfigTickOffset(serviceClient);
            long resolvedOffset = resolveTickOffsetMillis(currentOffset);
            AgentLog.debug("resign retry with configTickOffset=" + currentOffset + "ms"
                    + ", resolvedTickOffset=" + resolvedOffset + "ms, retries=" + retries);

            syncSignerTickOffset(executionContext, resolvedOffset);

            clearSignatureHeaders(requestMessage);

            Method isUseUrlSignature = requestMessage.getClass().getMethod("isUseUrlSignature", new Class[0]);
            boolean useUrlSignature = Boolean.TRUE.equals(isUseUrlSignature.invoke(requestMessage, new Object[0]));

            Method getSigner = executionContext.getClass().getMethod("getSigner", new Class[0]);
            Object signer = getSigner.invoke(executionContext, new Object[0]);
            if (signer != null && !useUrlSignature) {
                invokeSigner(signer, requestMessage);
            }

            Method getSignerHandlers = executionContext.getClass().getMethod("getSignerHandlers", new Class[0]);
            Object handlersObj = getSignerHandlers.invoke(executionContext, new Object[0]);
            if (handlersObj instanceof List) {
                List handlers = (List) handlersObj;
                for (Iterator it = handlers.iterator(); it.hasNext();) {
                    Object handler = it.next();
                    if (handler != null) {
                        invokeSigner(handler, requestMessage);
                    }
                }
            }
        } catch (Throwable t) {
            if (!resignRetryWarned) {
                resignRetryWarned = true;
                AgentLog.warn("failed to re-sign OSS request before retry; fallback to SDK behavior", t);
            } else {
                AgentLog.debug("failed to re-sign OSS request before retry: " + t.toString());
            }
        }
    }

    public static void onConfigTickOffsetUpdatedFromSdk(long offset) {
        Object suppressed = suppressSdkTickOffsetHook.get();
        if (Boolean.TRUE.equals(suppressed)) {
            return;
        }
        logConfigTickOffset("ServerTime", offset);
    }

    public static void onConfigTickOffsetUpdatedFromPreSync(long offset) {
        logConfigTickOffset("pre-sync", offset);
    }

    public static void onConfigTickOffsetUpdated(long offset) {
        onConfigTickOffsetUpdatedFromSdk(offset);
    }

    private static void logConfigTickOffset(String source, long offset) {
        if (lastConfigTickOffsetLogged != offset) {
            lastConfigTickOffsetLogged = offset;
            AgentLog.info("OSS config tickOffset updated (from " + source + "): " + offset + "ms");
        }
    }

    static void installEndpointTimeSyncerForTest(EndpointTimeSyncer syncer) {
        endpointTimeSyncer = syncer;
    }

    static void resetPreSyncStateForTest() {
        authoritativeClockReady.set(false);
        globalPreSyncSucceeded.set(false);
        globalPreSyncInFlight.set(false);
    }

    private static void clearSignatureHeaders(Object requestMessage) throws Exception {
        Method getHeaders = requestMessage.getClass().getMethod("getHeaders", new Class[0]);
        Object headersObj = getHeaders.invoke(requestMessage, new Object[0]);
        if (!(headersObj instanceof Map)) {
            return;
        }
        Map headers = (Map) headersObj;
        removeHeader(headers, "Date");
        removeHeader(headers, "date");
        removeHeader(headers, "x-oss-date");
        removeHeader(headers, "X-OSS-DATE");
        removeHeader(headers, "Authorization");
        removeHeader(headers, "authorization");
        removeHeader(headers, "x-oss-content-sha256");
        removeHeader(headers, "X-OSS-CONTENT-SHA256");
    }

    private static boolean applyPreSyncTickOffset(Object serviceClient, Object executionContext,
                                                  long serverTimeMillis, long tickOffset) {
        boolean appliedConfig = false;
        boolean appliedSigner = false;

        if (serviceClient != null) {
            try {
                setConfigServerTimeMillis(serviceClient, serverTimeMillis);
                appliedConfig = true;
            } catch (Throwable t) {
                AgentLog.warn("failed to apply OSS pre-sync tickOffset to client config", t);
            }
        }

        if (executionContext != null) {
            try {
                syncSignerTickOffset(executionContext, tickOffset);
                appliedSigner = true;
            } catch (Throwable t) {
                AgentLog.debug("failed to sync signer tickOffset during OSS pre-sync: " + t.toString());
            }
        }

        return appliedConfig || appliedSigner;
    }

    private static void removeHeader(Map headers, String key) {
        if (headers != null) {
            headers.remove(key);
        }
    }

    private static void invokeSigner(Object signer, Object requestMessage) throws Exception {
        Method sign = getCachedSignMethod(signer.getClass());
        if (sign == null) {
            throw new NoSuchMethodException("sign(RequestMessage) not found on " + signer.getClass().getName());
        }
        sign.invoke(signer, new Object[] { requestMessage });
    }

    private static long getConfigTickOffset(Object serviceClient) throws Exception {
        Object config = null;
        try {
            Method getter = serviceClient.getClass().getMethod("getClientConfiguration", new Class[0]);
            config = getter.invoke(serviceClient, new Object[0]);
        } catch (NoSuchMethodException e) {
            Field f = findField(serviceClient.getClass(), "config");
            if (f == null) {
                throw e;
            }
            f.setAccessible(true);
            config = f.get(serviceClient);
        }
        if (config == null) {
            return 0L;
        }
        Method getTickOffset = config.getClass().getMethod("getTickOffset", new Class[0]);
        Object val = getTickOffset.invoke(config, new Object[0]);
        if (val instanceof Long) {
            return ((Long) val).longValue();
        }
        return 0L;
    }

    private static void setConfigServerTimeMillis(Object serviceClient, long serverTimeMillis) throws Exception {
        Object config = null;
        try {
            Method getter = serviceClient.getClass().getMethod("getClientConfiguration", new Class[0]);
            config = getter.invoke(serviceClient, new Object[0]);
        } catch (NoSuchMethodException e) {
            Field f = findField(serviceClient.getClass(), "config");
            if (f == null) {
                throw e;
            }
            f.setAccessible(true);
            config = f.get(serviceClient);
        }
        if (config == null) {
            return;
        }
        Method setter = config.getClass().getMethod("setTickOffset", new Class[] { Long.TYPE });
        suppressSdkTickOffsetHook.set(Boolean.TRUE);
        try {
            setter.invoke(config, new Object[] { new Long(serverTimeMillis) });
        } finally {
            suppressSdkTickOffsetHook.remove();
        }
    }

    private static void syncSignerTickOffset(Object executionContext, long currentOffset) throws Exception {
        Method getSigner = executionContext.getClass().getMethod("getSigner", new Class[0]);
        Object signer = getSigner.invoke(executionContext, new Object[0]);
        if (signer != null) {
            syncSingleSignerTickOffset(signer, currentOffset);
        }

        Method getSignerHandlers = executionContext.getClass().getMethod("getSignerHandlers", new Class[0]);
        Object handlersObj = getSignerHandlers.invoke(executionContext, new Object[0]);
        if (handlersObj instanceof List) {
            List handlers = (List) handlersObj;
            for (Iterator it = handlers.iterator(); it.hasNext();) {
                Object handler = it.next();
                if (handler != null) {
                    syncSingleSignerTickOffset(handler, currentOffset);
                }
            }
        }
    }

    private static void syncSingleSignerTickOffset(Object signer, long currentOffset) throws Exception {
        Class signerClass = signer.getClass();
        if (unsupportedSignerParamsClasses.contains(signerClass)) {
            return;
        }
        Field signerParamsField = getCachedSignerParamsField(signerClass);
        if (signerParamsField == null) {
            unsupportedSignerParamsClasses.add(signerClass);
            return;
        }
        signerParamsField.setAccessible(true);
        Object signerParams = signerParamsField.get(signer);
        if (signerParams == null) {
            return;
        }
        Method setTickOffset = getCachedSignerParamsSetTickOffsetMethod(signerParams.getClass());
        setTickOffset.invoke(signerParams, new Object[] { new Long(currentOffset) });
        AgentLog.debug("synced signerParams.tickOffset=" + currentOffset + "ms for " + signer.getClass().getName());
    }

    private static Field findField(Class clazz, String name) {
        Class c = clazz;
        while (c != null) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        return null;
    }

    private static Method getCachedSignMethod(Class clazz) {
        if (signMethodCache.containsKey(clazz)) {
            Object cached = signMethodCache.get(clazz);
            return cached == NULL_REFLECTION ? null : (Method) cached;
        }
        Method m = findSignMethod(clazz);
        signMethodCache.put(clazz, m == null ? NULL_REFLECTION : m);
        return m;
    }

    private static Field getCachedSignerParamsField(Class clazz) {
        if (signerParamsFieldCache.containsKey(clazz)) {
            Object cached = signerParamsFieldCache.get(clazz);
            return cached == NULL_REFLECTION ? null : (Field) cached;
        }
        Field f = findField(clazz, "signerParams");
        signerParamsFieldCache.put(clazz, f == null ? NULL_REFLECTION : f);
        return f;
    }

    private static Method getCachedSignerParamsSetTickOffsetMethod(Class clazz) throws Exception {
        if (signerParamsSetTickOffsetMethodCache.containsKey(clazz)) {
            return (Method) signerParamsSetTickOffsetMethodCache.get(clazz);
        }
        Method m = clazz.getMethod("setTickOffset", new Class[] { Long.TYPE });
        signerParamsSetTickOffsetMethodCache.put(clazz, m);
        return m;
    }

    private static Method findSignMethod(Class clazz) {
        Method[] methods = clazz.getMethods();
        int i;
        for (i = 0; i < methods.length; i++) {
            Method m = methods[i];
            if (!"sign".equals(m.getName())) {
                continue;
            }
            Class[] pts = m.getParameterTypes();
            if (pts != null && pts.length == 1) {
                return m;
            }
        }
        return null;
    }

    private static URI getRequestEndpoint(Object requestMessage) throws Exception {
        Method getEndpoint = requestMessage.getClass().getMethod("getEndpoint", new Class[0]);
        Object v = getEndpoint.invoke(requestMessage, new Object[0]);
        if (v instanceof URI) {
            return (URI) v;
        }
        return null;
    }

    private static String endpointKey(URI endpoint) {
        if (endpoint == null) {
            return null;
        }
        String scheme = endpoint.getScheme();
        String host = endpoint.getHost();
        int port = endpoint.getPort();
        if (scheme == null || host == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(scheme).append("://").append(host);
        if (port >= 0) {
            sb.append(':').append(port);
        }
        return sb.toString();
    }

    private static boolean isPreSyncInsecureHttpsUsed(URI endpoint, OssEndpointTimeSync.SyncResult result) {
        if (result != null && result.isSuccess()) {
            return result.isInsecureHttpsUsed();
        }
        if (endpoint == null) {
            return false;
        }
        return "https".equalsIgnoreCase(endpoint.getScheme());
    }

    private static void logPreSyncFailure(String endpointKey, String reason, boolean insecureHttps) {
        String msg = "OSS endpoint pre-sync failed: " + endpointKey + " (" + reason + ", insecureHttps="
                + insecureHttps + ")";
        AgentLog.warn(msg);
    }

    private static String formatUtcTime(long millis) {
        try {
            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            f.setTimeZone(TimeZone.getTimeZone("UTC"));
            return f.format(new Date(millis));
        } catch (Throwable t) {
            return String.valueOf(millis);
        }
    }
}
