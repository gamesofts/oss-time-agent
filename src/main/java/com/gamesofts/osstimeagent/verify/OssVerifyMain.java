package com.gamesofts.osstimeagent.verify;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.SimplifiedObjectMeta;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class OssVerifyMain {
    private static final int STREAM_BYTES = 256 * 1024;

    public static void main(String[] args) throws Exception {
        Map<String, String> cfg = loadSimpleJson(new File("config.json"));
        String endpoint = cfg.get("endpoint");
        String region = cfg.get("region");
        String bucket = cfg.get("bucket");
        String ak = cfg.get("accessKeyId");
        String sk = cfg.get("accessKeySecret");

        if (endpoint == null || bucket == null || ak == null || sk == null) {
            System.err.println("CONFIG_ERROR missing required keys in config.json");
            System.exit(2);
            return;
        }

        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        OSS client = null;
        String basePrefix = "oss-time-agent-verify-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString();
        String baseKey = basePrefix + "-base.txt";
        String streamKey = basePrefix + "-stream.bin";
        String copyKey = basePrefix + "-copy.txt";
        String payload = "oss-time-agent-verify-payload-" + System.currentTimeMillis();
        byte[] payloadBytes = payload.getBytes("UTF-8");
        byte[] streamBytes = buildPatternBytes(STREAM_BYTES);

        try {
            DefaultCredentialProvider provider = CredentialsProviderFactory.newDefaultCredentialProvider(ak, sk);
            client = new OSSClientBuilder().build(endpoint, provider, conf);

            if (region != null && region.length() > 0) {
                System.out.println("REGION_INFO " + region + " (not explicitly set on OSS client)");
            }

            ObjectListing listing = client.listObjects(bucket);
            System.out.println("LIST_OK count=" + listing.getObjectSummaries().size());

            PutObjectResult putResult = client.putObject(bucket, baseKey, new ByteArrayInputStream(payloadBytes));
            System.out.println("PUT_OK key=" + baseKey + " etag=" + putResult.getETag());

            verifySimplifiedMeta(client, bucket, baseKey, payloadBytes.length, putResult.getETag());
            verifyObjectExists(client, bucket, baseKey, true, false);
            verifyBaseObjectGet(client, bucket, baseKey, payload);

            verifyStreamPutAndGet(client, bucket, streamKey, streamBytes);

            CopyObjectResult copyResult = verifyCopy(client, bucket, baseKey, copyKey);
            verifyCopyMeta(client, bucket, copyKey, payloadBytes.length, copyResult.getETag());
            verifyCopiedObjectContent(client, bucket, copyKey, payload);

            client.deleteObject(bucket, baseKey);
            System.out.println("DELETE_OK key=" + baseKey);
            client.deleteObject(bucket, streamKey);
            System.out.println("DELETE_OK key=" + streamKey);
            client.deleteObject(bucket, copyKey);
            System.out.println("DELETE_OK key=" + copyKey);

            verifyObjectExists(client, bucket, baseKey, false, true);
            System.out.println("VERIFY_DONE");
        } catch (Throwable t) {
            System.err.println("VERIFY_FAILED " + t.getClass().getName() + ": " + t.getMessage());
            t.printStackTrace(System.err);
            if (client != null) {
                safeDeleteAll(client, bucket, new String[] { baseKey, streamKey, copyKey });
            }
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw new RuntimeException(t);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private static void verifyBaseObjectGet(OSS client, String bucket, String key, String expectedPayload) throws Exception {
        OSSObject obj = client.getObject(bucket, key);
        String downloaded;
        try {
            downloaded = readAllAsUtf8(obj.getObjectContent());
        } finally {
            obj.close();
        }
        if (!expectedPayload.equals(downloaded)) {
            throw new IllegalStateException("GET_MISMATCH key=" + key + " expected=" + expectedPayload + " actual=" + downloaded);
        }
        System.out.println("GET_OK key=" + key + " bytes=" + downloaded.getBytes("UTF-8").length);
    }

    private static void verifyObjectExists(OSS client, String bucket, String key, boolean expected, boolean afterDelete)
            throws Exception {
        boolean actual = client.doesObjectExist(bucket, key);
        if (actual != expected) {
            throw new IllegalStateException("EXISTS_MISMATCH key=" + key + " expected=" + expected + " actual=" + actual);
        }
        System.out.println("EXISTS_OK key=" + key + " exists=" + actual + (afterDelete ? " (after delete)" : ""));
    }

    private static void verifyStreamPutAndGet(OSS client, String bucket, String key, byte[] expectedBytes) throws Exception {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(expectedBytes.length);
        metadata.setContentType("application/octet-stream");
        PutObjectResult putResult = client.putObject(bucket, key, new ByteArrayInputStream(expectedBytes), metadata);
        if (putResult == null || putResult.getETag() == null || putResult.getETag().length() == 0) {
            throw new IllegalStateException("STREAM_MISMATCH key=" + key + " put etag empty");
        }
        System.out.println("STREAM_PUT_OK key=" + key + " bytes=" + expectedBytes.length + " etag=" + putResult.getETag());

        OSSObject obj = client.getObject(bucket, key);
        byte[] actualBytes;
        try {
            actualBytes = readAllBytes(obj.getObjectContent());
        } finally {
            obj.close();
        }

        assertBytesEqual("STREAM_MISMATCH key=" + key, expectedBytes, actualBytes);
        System.out.println("STREAM_GET_OK key=" + key + " bytes=" + actualBytes.length);
    }

    private static CopyObjectResult verifyCopy(OSS client, String bucket, String srcKey, String dstKey) throws Exception {
        CopyObjectResult result = client.copyObject(bucket, srcKey, bucket, dstKey);
        if (result == null) {
            throw new IllegalStateException("COPY_MISMATCH key=" + dstKey + " result=null");
        }
        if (result.getETag() == null || result.getETag().length() == 0) {
            throw new IllegalStateException("COPY_MISMATCH key=" + dstKey + " etag=empty");
        }
        Date lastModified = result.getLastModified();
        if (lastModified == null) {
            throw new IllegalStateException("COPY_MISMATCH key=" + dstKey + " lastModified=null");
        }
        System.out.println("COPY_OK key=" + dstKey + " etag=" + result.getETag());
        return result;
    }

    private static void verifyCopyMeta(OSS client, String bucket, String key, int expectedSize, String expectedEtag)
            throws Exception {
        SimplifiedObjectMeta meta = client.getSimplifiedObjectMeta(bucket, key);
        if (meta == null) {
            throw new IllegalStateException("COPY_MISMATCH key=" + key + " meta=null");
        }
        if (meta.getSize() != expectedSize) {
            throw new IllegalStateException("COPY_MISMATCH key=" + key + " size=" + meta.getSize()
                    + " expected=" + expectedSize);
        }
        String etag = meta.getETag();
        if (etag == null || etag.length() == 0) {
            throw new IllegalStateException("COPY_MISMATCH key=" + key + " etag=empty");
        }
        if (expectedEtag != null && expectedEtag.length() > 0 && !expectedEtag.equalsIgnoreCase(etag)) {
            throw new IllegalStateException("COPY_MISMATCH key=" + key + " etag=" + etag
                    + " expected=" + expectedEtag);
        }
        if (meta.getLastModified() == null) {
            throw new IllegalStateException("COPY_MISMATCH key=" + key + " lastModified=null");
        }
        System.out.println("COPY_META_OK key=" + key + " size=" + meta.getSize() + " etag=" + etag);
    }

    private static void verifyCopiedObjectContent(OSS client, String bucket, String key, String expectedPayload) throws Exception {
        OSSObject obj = client.getObject(bucket, key);
        String downloaded;
        try {
            downloaded = readAllAsUtf8(obj.getObjectContent());
        } finally {
            obj.close();
        }
        if (!expectedPayload.equals(downloaded)) {
            throw new IllegalStateException("COPY_MISMATCH key=" + key + " expected=" + expectedPayload + " actual=" + downloaded);
        }
        System.out.println("COPY_GET_OK key=" + key + " bytes=" + downloaded.getBytes("UTF-8").length);
    }

    private static void safeDelete(OSS client, String bucket, String key) {
        try {
            client.deleteObject(bucket, key);
            System.out.println("DELETE_OK key=" + key + " (cleanup)");
        } catch (Throwable t) {
            System.err.println("DELETE_FAILED key=" + key + " " + t.toString());
        }
    }

    private static void safeDeleteAll(OSS client, String bucket, String[] keys) {
        if (keys == null) {
            return;
        }
        int i;
        for (i = 0; i < keys.length; i++) {
            String key = keys[i];
            if (key == null || key.length() == 0) {
                continue;
            }
            safeDelete(client, bucket, key);
        }
    }

    private static void verifySimplifiedMeta(OSS client, String bucket, String key, int expectedSize, String expectedEtag)
            throws Exception {
        SimplifiedObjectMeta meta = client.getSimplifiedObjectMeta(bucket, key);
        if (meta == null) {
            throw new IllegalStateException("META_MISMATCH key=" + key + " meta=null");
        }
        if (meta.getSize() != expectedSize) {
            throw new IllegalStateException("META_MISMATCH key=" + key + " size=" + meta.getSize()
                    + " expected=" + expectedSize);
        }
        String etag = meta.getETag();
        if (etag == null || etag.length() == 0) {
            throw new IllegalStateException("META_MISMATCH key=" + key + " etag=empty");
        }
        if (expectedEtag != null && expectedEtag.length() > 0 && !expectedEtag.equalsIgnoreCase(etag)) {
            throw new IllegalStateException("META_MISMATCH key=" + key + " etag=" + etag
                    + " expected=" + expectedEtag);
        }
        if (meta.getLastModified() == null) {
            throw new IllegalStateException("META_MISMATCH key=" + key + " lastModified=null");
        }
        System.out.println("SIMPLIFIED_META_OK key=" + key + " size=" + meta.getSize() + " etag=" + etag);
    }

    private static byte[] buildPatternBytes(int size) {
        byte[] data = new byte[size];
        int i;
        for (i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 251);
        }
        return data;
    }

    private static void assertBytesEqual(String prefix, byte[] expected, byte[] actual) {
        if (expected == null || actual == null) {
            throw new IllegalStateException(prefix + " null-bytes expected=" + (expected == null)
                    + " actual=" + (actual == null));
        }
        if (expected.length != actual.length) {
            throw new IllegalStateException(prefix + " size=" + actual.length + " expected=" + expected.length);
        }
        int i;
        for (i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                throw new IllegalStateException(prefix + " firstDiffIndex=" + i);
            }
        }
    }

    private static byte[] readAllBytes(InputStream in) throws Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            while (true) {
                int n = in.read(buf);
                if (n < 0) {
                    break;
                }
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    private static String readAllAsUtf8(InputStream in) throws Exception {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            while (true) {
                int n = in.read(buf);
                if (n < 0) {
                    break;
                }
                out.write(buf, 0, n);
            }
            return new String(out.toByteArray(), "UTF-8");
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    private static Map<String, String> loadSimpleJson(File file) throws Exception {
        Map<String, String> out = new HashMap<String, String>();
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        try {
            byte[] data = new byte[(int) file.length()];
            int off = 0;
            while (off < data.length) {
                int n = in.read(data, off, data.length - off);
                if (n < 0) {
                    break;
                }
                off += n;
            }
            String s = new String(data, 0, off, "UTF-8");
            extract(s, out, "endpoint");
            extract(s, out, "region");
            extract(s, out, "accessKeyId");
            extract(s, out, "accessKeySecret");
            extract(s, out, "bucket");
            return out;
        } finally {
            in.close();
        }
    }

    private static void extract(String s, Map<String, String> out, String key) {
        String k = "\"" + key + "\"";
        int p = s.indexOf(k);
        if (p < 0) {
            return;
        }
        int c = s.indexOf(':', p + k.length());
        if (c < 0) {
            return;
        }
        int q1 = s.indexOf('"', c + 1);
        if (q1 < 0) {
            return;
        }
        int q2 = s.indexOf('"', q1 + 1);
        if (q2 < 0) {
            return;
        }
        out.put(key, s.substring(q1 + 1, q2));
    }
}
