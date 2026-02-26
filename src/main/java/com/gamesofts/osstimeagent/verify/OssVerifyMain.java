package com.gamesofts.osstimeagent.verify;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectResult;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class OssVerifyMain {
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
        String key = "oss-time-agent-verify-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString() + ".txt";
        String payload = "oss-time-agent-verify-payload-" + System.currentTimeMillis();
        try {
            DefaultCredentialProvider provider = CredentialsProviderFactory.newDefaultCredentialProvider(ak, sk);
            client = new OSSClientBuilder().build(endpoint, provider, conf);

            if (region != null && region.length() > 0) {
                System.out.println("REGION_INFO " + region + " (not explicitly set on OSS client)");
            }

            ObjectListing listing = client.listObjects(bucket);
            System.out.println("LIST_OK count=" + listing.getObjectSummaries().size());

            PutObjectResult putResult = client.putObject(bucket, key,
                    new ByteArrayInputStream(payload.getBytes("UTF-8")));
            System.out.println("PUT_OK key=" + key + " etag=" + putResult.getETag());

            OSSObject obj = client.getObject(bucket, key);
            String downloaded;
            try {
                downloaded = readAllAsUtf8(obj.getObjectContent());
            } finally {
                obj.close();
            }

            if (!payload.equals(downloaded)) {
                System.err.println("GET_MISMATCH key=" + key + " expected=" + payload + " actual=" + downloaded);
                safeDelete(client, bucket, key);
                System.exit(3);
                return;
            }
            System.out.println("GET_OK key=" + key + " bytes=" + downloaded.getBytes("UTF-8").length);

            client.deleteObject(bucket, key);
            System.out.println("DELETE_OK key=" + key);
            System.out.println("VERIFY_DONE");
        } catch (Throwable t) {
            System.err.println("VERIFY_FAILED " + t.getClass().getName() + ": " + t.getMessage());
            t.printStackTrace(System.err);
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

    private static void safeDelete(OSS client, String bucket, String key) {
        try {
            client.deleteObject(bucket, key);
            System.out.println("DELETE_OK key=" + key + " (cleanup)");
        } catch (Throwable t) {
            System.err.println("DELETE_FAILED key=" + key + " " + t.toString());
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
