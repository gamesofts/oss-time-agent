package com.gamesofts.osstimeagent;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectResult;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class OssSmokeMain {
    public static void main(String[] args) throws Exception {
        Map<String, String> cfg = loadSimpleJson(new File("config.json"));
        String endpoint = cfg.get("endpoint");
        String region = cfg.get("region");
        String bucket = cfg.get("bucket");
        String ak = cfg.get("accessKeyId");
        String sk = cfg.get("accessKeySecret");

        if (endpoint == null || bucket == null || ak == null || sk == null) {
            throw new IllegalArgumentException("config.json missing required keys");
        }

        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        OSS client = null;
        String key = "oss-time-agent-smoke-" + UUID.randomUUID().toString() + ".txt";
        try {
            DefaultCredentialProvider provider = CredentialsProviderFactory.newDefaultCredentialProvider(ak, sk);
            client = new OSSClientBuilder().build(endpoint, provider, conf);
            if (region != null && region.length() > 0) {
                System.out.println("region from config.json=" + region + " (not explicitly set on OSS client in this sample)");
            }

            ObjectListing listing = client.listObjects(bucket);
            System.out.println("list ok, count=" + listing.getObjectSummaries().size());

            PutObjectResult putResult = client.putObject(bucket, key, new ByteArrayInputStream("hello-oss-time-agent".getBytes("UTF-8")));
            System.out.println("put ok, etag=" + putResult.getETag());

            OSSObject obj = client.getObject(bucket, key);
            try {
                InputStream in = obj.getObjectContent();
                byte[] buf = new byte[64];
                int n = in.read(buf);
                System.out.println("get ok, bytes=" + n + ", first=" + new String(buf, 0, n, "UTF-8"));
                in.close();
            } finally {
                obj.close();
            }

            client.deleteObject(bucket, key);
            System.out.println("delete ok, key=" + key);
        } finally {
            if (client != null) {
                client.shutdown();
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
