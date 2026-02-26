package com.gamesofts.osstimeagent.instrument.asm;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class OssAsmPatcherTest {
    @Test
    public void testPatchOssOperationClass() throws Exception {
        byte[] original = readAll("com/aliyun/oss/internal/OSSOperation.class");
        OssAsmPatcher.PatchStats stats = new OssAsmPatcher.PatchStats();
        byte[] patched = OssAsmPatcher.patch("com/aliyun/oss/internal/OSSOperation", original, stats);
        if (supportsOssOperationTickOffsetPatch()) {
            Assert.assertNotNull(patched);
            Assert.assertTrue(stats.classModified);
            Assert.assertTrue("tickOffset patch expected", stats.tickOffsetPatched);
        } else {
            Assert.assertNull("older 3.x may not expose tickOffset injection point", patched);
            Assert.assertFalse(stats.tickOffsetPatched);
        }
    }

    @Test
    public void testPatchServiceClientClass() throws Exception {
        byte[] original = readAll("com/aliyun/oss/common/comm/ServiceClient.class");
        OssAsmPatcher.PatchStats stats = new OssAsmPatcher.PatchStats();
        byte[] patched = OssAsmPatcher.patch("com/aliyun/oss/common/comm/ServiceClient", original, stats);
        Assert.assertNotNull(patched);
        Assert.assertTrue(stats.classModified);
        Assert.assertTrue("shouldRetry patch expected", stats.serviceClientRetryPatched);
        Assert.assertTrue("sendRequestImpl pre-sync-before-sign patch expected", stats.serviceClientPreSyncBeforeSignPatched);
        Assert.assertTrue("sendRequestImpl resign patch expected", stats.serviceClientResignRetryPatched);
    }

    @Test
    public void testPatchClientConfigurationClass() throws Exception {
        byte[] original = readAll("com/aliyun/oss/ClientConfiguration.class");
        OssAsmPatcher.PatchStats stats = new OssAsmPatcher.PatchStats();
        byte[] patched = OssAsmPatcher.patch("com/aliyun/oss/ClientConfiguration", original, stats);
        Assert.assertNotNull(patched);
        Assert.assertTrue(stats.classModified);
        Assert.assertFalse("ctor auto-correct patch not required for cross-version compatibility",
                stats.clientConfigClockSkewPatched);
        Assert.assertTrue("setTickOffset hook patch expected", stats.clientConfigTickOffsetHookPatched);
    }

    private void assertPatchable(String resource, String internalName) throws Exception {
        byte[] original = readAll(resource);
        OssAsmPatcher.PatchStats stats = new OssAsmPatcher.PatchStats();
        byte[] patched = OssAsmPatcher.patch(internalName, original, stats);
        Assert.assertNotNull("expected patched bytes for " + internalName, patched);
        Assert.assertTrue(patched.length > 0);
        Assert.assertTrue(stats.classModified);
    }

    private byte[] readAll(String resource) throws Exception {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (in == null) {
            throw new IllegalStateException("missing resource: " + resource);
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            for (;;) {
                int n = in.read(buf);
                if (n < 0) {
                    break;
                }
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } finally {
            in.close();
        }
    }

    private boolean supportsOssOperationTickOffsetPatch() {
        try {
            Class.forName("com.aliyun.oss.internal.signer.OSSSignerBase");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
