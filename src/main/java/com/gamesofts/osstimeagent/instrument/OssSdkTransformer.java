package com.gamesofts.osstimeagent.instrument;

import com.gamesofts.osstimeagent.instrument.asm.OssAsmPatcher;
import com.gamesofts.osstimeagent.instrument.asm.OssAsmPatcher.PatchStats;
import com.gamesofts.osstimeagent.util.AgentLog;

import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class OssSdkTransformer implements ClassFileTransformer {
    private static final Set<String> TARGET_CLASS_NAMES;

    static {
        Set<String> s = new HashSet<String>();
        s.add("com/aliyun/oss/internal/OSSOperation");
        s.add("com/aliyun/oss/common/comm/ServiceClient");
        s.add("com/aliyun/oss/ClientConfiguration");
        TARGET_CLASS_NAMES = Collections.unmodifiableSet(s);
    }

    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
                            ProtectionDomain protectionDomain, byte[] classfileBuffer) {
        if (className == null || classfileBuffer == null) {
            return null;
        }
        if (!TARGET_CLASS_NAMES.contains(className)) {
            return null;
        }
        try {
            PatchStats stats = new PatchStats();
            return OssAsmPatcher.patch(className, classfileBuffer, stats);
        } catch (Throwable t) {
            AgentLog.warn("failed to patch class " + className.replace('/', '.'), t);
            return null;
        }
    }

    public Set<String> getTargetClassNames() {
        return TARGET_CLASS_NAMES;
    }
}
