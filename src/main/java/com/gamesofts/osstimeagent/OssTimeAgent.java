package com.gamesofts.osstimeagent;

import com.gamesofts.osstimeagent.bridge.OssTimeBridge;
import com.gamesofts.osstimeagent.config.AgentConfig;
import com.gamesofts.osstimeagent.instrument.OssSdkTransformer;
import com.gamesofts.osstimeagent.time.RealTimeClock;
import com.gamesofts.osstimeagent.util.AgentLog;

import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class OssTimeAgent {
    private OssTimeAgent() {
    }

    public static void premain(String agentArgs, Instrumentation inst) {
        AgentConfig config;
        try {
            config = AgentConfig.parse(agentArgs);
        } catch (Throwable t) {
            AgentLog.warn("failed to parse agent args, agent disabled", t);
            return;
        }

        AgentLog.setLevel(config.getLogLevel());

        RealTimeClock clock = new RealTimeClock();
        OssTimeBridge.installClock(clock);

        try {
            OssSdkTransformer transformer = new OssSdkTransformer();
            boolean canRetransform = false;
            try {
                canRetransform = inst.isRetransformClassesSupported();
            } catch (Throwable ignore) {
            }
            inst.addTransformer(transformer, canRetransform);
            RetransformSummary summary = new RetransformSummary();
            if (canRetransform) {
                summary = retransformLoadedTargets(inst, transformer.getTargetClassNames());
            }
            if (summary.failed > 0) {
                AgentLog.warn("transformer register failed partially (retransform=" + canRetransform
                        + ", retransformFailed=" + summary.failed
                        + ", failedClasses=" + summary.failedClassNames + ")");
            } else {
                AgentLog.info("transformer registered (retransform=" + canRetransform + ")");
            }
        } catch (Throwable t) {
            AgentLog.warn("failed to register transformer; agent remains passive", t);
        }
    }

    private static RetransformSummary retransformLoadedTargets(Instrumentation inst, Set<String> targets) {
        RetransformSummary summary = new RetransformSummary();
        Class[] classes;
        try {
            classes = inst.getAllLoadedClasses();
        } catch (Throwable t) {
            AgentLog.warn("cannot enumerate loaded classes for retransform", t);
            summary.failed++;
            return summary;
        }
        int hit = 0;
        int failed = 0;
        int i;
        for (i = 0; i < classes.length; i++) {
            Class c = classes[i];
            if (c == null) {
                continue;
            }
            String name = c.getName().replace('.', '/');
            if (!targets.contains(name)) {
                continue;
            }
            hit++;
            try {
                if (inst.isModifiableClass(c)) {
                    inst.retransformClasses(new Class[] { c });
                } else {
                    failed++;
                    summary.failedClassNames.add(c.getName() + "(not modifiable)");
                }
            } catch (Throwable t) {
                failed++;
                summary.failedClassNames.add(c.getName());
                AgentLog.warn("retransform failed for " + c.getName(), t);
            }
        }
        summary.hit = hit;
        summary.failed = failed;
        return summary;
    }

    private static String sanitize(String agentArgs) {
        if (agentArgs == null) {
            return "";
        }
        return agentArgs;
    }

    private static final class RetransformSummary {
        int hit;
        int failed;
        final List failedClassNames = new ArrayList();
    }
}
