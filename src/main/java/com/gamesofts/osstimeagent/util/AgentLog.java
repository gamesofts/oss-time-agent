package com.gamesofts.osstimeagent.util;

public final class AgentLog {
    private static final String PREFIX = "[oss-time-agent] ";
    private static volatile int level = 1; // 0=warn,1=info,2=debug

    private AgentLog() {
    }

    public static void setLevel(String name) {
        if (name == null) {
            return;
        }
        if ("debug".equalsIgnoreCase(name)) {
            level = 2;
        } else if ("warn".equalsIgnoreCase(name)) {
            level = 0;
        } else {
            level = 1;
        }
    }

    public static void warn(String msg) {
        log(0, "WARN", msg, null);
    }

    public static void warn(String msg, Throwable t) {
        log(0, "WARN", msg, t);
    }

    public static void info(String msg) {
        log(1, "INFO", msg, null);
    }

    public static void debug(String msg) {
        log(2, "DEBUG", msg, null);
    }

    private static void log(int required, String tag, String msg, Throwable t) {
        if (level < required) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(PREFIX).append(tag).append(' ').append(msg);
        System.err.println(sb.toString());
        if (t != null) {
            t.printStackTrace(System.err);
        }
    }
}
