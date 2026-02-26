package com.gamesofts.osstimeagent.config;

public final class AgentConfig {
    public static final String DEFAULT_LOG_LEVEL = "info";

    private final String logLevel;

    private AgentConfig(String logLevel) {
        this.logLevel = logLevel;
    }

    public static AgentConfig parse(String agentArgs) {
        String logLevel = DEFAULT_LOG_LEVEL;

        if (agentArgs != null && agentArgs.trim().length() > 0) {
            String[] parts = agentArgs.split(",");
            int i;
            for (i = 0; i < parts.length; i++) {
                String part = parts[i];
                if (part == null) {
                    continue;
                }
                part = part.trim();
                if (part.length() == 0) {
                    continue;
                }
                int idx = part.indexOf('=');
                if (idx < 0) {
                    continue;
                }
                String key = part.substring(0, idx).trim();
                String value = part.substring(idx + 1).trim();
                if ("log".equalsIgnoreCase(key) || "logLevel".equalsIgnoreCase(key)) {
                    logLevel = value;
                }
            }
        }

        if (logLevel == null || logLevel.trim().length() == 0) {
            logLevel = DEFAULT_LOG_LEVEL;
        }
        return new AgentConfig(logLevel);
    }

    public String getLogLevel() {
        return logLevel;
    }
}
