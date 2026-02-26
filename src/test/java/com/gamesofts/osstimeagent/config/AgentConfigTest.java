package com.gamesofts.osstimeagent.config;

import org.junit.Assert;
import org.junit.Test;

public class AgentConfigTest {
    @Test
    public void testParseEmptyArgsAllowed() {
        AgentConfig c = AgentConfig.parse(null);
        Assert.assertEquals("info", c.getLogLevel());
    }

    @Test
    public void testParseLogLevel() {
        AgentConfig c = AgentConfig.parse("log=debug");
        Assert.assertEquals("debug", c.getLogLevel());
    }

    @Test
    public void testIgnoreLegacyNtpArgs() {
        AgentConfig c = AgentConfig.parse("ntp=ntp.aliyun.com,timeoutMs=1000,log=warn");
        Assert.assertEquals("warn", c.getLogLevel());
    }
}
