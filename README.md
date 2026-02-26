# OSS Time Agent

用于缓解阿里云 OSS Java SDK 请求时间偏差（clock skew）问题的 Java Agent。

## 使用方式

```bash
java -javaagent:/path/to/oss-time-agent.jar -jar app.jar
```

## 行为说明

- 若应用未引入 OSS SDK，agent 会保持被动，不影响应用启动。
- 首次访问 OSS endpoint 前，agent 会先发起一次轻量请求，从响应头 `Date` 获取服务端时间并更新 agent 内部时钟。
- 运行过程中继续依赖 OSS SDK 的 `RequestTimeTooSkewed` 自动校时机制。


## OSS SDK 3.x 兼容性

- 基线验证版本：`3.18.5`
- 兼容：`3.8.1`、`3.11.3`、`3.15.1`、`3.18.5`