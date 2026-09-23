# PulseGuard：支付风险调查工作台

[English](README.md) · [业务闭环与演示](docs/analyst-workflow.md) · [简历与面试说明](docs/resume.md)

面向电商支付风险运营团队，把支付事件接入、可靠投递、实时规则计算、证据调查和审核结案串起来。技术栈为 **Java 17、Spring Boot、Kafka、Spark Structured Streaming、MongoDB、Docker 和 Kubernetes**。

## 解决什么问题

支付回调可能重试，消息队列可能暂时不可用，事件可能乱序，而审核员需要知道“哪些交易触发了告警、谁在处理、最后判断是什么”。PulseGuard 为这些问题提供可运行的处理链路：

- **重复回调与故障恢复**：不可变交易 ID、冲突检测、MongoDB 内嵌 outbox、租约与重试，避免接收成功后丢失待投递任务。
- **可解释流式信号**：按账户、币种和事件时间计算高金额、高频、小额支付集中出现等规则，明确水位线与重复事件的语义。
- **准确的调查证据**：保存首次触发告警的交易 ID；后续同窗口的新交易不会混进原告警的证据。卡测试规则只选择符合小额条件的交易，并显示缺失及截断信息。
- **多人协作审核**：领取、备注、释放、结案、重开；版本比较防止过期页面覆盖最新决定，操作 ID 防止超时重试重复记账。
- **处理结果反馈**：区分确认风险、误报、正常业务场景，并按规则统计当前结案结果。重开时清除当前结果，保留历史记录。

## 运行与演示

准备 Docker Compose 和 Python 3.10+，为整套服务分配约 4 核 CPU、6–8 GB 内存：

```bash
docker compose up -d --build
python scripts/e2e.py --with-recovery --timeout 300
python scripts/analyst_scenario.py --timeout 300
```

打开 `http://localhost:8080`，在 Connection settings 中填写本地开发密钥 `local-dev-key`。关闭 Sample data 即使用真实 API。演示脚本会通过 API 发送合成交易，经 Kafka 和 Spark 生成告警，竞争领取同一告警，并实际完成结案、重开和结果统计；报告保存在 `artifacts/analyst-scenario.json`。

[在线展示](https://wxw2002a.github.io/pulseguard/)是带运行来源的只读 CI 快照。Sample data 是独立的浏览器示例。它们不能代替本地完整后端运行。

## 如何证明工程能力

阅读 [业务验收条件](docs/analyst-workflow.md)、[测试分层](docs/testing.md)、[验证记录](docs/verification.md) 与 [架构取舍](docs/architecture.md)。新增工作流还验证：两名审核员并发领取只有一人成功；旧版本写入返回 409；同一操作重试不增加历史；结案必须有判断；重开保留历史并清除当前判断；后续交易不改变原始证据。

## 真实边界

这是支付事件接收后的风险调查系统，不接管支付授权、不拦截或移动资金。规则是透明的演示策略，数据是合成事件，审核标签是场景脚本或用户判断，不能声称真实反欺诈准确率或减少了多少损失。审核员名称仍是共享 API key 下的自报标签；生产使用需要身份认证、权限、私有入口、可靠集群与数据治理。Kubernetes 默认验证单 Pod Spark local 模式，不能写成已验证分布式 Spark 集群。

MIT License。
