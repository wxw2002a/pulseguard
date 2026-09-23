# Resume and interview notes / 简历与面试说明

These statements describe implemented behavior. Attach the verification report for the exact revision you demonstrate. Add measured figures only after reading their workload, source revision and limits.

## 中文简历

**PulseGuard｜支付风险流式分析与协作调查平台**

Java 17 / Spring Boot / Kafka / Spark Structured Streaming / MongoDB / Kubernetes

- 设计支付事件接入与风险调查闭环，通过不可变交易 ID、MongoDB 内嵌 Outbox、租约重试及幂等投影，处理重复回调、消息重放和服务恢复。
- 基于 Spark 事件时间窗口与水位线计算账户级风险信号，保存首次触发规则的交易证据集合，区分迟到数据、证据截断及缺失记录，保证调查依据可追溯。
- 实现告警领取、审核结案与重开流程，使用 MongoDB 原子版本比较和操作 ID 保障并发安全与重试幂等，并提供游标分页及按规则统计审核结果。
- 编写真实 HTTP 并发竞争、消息重放、断点恢复和浏览器交互测试，将结果汇总到 CI 质量门禁；提供可复现的业务演示脚本与部署文档。

## English resume

**PulseGuard — Streaming Payment Risk and Investigation Platform**

- Built a Java/Spring Boot payment-event pipeline using an embedded MongoDB outbox, Kafka and Spark Structured Streaming, with immutable event IDs, leased publication retries and replay-safe projections.
- Preserved rule-specific transaction evidence at initial detection, exposing missing records and bounded capture rather than mixing later arrivals into an analyst's evidence.
- Implemented claim, review, resolution and reopen workflows using atomic version checks and idempotent operation IDs, with cursor-based queues and per-rule analyst outcome reporting.
- Added reproducible concurrent-client and end-to-end business scenarios alongside database, streaming, browser and deployment checks in CI.

## Be ready to explain

1. **Why embedded outbox?** A single-document insert makes event acceptance and publication intent atomic. Publication remains at least once, so downstream deduplication and idempotent projection are required.
2. **Why pin evidence?** The current account window can include later transactions, while the alert records the first qualifying microbatch. Without membership IDs, querying that entire minute changes the story after a decision.
3. **Why not just append notes?** Atomic history append alone does not prevent lost workflow updates. The update also needs a version precondition, legal transition, owner check and command identity.
4. **What if a client times out?** Reuse the same operation ID and normalized command. The retained entry lets the API recognize success without incrementing the version or adding a second note.
5. **What does a false-positive count mean?** It is an analyst's current label on a resolved alert, not independently validated model precision. There is no labeled population of undetected fraud from which recall could be calculated.
6. **Where does it stop scaling?** Each open hot-account window retains exact IDs; evidence is capped at 200, but that does not cap Spark's deduplication state. Review history has a 500-action bound. Aggregate reporting and single-node development infrastructure need measured redesign for larger workloads.
7. **Is ownership authorization?** No. Labels are self-reported under a shared API key. Authenticated principals and role enforcement must precede sensitive production use.
8. **Can the public demo accept payments?** No. Pages serves a read-only snapshot. The local Compose stack executes the backend, while synthetic fixture mode is clearly separate.

Do not claim production fraud prevention, real customer adoption, a percentage loss reduction, distributed Spark performance or throughput beyond the workload actually measured. Explain the concrete failure modes and show the tests instead.
