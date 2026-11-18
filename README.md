# container-image-pull-scheduler
JavaScript 容器镜像拉取调度库，处理层缓存、带宽预算、优先级和多节点分发

## 背景

一批 Pod 同时创建时，每个节点独立下载相同镜像层会把镜像仓库带宽瞬间打满，
小镜像也被大镜像挤住。本库把镜像层（layer）作为全局调度单位：
同一层只从仓库下载一次，完成后分发给所有需要它的节点，
并按 Pod 优先级与截止时间分配共享带宽，输出每轮下载分配与预计 ready 时间。

## 快速开始

```js
const { PullScheduler } = require('./src/scheduler');

const s = new PullScheduler({ registryBandwidth: 100, roundDuration: 1 });

s.registerLayer('sha256:base', 50);   // digest + 大小（MB）
s.registerLayer('sha256:app', 150);
s.addImage('web:1.0', { digest: 'sha256:m1', layers: ['sha256:base', 'sha256:app'] });

s.addNode('n1');                       // 可选 { cachedLayers: [...] }
s.addNode('n2', { cachedLayers: ['sha256:base'] });

s.addPod({ id: 'p1', node: 'n1', image: 'web:1.0', priority: 5, deadline: 10 });
s.addPod({ id: 'p2', node: 'n2', image: 'web:1.0', priority: 5, deadline: 10 });

const plan = s.plan();
// plan.rounds        每轮下载分配（digest、字节数、需求节点、是否共享、是否完成）
// plan.pullOrder     建议的层拉取顺序
// plan.waitingImages 等待中的镜像及预计 ready 轮次
// plan.pods          每个 Pod 的 readyRound / readyAt / missedDeadline
// plan.layers        每层的实际下载字节数与共享节点列表
// plan.summary       总轮次、仓库总流量、去重节省流量
```

### 动态事件

`simulate({ events })` 支持在指定轮次注入故障与变更：

```js
const plan = s.simulate({
  events: [
    { round: 2, type: 'image-update', image: 'web:1.0',
      manifest: { digest: 'sha256:m2', layers: ['sha256:new'] } },
    { round: 3, type: 'node-failure', node: 'n1' },
  ],
});
```

- `image-update`：镜像摘要变化，调度器立即按新清单重新计算需求，
  不再被引用的半成品下载会被取消并计入 `round.cancelled`。
- `node-failure`：节点宕机，其上未就绪的 Pod 按缓存亲和度
  （已缓存所需层最多的存活节点）迁移；进行中的共享下载不中断、不重启。

## 缓存一致性

- **digest 即事实**：节点缓存以层 digest 为键，只有完整下载完成的层才进入缓存；
  半成品不计入缓存，断电/故障后不会被视为可用。
- **清单决定有效性**：Pod 能否启动只取决于其镜像当前清单引用的所有层
  是否都在本节点缓存中。镜像摘要变化后，旧 digest 的缓存层立即失效
  （不再被新清单引用），即使它物理上还在节点上。
- **不完整不启动**：`readyRound` 只在节点缓存覆盖清单全部层的那一轮才赋值，
  层乱序到达、部分命中都不会提前放行容器启动。
- **故障即失效**：节点宕机后其缓存随节点一起退出调度，
  迁移到其他节点的 Pod 按目标节点缓存重新计算缺口。

## 公平策略

- **全局去重**：同一 digest 全集群只下载一次，多节点需求合并为一次仓库流量，
  完成后同时分发（`allocation.shared = true`），从根上消除热点镜像的带宽放大。
- **严格优先级分层**：高优先级 Pod 的层先占用带宽，保证关键业务先就绪。
- **同级 max-min 公平**：同优先级的层平分带宽，下完的层自动让出余量；
  小镜像几轮内即可就绪，不会被同优先级的大镜像挤死。
- **截止时间破平**：同优先级内按最早截止时间（EDF）排序，
  需求节点更多的热点层优先；`missedDeadline` 标记无法按时就绪的 Pod。

## 测试

```sh
npm test   # node --test
```

覆盖场景：冷启动热点镜像（去重与共享分发）、layer 乱序与部分缓存、
带宽不足（优先级、公平性、超时标记）、镜像摘要变化（取消与缓存失效）、
节点故障（迁移与下载不重启）、全部缓存命中。
