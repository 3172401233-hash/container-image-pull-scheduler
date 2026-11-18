'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { PullScheduler } = require('../src/scheduler');

function bytesFor(plan, digest) {
  return plan.rounds
    .flatMap((r) => r.allocations)
    .filter((a) => a.digest === digest)
    .reduce((sum, a) => sum + a.bytes, 0);
}

test('冷启动热点镜像：同一层跨节点只下载一次并共享分发', () => {
  const s = new PullScheduler({ registryBandwidth: 100 });
  s.registerLayer('sha256:base', 50);
  s.registerLayer('sha256:app', 150);
  s.addImage('web:1.0', { digest: 'sha256:manifest1', layers: ['sha256:base', 'sha256:app'] });
  for (const node of ['n1', 'n2', 'n3']) {
    s.addNode(node);
    s.addPod({ id: `pod-${node}`, node, image: 'web:1.0', priority: 5, deadline: 10 });
  }

  const plan = s.plan();

  // 全局去重：200MB 的层只从仓库拉 200MB，而不是 3 节点 x 200MB。
  assert.equal(plan.summary.totalBytesFromRegistry, 200);
  assert.equal(bytesFor(plan, 'sha256:base'), 50);
  assert.equal(bytesFor(plan, 'sha256:app'), 150);
  assert.equal(plan.summary.dedupSavedBytes, 400); // (3-1) * (50+150)

  // 跨节点共享：每层都分发给全部 3 个节点。
  assert.deepEqual(plan.layers['sha256:base'].sharedWith, ['n1', 'n2', 'n3']);
  assert.deepEqual(plan.layers['sha256:app'].sharedWith, ['n1', 'n2', 'n3']);

  // 所有 Pod 同时 ready：round1 下完 base(50)+app 的 50，round2 下完 app 剩余 100。
  for (const node of ['n1', 'n2', 'n3']) {
    assert.equal(plan.pods[`pod-${node}`].readyRound, 2);
    assert.equal(plan.pods[`pod-${node}`].readyAt, 2);
  }
  assert.equal(plan.rounds.length, 2);
  assert.equal(plan.waitingImages.length, 1);
  assert.equal(plan.waitingImages[0].image, 'web:1.0');
  assert.equal(plan.waitingImages[0].readyRound, 2);
});

test('layer 乱序：缓存命中与顺序无关，缓存不完整不得启动', () => {
  const s = new PullScheduler({ registryBandwidth: 10 });
  s.registerLayer('sha256:l1', 10);
  s.registerLayer('sha256:l2', 10);
  s.registerLayer('sha256:l3', 10);
  s.addImage('app:2.0', { digest: 'sha256:m2', layers: ['sha256:l1', 'sha256:l2', 'sha256:l3'] });
  // 节点已缓存中间层 l2 —— 命中与层在清单中的位置无关。
  s.addNode('n1', { cachedLayers: ['sha256:l2'] });
  s.addPod({ id: 'p1', node: 'n1', image: 'app:2.0', priority: 1 });

  const plan = s.plan();

  // 只需补 l1 和 l3，l2 不重复下载。
  assert.equal(plan.summary.totalBytesFromRegistry, 20);
  assert.equal(bytesFor(plan, 'sha256:l2'), 0);

  // 同优先级 max-min 平分带宽：l1、l3 各 10MB，两轮后同时完成。
  // round1 结束时两层各下一半，缓存不完整，不得 ready。
  const round1 = plan.rounds[0];
  assert.equal(round1.completedLayers.length, 0);
  assert.equal(round1.readyPods.length, 0);
  assert.deepEqual(
    plan.rounds[1].completedLayers.map((c) => c.digest).sort(),
    ['sha256:l1', 'sha256:l3']
  );
  assert.equal(plan.pods.p1.readyRound, 2);
});

test('带宽不足：高优先级先调度，小镜像不被大镜像挤死，超时标记 missedDeadline', () => {
  const s = new PullScheduler({ registryBandwidth: 10 });
  s.registerLayer('sha256:big', 100);
  s.registerLayer('sha256:small', 5);
  s.registerLayer('sha256:vip', 30);
  s.addImage('big:1.0', { digest: 'sha256:mb', layers: ['sha256:big'] });
  s.addImage('small:1.0', { digest: 'sha256:ms', layers: ['sha256:small'] });
  s.addImage('vip:1.0', { digest: 'sha256:mv', layers: ['sha256:vip'] });
  s.addNode('n1');
  s.addNode('n2');
  s.addNode('n3');
  s.addPod({ id: 'big-pod', node: 'n1', image: 'big:1.0', priority: 5, deadline: 5 });
  s.addPod({ id: 'small-pod', node: 'n2', image: 'small:1.0', priority: 5, deadline: 50 });
  s.addPod({ id: 'vip-pod', node: 'n3', image: 'vip:1.0', priority: 9, deadline: 50 });

  const plan = s.plan();

  // 高优先级 vip 独占带宽：30/10 = 3 轮 ready。
  assert.equal(plan.pods['vip-pod'].readyRound, 3);
  // 同优先级 max-min 公平：small 与 big 平分带宽，small 第 4 轮即 ready，
  // 不会被 100MB 的大镜像堵到最后。
  assert.equal(plan.pods['small-pod'].readyRound, 4);
  assert.ok(plan.pods['big-pod'].readyRound > plan.pods['small-pod'].readyRound);
  // big 需要 100MB，vip 用掉 30 后剩余 70MB 带宽预算内无法赶上 deadline=5。
  assert.equal(plan.pods['big-pod'].missedDeadline, true);
  assert.equal(plan.pods['vip-pod'].missedDeadline, false);
  // 每轮带宽分配不超过预算。
  for (const round of plan.rounds) {
    const spent = round.allocations.reduce((sum, a) => sum + a.bytes, 0);
    assert.ok(spent <= 10 + 1e-9, `round ${round.round} overspent: ${spent}`);
  }
});

test('镜像摘要变化：旧层下载取消，新清单层未就绪前不得启动', () => {
  const s = new PullScheduler({ registryBandwidth: 10 });
  s.registerLayer('sha256:old1', 20);
  s.registerLayer('sha256:old2', 20);
  s.registerLayer('sha256:new1', 20);
  s.addImage('api:1.0', { digest: 'sha256:m-old', layers: ['sha256:old1', 'sha256:old2'] });
  s.addNode('n1', { cachedLayers: ['sha256:old2'] });
  s.addPod({ id: 'p1', node: 'n1', image: 'api:1.0', priority: 1 });

  const plan = s.simulate({
    events: [
      {
        round: 2,
        type: 'image-update',
        image: 'api:1.0',
        manifest: { digest: 'sha256:m-new', layers: ['sha256:new1'] },
      },
    ],
  });

  // round1 正在下 old1（10/20），round2 清单切换后 old1 被取消并记录浪费。
  const round2 = plan.rounds[1];
  assert.deepEqual(round2.events, [{ type: 'image-update', image: 'api:1.0', digest: 'sha256:m-new' }]);
  assert.deepEqual(round2.cancelled, [{ digest: 'sha256:old1', wastedBytes: 10 }]);

  // 缓存一致性：节点上旧的 old2 缓存不被新清单承认，必须等新层 new1 下完。
  // new1 共 20MB，round2、round3 各下 10MB，round3 就绪。
  assert.equal(plan.pods.p1.readyRound, 3);
  assert.equal(bytesFor(plan, 'sha256:new1'), 20);
  // old1 总共只下了 10MB 就被取消，没有重复下载。
  assert.equal(bytesFor(plan, 'sha256:old1'), 10);
});

test('节点故障：Pod 迁移到缓存最匹配的存活节点，共享下载不重启', () => {
  const s = new PullScheduler({ registryBandwidth: 20 });
  s.registerLayer('sha256:hot', 40);
  s.addImage('hot:1.0', { digest: 'sha256:mh', layers: ['sha256:hot'] });
  s.addNode('n1');
  s.addNode('n2');
  s.addPod({ id: 'p1', node: 'n1', image: 'hot:1.0', priority: 5 });
  s.addPod({ id: 'p2', node: 'n2', image: 'hot:1.0', priority: 5 });

  const plan = s.simulate({ events: [{ round: 2, type: 'node-failure', node: 'n1' }] });

  // 共享下载只进行一次：40MB 总量，故障后不重复拉取。
  assert.equal(bytesFor(plan, 'sha256:hot'), 40);
  assert.equal(plan.summary.totalBytesFromRegistry, 40);

  // p1 被迁移到 n2（缓存亲和），两个 Pod 都在 round2 ready。
  assert.equal(plan.pods.p1.rescheduledFrom, 'n1');
  assert.equal(plan.pods.p1.node, 'n2');
  assert.equal(plan.pods.p1.readyRound, 2);
  assert.equal(plan.pods.p2.readyRound, 2);
  const round2 = plan.rounds[1];
  assert.ok(round2.events.some((e) => e.type === 'node-failure' && e.node === 'n1'));
  assert.ok(
    round2.events.some((e) => e.type === 'pod-rescheduled' && e.pod === 'p1' && e.to === 'n2')
  );
});

test('节点故障且无存活节点：Pod 标记失败', () => {
  const s = new PullScheduler({ registryBandwidth: 20 });
  s.registerLayer('sha256:x', 10);
  s.addImage('x:1.0', { digest: 'sha256:mx', layers: ['sha256:x'] });
  s.addNode('only');
  s.addPod({ id: 'p1', node: 'only', image: 'x:1.0', priority: 1 });

  const plan = s.simulate({ events: [{ round: 1, type: 'node-failure', node: 'only' }] });

  assert.equal(plan.pods.p1.failed, true);
  assert.equal(plan.pods.p1.readyRound, null);
  assert.equal(plan.summary.failedPods, 1);
});

test('全部缓存命中：无需下载，readyRound 为 0', () => {
  const s = new PullScheduler({ registryBandwidth: 10 });
  s.registerLayer('sha256:a', 10);
  s.addImage('a:1.0', { digest: 'sha256:ma', layers: ['sha256:a'] });
  s.addNode('n1', { cachedLayers: ['sha256:a'] });
  s.addPod({ id: 'p1', node: 'n1', image: 'a:1.0', priority: 1 });

  const plan = s.plan();

  assert.equal(plan.pods.p1.readyRound, 0);
  assert.equal(plan.rounds.length, 0);
  assert.equal(plan.summary.totalBytesFromRegistry, 0);
});
