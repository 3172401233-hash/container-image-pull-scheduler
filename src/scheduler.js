'use strict';

const EPSILON = 1e-9;

/**
 * 容器镜像拉取调度器。
 *
 * 模型：
 * - 镜像层（layer）以 digest 全局唯一标识，是缓存与去重的最小单位。
 * - 同一 digest 的层在整个集群中只从镜像仓库下载一次（全局去重），
 *   下载完成后分发给所有需要它的节点（跨节点共享，不重复消耗仓库带宽）。
 * - 节点缓存按 digest 记录完整层；镜像清单（manifest）变化后，
 *   旧 digest 的缓存层不再被新清单引用，Pod 必须等新层全部落盘才能启动。
 * - 带宽按轮（round）分配，每轮可用带宽 = registryBandwidth。
 */
class PullScheduler {
  constructor({ registryBandwidth, roundDuration = 1 } = {}) {
    if (!Number.isFinite(registryBandwidth) || registryBandwidth <= 0) {
      throw new Error('registryBandwidth must be a positive number');
    }
    this.registryBandwidth = registryBandwidth;
    this.roundDuration = roundDuration;
    this.layers = new Map(); // digest -> { digest, size }
    this.images = new Map(); // name -> { name, digest, layers[] }
    this.nodes = new Map(); // id -> { id, cache:Set, alive }
    this.pods = new Map(); // id -> pod
    this.downloads = new Map(); // digest -> { digest, remaining, downloaded }
  }

  registerLayer(digest, size) {
    if (!digest) throw new Error('layer digest is required');
    if (!(size > 0)) throw new Error(`layer ${digest} size must be > 0`);
    this.layers.set(digest, { digest, size });
    return this;
  }

  addImage(name, manifest) {
    return this._setImage(name, manifest);
  }

  updateImage(name, manifest) {
    return this._setImage(name, manifest);
  }

  _setImage(name, { digest, layers } = {}) {
    if (!name) throw new Error('image name is required');
    if (!digest) throw new Error(`image ${name} manifest digest is required`);
    if (!Array.isArray(layers) || layers.length === 0) {
      throw new Error(`image ${name} must reference at least one layer`);
    }
    for (const layerDigest of layers) {
      if (!this.layers.has(layerDigest)) {
        throw new Error(`image ${name} references unknown layer ${layerDigest}`);
      }
    }
    this.images.set(name, { name, digest, layers: [...layers] });
    return this;
  }

  addNode(id, { cachedLayers = [] } = {}) {
    if (!id) throw new Error('node id is required');
    for (const digest of cachedLayers) {
      if (!this.layers.has(digest)) {
        throw new Error(`node ${id} caches unknown layer ${digest}`);
      }
    }
    this.nodes.set(id, { id, cache: new Set(cachedLayers), alive: true });
    return this;
  }

  addPod({ id, node, image, priority = 0, deadline = Infinity } = {}) {
    if (!id) throw new Error('pod id is required');
    if (!this.nodes.has(node)) throw new Error(`unknown node ${node}`);
    if (!this.images.has(image)) throw new Error(`unknown image ${image}`);
    this.pods.set(id, {
      id,
      node,
      image,
      priority,
      deadline,
      readyRound: null,
      failed: false,
      rescheduledFrom: null,
    });
    return this;
  }

  failNode(id) {
    const node = this.nodes.get(id);
    if (node) node.alive = false;
    return this;
  }

  missingLayers(pod) {
    const image = this.images.get(pod.image);
    const node = this.nodes.get(pod.node);
    if (!image || !node || !node.alive) return [];
    return image.layers.filter((digest) => !node.cache.has(digest));
  }

  podReady(pod) {
    const node = this.nodes.get(pod.node);
    return Boolean(node && node.alive) && this.missingLayers(pod).length === 0;
  }

  _collectDemand() {
    const demand = new Map();
    for (const pod of this.pods.values()) {
      if (pod.failed || pod.readyRound !== null) continue;
      const node = this.nodes.get(pod.node);
      if (!node || !node.alive) continue;
      for (const digest of this.missingLayers(pod)) {
        let entry = demand.get(digest);
        if (!entry) {
          entry = { digest, nodes: new Set(), pods: [], priority: -Infinity, deadline: Infinity };
          demand.set(digest, entry);
        }
        entry.nodes.add(pod.node);
        entry.pods.push(pod.id);
        entry.priority = Math.max(entry.priority, pod.priority);
        entry.deadline = Math.min(entry.deadline, pod.deadline);
      }
    }
    return demand;
  }

  /**
   * 带宽分配：
   * - 严格按优先级分层，高优先级层先拿带宽；
   * - 同优先级内按 max-min 公平分配（每层先平分，下完的层让出余量），
   *   保证小镜像不会被同优先级的大镜像挤死；
   * - 同层多节点需求只算一次下载，天然去重。
   */
  _allocate(demand) {
    const items = [...demand.values()].map((entry) => ({
      ...entry,
      remaining:
        this.downloads.get(entry.digest)?.remaining ?? this.layers.get(entry.digest).size,
    }));
    const tiers = new Map();
    for (const item of items) {
      if (!tiers.has(item.priority)) tiers.set(item.priority, []);
      tiers.get(item.priority).push(item);
    }
    const priorities = [...tiers.keys()].sort((a, b) => b - a);
    let budget = this.registryBandwidth;
    const allocations = [];
    for (const priority of priorities) {
      if (budget <= EPSILON) break;
      const tier = tiers.get(priority).sort(
        (a, b) =>
          a.deadline - b.deadline ||
          b.nodes.size - a.nodes.size ||
          (a.digest < b.digest ? -1 : a.digest > b.digest ? 1 : 0)
      );
      const needs = new Map(tier.map((item) => [item.digest, item.remaining]));
      const granted = new Map(tier.map((item) => [item.digest, 0]));
      let active = tier.map((item) => item.digest);
      while (budget > EPSILON && active.length > 0) {
        const share = budget / active.length;
        const stillActive = [];
        let spent = 0;
        for (const digest of active) {
          const give = Math.min(share, needs.get(digest));
          granted.set(digest, granted.get(digest) + give);
          needs.set(digest, needs.get(digest) - give);
          spent += give;
          if (needs.get(digest) > EPSILON) stillActive.push(digest);
        }
        budget -= spent;
        active = stillActive;
        if (spent <= EPSILON) break;
      }
      for (const item of tier) {
        const bytes = granted.get(item.digest);
        if (bytes > EPSILON) {
          allocations.push({
            digest: item.digest,
            bytes,
            nodes: [...item.nodes].sort(),
            pods: [...item.pods].sort(),
            priority: item.priority,
            deadline: item.deadline,
          });
        }
      }
    }
    return allocations;
  }

  _bestNodeFor(pod) {
    const image = this.images.get(pod.image);
    let best = null;
    let bestScore = -1;
    for (const node of this.nodes.values()) {
      if (!node.alive) continue;
      const score = image.layers.filter((digest) => node.cache.has(digest)).length;
      if (score > bestScore || (score === bestScore && best !== null && node.id < best)) {
        best = node.id;
        bestScore = score;
      }
    }
    return best;
  }

  _applyEvent(event, roundLog) {
    if (event.type === 'image-update') {
      this.updateImage(event.image, event.manifest);
      roundLog.events.push({
        type: 'image-update',
        image: event.image,
        digest: event.manifest.digest,
      });
    } else if (event.type === 'node-failure') {
      this.failNode(event.node);
      roundLog.events.push({ type: 'node-failure', node: event.node });
      for (const pod of this.pods.values()) {
        if (pod.node !== event.node || pod.readyRound !== null || pod.failed) continue;
        const target = this._bestNodeFor(pod);
        if (target) {
          pod.rescheduledFrom = event.node;
          pod.node = target;
          roundLog.events.push({
            type: 'pod-rescheduled',
            pod: pod.id,
            from: event.node,
            to: target,
          });
        } else {
          pod.failed = true;
          roundLog.events.push({ type: 'pod-failed', pod: pod.id, node: event.node });
        }
      }
    } else {
      throw new Error(`unknown event type ${event.type}`);
    }
  }

  /**
   * 运行调度模拟，返回完整下载计划。
   * events: [{ round, type: 'image-update'|'node-failure', ... }]
   */
  simulate({ maxRounds = 10000, events = [] } = {}) {
    const eventsByRound = new Map();
    for (const event of events) {
      if (!eventsByRound.has(event.round)) eventsByRound.set(event.round, []);
      eventsByRound.get(event.round).push(event);
    }

    const rounds = [];
    for (const pod of this.pods.values()) {
      if (!pod.failed && this.podReady(pod)) pod.readyRound = 0;
    }

    let round = 0;
    while (round < maxRounds) {
      const pending = [...this.pods.values()].some((p) => !p.failed && p.readyRound === null);
      if (!pending) break;
      round += 1;
      const log = {
        round,
        time: round * this.roundDuration,
        events: [],
        allocations: [],
        completedLayers: [],
        readyPods: [],
        cancelled: [],
      };

      for (const event of eventsByRound.get(round) ?? []) {
        this._applyEvent(event, log);
      }

      const demand = this._collectDemand();

      // 清单变化等原因导致不再被任何 Pod 需要的半成品下载：取消并计入浪费。
      for (const [digest, download] of [...this.downloads]) {
        if (!demand.has(digest)) {
          log.cancelled.push({ digest, wastedBytes: download.downloaded });
          this.downloads.delete(digest);
        }
      }

      const allocations = this._allocate(demand);
      const completed = [];
      for (const allocation of allocations) {
        let download = this.downloads.get(allocation.digest);
        if (!download) {
          download = {
            digest: allocation.digest,
            remaining: this.layers.get(allocation.digest).size,
            downloaded: 0,
          };
          this.downloads.set(allocation.digest, download);
        }
        download.remaining -= allocation.bytes;
        download.downloaded += allocation.bytes;
        const done = download.remaining <= EPSILON;
        if (done) {
          download.remaining = 0;
          completed.push(allocation.digest);
        }
        log.allocations.push({
          digest: allocation.digest,
          bytes: allocation.bytes,
          nodes: allocation.nodes,
          pods: allocation.pods,
          shared: allocation.nodes.length > 1,
          completed: done,
        });
      }

      // 下载完成：同一 digest 一次性分发给所有需求节点（跨节点共享）。
      for (const digest of completed) {
        const entry = demand.get(digest);
        for (const nodeId of entry.nodes) {
          this.nodes.get(nodeId).cache.add(digest);
        }
        this.downloads.delete(digest);
        log.completedLayers.push({ digest, deliveredTo: [...entry.nodes].sort() });
      }

      // 缓存完整（镜像所有层都在节点上）才允许启动容器。
      for (const pod of this.pods.values()) {
        if (pod.failed || pod.readyRound !== null) continue;
        if (this.podReady(pod)) {
          pod.readyRound = round;
          log.readyPods.push({ pod: pod.id, node: pod.node, readyRound: round });
        }
      }

      rounds.push(log);

      const madeProgress =
        log.allocations.length > 0 || log.events.length > 0 || log.readyPods.length > 0;
      const hasFutureEvents = [...eventsByRound.keys()].some((r) => r > round);
      if (!madeProgress && !hasFutureEvents) break;
    }

    return this._buildPlan(rounds);
  }

  plan(options = {}) {
    return this.simulate(options);
  }

  _buildPlan(rounds) {
    const pods = {};
    for (const pod of this.pods.values()) {
      pods[pod.id] = {
        node: pod.node,
        image: pod.image,
        priority: pod.priority,
        deadline: pod.deadline,
        readyRound: pod.readyRound,
        readyAt: pod.readyRound === null ? null : pod.readyRound * this.roundDuration,
        missedDeadline:
          pod.readyRound !== null &&
          Number.isFinite(pod.deadline) &&
          pod.readyRound > pod.deadline,
        failed: pod.failed,
        rescheduledFrom: pod.rescheduledFrom,
      };
    }

    const layerStats = new Map();
    for (const log of rounds) {
      for (const allocation of log.allocations) {
        let stat = layerStats.get(allocation.digest);
        if (!stat) {
          stat = {
            digest: allocation.digest,
            size: this.layers.get(allocation.digest).size,
            bytesDownloaded: 0,
            nodes: new Set(),
            completedRound: null,
          };
          layerStats.set(allocation.digest, stat);
        }
        stat.bytesDownloaded += allocation.bytes;
        for (const nodeId of allocation.nodes) stat.nodes.add(nodeId);
        if (allocation.completed) stat.completedRound = log.round;
      }
    }

    const layers = {};
    let totalBytes = 0;
    let dedupSavedBytes = 0;
    for (const stat of layerStats.values()) {
      totalBytes += stat.bytesDownloaded;
      if (stat.nodes.size > 1) dedupSavedBytes += stat.size * (stat.nodes.size - 1);
      layers[stat.digest] = {
        size: stat.size,
        bytesDownloaded: stat.bytesDownloaded,
        sharedWith: [...stat.nodes].sort(),
        completedRound: stat.completedRound,
      };
    }

    const pullOrder = [];
    const seen = new Set();
    for (const log of rounds) {
      for (const allocation of log.allocations) {
        if (!seen.has(allocation.digest)) {
          seen.add(allocation.digest);
          pullOrder.push(allocation.digest);
        }
      }
    }

    const waiting = new Map();
    for (const pod of this.pods.values()) {
      if (pod.failed || pod.readyRound === null || pod.readyRound === 0) continue;
      let entry = waiting.get(pod.image);
      if (!entry) {
        entry = { image: pod.image, pods: [], readyRound: 0 };
        waiting.set(pod.image, entry);
      }
      entry.pods.push(pod.id);
      entry.readyRound = Math.max(entry.readyRound, pod.readyRound);
    }
    const waitingImages = [...waiting.values()].map((entry) => ({
      image: entry.image,
      pods: entry.pods.sort(),
      readyRound: entry.readyRound,
      readyAt: entry.readyRound * this.roundDuration,
    }));

    return {
      rounds,
      pods,
      layers,
      pullOrder,
      waitingImages,
      summary: {
        totalRounds: rounds.length,
        totalBytesFromRegistry: totalBytes,
        dedupSavedBytes,
        readyPods: Object.values(pods).filter((p) => p.readyRound !== null).length,
        failedPods: Object.values(pods).filter((p) => p.failed).length,
      },
    };
  }
}

module.exports = { PullScheduler };
