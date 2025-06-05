const _ = require('lodash');
const fs = require('fs-extra');
const path = require('path');

class GlobalPriorityQueue {
  constructor() {
    this.queue = [];
    this.lock = false;
    this.processingInterval = 5000; // 5 seconds
    this.lastProcessedTime = 0;
  }

  async addBatches(batches) {
    this.queue.push(...batches);
    this.queue = _.orderBy(this.queue, ['priority', 'created_at'], ['desc', 'asc']);
    await this.persistQueue();
  }

  async getNextBatch() {
    if (this.queue.length === 0) return null;
    
    const now = Date.now();
    if (now - this.lastProcessedTime < this.processingInterval) {
      return null; // Rate limiting
    }

    const nextBatch = this.queue.shift();
    this.lastProcessedTime = now;
    await this.persistQueue();
    return nextBatch;
  }

  async persistQueue() {
    const queuePath = path.join(__dirname, '../data/queue_state.json');
    await fs.writeJson(queuePath, {
      queue: this.queue,
      lastProcessedTime: this.lastProcessedTime
    });
  }

  async initialize() {
    const queuePath = path.join(__dirname, '../data/queue_state.json');
    if (await fs.pathExists(queuePath)) {
      const state = await fs.readJson(queuePath);
      this.queue = state.queue;
      this.lastProcessedTime = state.lastProcessedTime || 0;
    }
  }
}

const globalQueue = new GlobalPriorityQueue();
globalQueue.initialize(); // Load any existing state

module.exports = {
  addBatches: (batches) => globalQueue.addBatches(batches),
  getNextBatch: () => globalQueue.getNextBatch(),
  isEmpty: () => globalQueue.queue.length === 0
};