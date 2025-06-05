const fs = require('fs-extra');
const path = require('path');
const { getNextBatch } = require('./queueService');
const BATCHES_DIR = path.join(__dirname, '../data/batches');

// Simulate external API call
async function processId(id) {
  const delay = Math.random() * 1000 + 500;
  await new Promise(resolve => setTimeout(resolve, delay));
  return { id, data: "processed" };
}

async function processBatch(batch) {
  try {
    // Update batch status
    const batchPath = path.join(BATCHES_DIR, `${batch.batch_id}.json`);
    await fs.writeJson(batchPath, { ...batch, status: 'triggered' });
    
    // Process each ID
    const results = await Promise.all(batch.ids.map(processId));
    
    // Mark batch as completed
    await fs.writeJson(batchPath, { 
      ...batch, 
      status: 'completed',
      processed_at: new Date().toISOString(),
      results
    });
    
    console.log(`Processed batch ${batch.batch_id} (Priority: ${batch.priority})`);
  } catch (error) {
    console.error(`Error processing batch ${batch.batch_id}:`, error);
  }
}

function startWorker() {
  async function processNext() {
    const batch = await getNextBatch();
    if (batch) {
      console.log(`Processing batch [${batch.ids}] (Priority: ${batch.priority})`);
      await processBatch(batch);
    }
    setTimeout(processNext, 1000); // Check every second
  }
  
  processNext();
  console.log('worker started');
}

module.exports = { startWorker };