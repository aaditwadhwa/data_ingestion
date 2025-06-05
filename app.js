const express = require('express');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs-extra');
const path = require('path');
const _ = require('lodash');

const app = express();
app.use(express.json());

// Priority levels
const PRIORITY = {
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1
};

// File system paths
const DATA_DIR = path.join(__dirname, 'data');
const INGESTIONS_DIR = path.join(DATA_DIR, 'ingestions');
const BATCHES_DIR = path.join(DATA_DIR, 'batches');

// Ensure directories exist
fs.ensureDirSync(INGESTIONS_DIR);
fs.ensureDirSync(BATCHES_DIR);

// Queue and worker
const queueService = require('./services/queueService');
const workerService = require('./services/workerService');

// Start the worker
workerService.startWorker();

// Ingestion endpoint
app.post('/ingest', async (req, res) => {
  try {
    const { ids, priority } = req.body;
    
    // Validate input
    if (!ids || !Array.isArray(ids) || ids.length === 0 || 
        !priority || !['HIGH', 'MEDIUM', 'LOW'].includes(priority)) {
      return res.status(400).json({ error: 'Invalid input' });
    }
    
    // Create ingestion record
    const ingestionId = uuidv4();
    const ingestion = {
      ingestion_id: ingestionId,
      ids,
      priority,
      created_at: new Date().toISOString(),
      status: 'queued'
    };
    
    // Split into batches of max 3 IDs
    const batches = _.chunk(ids, 3).map((batchIds, index) => {
      const batchId = uuidv4();
      return {
        batch_id: batchId,
        ingestion_id: ingestionId,
        ids: batchIds,
        priority: PRIORITY[priority],
        created_at: new Date().toISOString(),
        status: 'queued'
      };
    });
    
    // Save ingestion and batches
    ingestion.batch_ids = batches.map(b => b.batch_id);
    await fs.writeJson(path.join(INGESTIONS_DIR, `${ingestionId}.json`), ingestion);
    await Promise.all(batches.map(batch => 
      fs.writeJson(path.join(BATCHES_DIR, `${batch.batch_id}.json`), batch)
    ));
    
    // Add to global queue
    await queueService.addBatches(batches);
    
    res.json({ ingestion_id: ingestionId });
  } catch (error) {
    console.error('Ingestion error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Status endpoint
app.get('/status/:ingestion_id', async (req, res) => {
  try {
    const { ingestion_id } = req.params;
    
    // Load ingestion
    const ingestionPath = path.join(INGESTIONS_DIR, `${ingestion_id}.json`);
    if (!await fs.pathExists(ingestionPath)) {
      return res.status(404).json({ error: 'Ingestion not found' });
    }
    
    const ingestion = await fs.readJson(ingestionPath);
    
    // Load all batches
    const batches = await Promise.all(
      ingestion.batch_ids.map(async batchId => {
        const batchPath = path.join(BATCHES_DIR, `${batchId}.json`);
        return await fs.readJson(batchPath);
      })
    );
    
    // Determine overall status
    let status = 'yet_to_start';
    if (batches.some(b => b.status === 'triggered')) {
      status = 'triggered';
    } else if (batches.every(b => b.status === 'completed')) {
      status = 'completed';
    } else if (batches.some(b => b.status !== 'yet_to_start')) {
      status = 'triggered';
    }
    
    res.json({
      ingestion_id,
      status,
      batches: batches.map(b => ({
        batch_id: b.batch_id,
        ids: b.ids,
        status: b.status
      }))
    });
  } catch (error) {
    console.error('Status error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});