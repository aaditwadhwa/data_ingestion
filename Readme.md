# 📥 Data Ingestion API System

A lightweight backend service built using **Node.js + Express** that simulates a data ingestion pipeline. It handles priority-based batch ingestion, processes data asynchronously in the background, and respects a **rate limit of 1 batch every 5 seconds**. Status of each batch is stored using the local file system for easy tracking.

---

## 🚀 Features

- ✅ Accepts batch ingestion requests with priority
- 🔄 Processes batches in the background (non-blocking)
- ⏳ Rate-limited: 1 batch per 5 seconds
- 📁 Uses filesystem for persistence (no database needed)
- 📊 Check status of any batch via API

---

## 🧠 Why This Approach?

When designing this system, I wanted to keep it simple, beginner-friendly, and easily deployable. So I chose:

- **Express.js** for building APIs quickly  
- **File system** for storing data instead of a full-blown DB  
- **Custom queue system** to handle priority + rate limiting logic  
- **Async processing** to keep the server responsive

---
