# ETL Platform

A Python-based ETL (Extract, Transform, Load) system that copies data from PostgreSQL to MySQL with some data transformation and PII masking.

## 🚀 Quick Start

```bash
# 1. Install
pip install -r requirements.txt

# 2. Start databases
docker-compose up -d

# 3. Run ETL
python extract.py 1
```

## 🔧 How It Works

```
PostgreSQL → Staging → Transform  → MySQL
```

## 🌐 Web Interface

```bash
uvicorn fast:app --reload
```
- UI: http://localhost:8000
- API: http://localhost:8000/docs

## 🛠️ Commands

```bash
open index.html with some webserver eg.live-server
```

## ⚙️ Database Info

**PostgreSQL**: localhost:5432 (user: postgres, pass: admin)  
**MySQL**: localhost:3307 (user: mysql, pass: pass)

