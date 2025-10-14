# Comoda Backfill

Data backfill services and batch processing jobs for the Comoda platform.

## Overview

This repository contains data backfill scripts, batch processing jobs, and data migration tools for the Comoda platform. It handles historical data ingestion, data corrections, and large-scale data processing tasks.

## Tech Stack

- **Language**: Python 3.9+
- **Task Queue**: Celery / Apache Airflow / Prefect
- **Database**: PostgreSQL / MongoDB
- **Message Broker**: Redis / RabbitMQ
- **Data Processing**: Pandas, Dask, Apache Spark
- **Scheduling**: Cron / Apache Airflow
- **Monitoring**: Prometheus + Grafana
- **Containerization**: Docker

## Project Structure

```
backfill/
├── jobs/                  # Backfill job definitions
│   ├── crypto/           # Cryptocurrency data backfills
│   ├── user/             # User data migrations
│   └── system/           # System maintenance jobs
├── processors/           # Data processing modules
│   ├── extractors/       # Data extraction logic
│   ├── transformers/     # Data transformation
│   └── loaders/          # Data loading utilities
├── schedulers/           # Job scheduling configurations
├── monitors/             # Job monitoring and alerting
├── utils/                # Utility functions and helpers
├── configs/              # Configuration files
├── tests/                # Test suite
├── requirements.txt      # Python dependencies
├── Dockerfile           # Docker configuration
├── docker-compose.yml   # Local development setup
└── README.md            # This file
```

## Getting Started

### Prerequisites

- Python 3.9+
- PostgreSQL / MongoDB
- Redis (for Celery)
- Docker (optional)

### Local Development Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd backfill
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. Start required services:
   ```bash
   # Start Redis (for Celery)
   redis-server
   
   # Start PostgreSQL (if running locally)
   pg_ctl start
   ```

6. Run database migrations:
   ```bash
   python manage.py migrate
   ```

### Docker Setup

1. Start all services with Docker Compose:
   ```bash
   docker-compose up --build
   ```

## Job Types

### 1. Historical Data Backfill

Backfill historical cryptocurrency data:

```bash
python -m jobs.crypto.price_backfill \
  --start-date 2020-01-01 \
  --end-date 2023-12-31 \
  --symbols BTC,ETH,ADA
```

### 2. Data Migration

Migrate user data between database schemas:

```bash
python -m jobs.user.migrate_user_data \
  --source-db legacy_db \
  --target-db new_db \
  --batch-size 1000
```

### 3. Data Correction

Fix data inconsistencies:

```bash
python -m jobs.system.data_correction \
  --job-type price_anomaly_fix \
  --date-range 2023-01-01,2023-12-31
```

## Scheduling

### Celery Tasks

Start Celery worker:

```bash
celery -A backfill worker --loglevel=info
```

Start Celery beat scheduler:

```bash
celery -A backfill beat --loglevel=info
```

Monitor tasks:

```bash
celery -A backfill flower
# Access web interface at http://localhost:5555
```

### Cron Jobs

Example cron configuration:

```bash
# Daily backfill at 2 AM
0 2 * * * /path/to/venv/bin/python /path/to/backfill/jobs/daily_sync.py

# Weekly data validation at midnight on Sunday
0 0 * * 0 /path/to/venv/bin/python /path/to/backfill/jobs/weekly_validation.py
```

### Airflow DAGs

For complex workflows, use Apache Airflow:

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    'crypto_data_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1)
)

# Define tasks...
```

## Configuration

### Environment Variables

Key environment variables (see `.env.example`):

- `DATABASE_URL`: Primary database connection
- `REDIS_URL`: Redis connection for Celery
- `CRYPTO_API_KEY`: Cryptocurrency data API key
- `BATCH_SIZE`: Default batch processing size
- `MAX_RETRIES`: Maximum job retry attempts
- `LOG_LEVEL`: Logging verbosity level

### Job Configuration

Jobs are configured via YAML files in `configs/`:

```yaml
# configs/price_backfill.yaml
job_name: "crypto_price_backfill"
source:
  api_endpoint: "https://api.cryptocompare.com/data/v2/histoday"
  rate_limit: 100  # requests per minute
target:
  database: "comoda_main"
  table: "crypto_prices"
batch_size: 1000
retry_policy:
  max_retries: 3
  backoff_factor: 2
```

## Data Sources

The backfill system integrates with:

- **Cryptocurrency APIs**:
  - CoinGecko
  - CoinMarketCap
  - Binance API
  - Coinbase Pro API
- **Financial Data**:
  - Alpha Vantage
  - Yahoo Finance
  - Quandl
- **Internal Systems**:
  - Application databases
  - User activity logs
  - Transaction records

## Monitoring & Logging

### Job Monitoring

Monitor job status and performance:

```bash
# View active jobs
python -m monitors.job_status

# Check job history
python -m monitors.job_history --days 7

# Generate performance report
python -m monitors.performance_report
```

### Logging

Centralized logging configuration:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backfill.log'),
        logging.StreamHandler()
    ]
)
```

### Alerting

Set up alerts for job failures:

```bash
# Slack notifications
python -m monitors.slack_alerts --webhook-url $SLACK_WEBHOOK

# Email notifications
python -m monitors.email_alerts --smtp-config configs/smtp.yaml
```

## Error Handling & Recovery

### Retry Logic

Jobs implement exponential backoff retry logic:

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def process_batch(batch_data):
    # Processing logic here
    pass
```

### Data Validation

Validate data integrity after backfill:

```bash
python -m utils.data_validator \
  --table crypto_prices \
  --start-date 2023-01-01 \
  --end-date 2023-12-31
```

### Recovery Procedures

Resume failed jobs:

```bash
python -m utils.job_recovery \
  --job-id 12345 \
  --resume-from-checkpoint
```

## Performance Optimization

### Database Optimization

- Use bulk inserts for large datasets
- Implement proper indexing strategies
- Use connection pooling
- Optimize query patterns

### Memory Management

- Process data in chunks/batches
- Use generators for large datasets
- Monitor memory usage
- Implement garbage collection strategies

### Parallel Processing

```python
from multiprocessing import Pool

def process_symbol(symbol):
    # Process single cryptocurrency symbol
    pass

symbols = ['BTC', 'ETH', 'ADA', 'DOT']
with Pool(processes=4) as pool:
    pool.map(process_symbol, symbols)
```

## Testing

Run the test suite:

```bash
pytest tests/
```

Test specific components:

```bash
# Test data processors
pytest tests/test_processors.py

# Test job execution
pytest tests/test_jobs.py

# Integration tests
pytest tests/integration/
```

## Deployment

### Production Deployment

Deploy using Docker:

```bash
docker build -t comoda-backfill .
docker run -d \
  --name backfill-worker \
  -e DATABASE_URL=$DATABASE_URL \
  -e REDIS_URL=$REDIS_URL \
  comoda-backfill
```

### Scaling

Scale workers horizontally:

```bash
# Scale Celery workers
docker-compose up --scale worker=5

# Use Kubernetes for auto-scaling
kubectl apply -f k8s/backfill-deployment.yaml
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b backfill/feature-name`
3. Make your changes and add tests
4. Run the test suite: `pytest`
5. Run linting: `flake8 .`
6. Commit your changes: `git commit -am 'Add backfill feature'`
7. Push to the branch: `git push origin backfill/feature-name`
8. Create a Pull Request

## Troubleshooting

### Common Issues

#### Memory Issues
```bash
# Monitor memory usage
python -m utils.memory_monitor --job-id 12345

# Reduce batch size
export BATCH_SIZE=500
```

#### Database Connection Issues
```bash
# Test database connectivity
python -m utils.db_health_check

# Reset connection pool
python -m utils.reset_connections
```

#### API Rate Limiting
```bash
# Check rate limit status
python -m utils.rate_limit_checker --api cryptocompare

# Adjust rate limits in config
vim configs/api_limits.yaml
```

## License

[License information to be added]