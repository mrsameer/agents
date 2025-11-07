# Quick Start Guide

Get the Disaster Data Kafka Consumer up and running in 5 minutes!

## Option 1: Quick Start with Docker (Recommended)

### Step 1: Start Infrastructure

```bash
# Start PostgreSQL and Kafka
docker-compose up -d

# Wait for services to be healthy (about 30 seconds)
docker-compose ps
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Initialize Database

```bash
python -c "from agent import initialize_database; initialize_database()"
```

### Step 4: Run Test

```bash
# Test with mock data (no Kafka messages needed)
python test_consumer.py
```

### Step 5: Run Consumer

```bash
# One-shot mode
python agent.py

# Or continuous mode
python -c "
import time
from agent import run_disaster_consumer

while True:
    result = run_disaster_consumer(batch_size=10)
    print(f'Stored: {result[\"stored_count\"]}')
    time.sleep(5)
"
```

### Step 6: Query Data

```bash
python -c "
from agent import get_statistics_summary
stats = get_statistics_summary()
print(f'Total events: {stats[\"total_events\"]}')
print(f'By type: {stats[\"by_disaster_type\"]}')
"
```

### Access Web UIs

- **Kafka UI**: http://localhost:8080
- **pgAdmin**: http://localhost:5050 (email: admin@disaster.com, password: admin)

---

## Option 2: Manual Setup (Without Docker)

### Step 1: Install PostgreSQL

```bash
# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib

# macOS
brew install postgresql

# Start PostgreSQL
sudo systemctl start postgresql  # Linux
brew services start postgresql   # macOS
```

### Step 2: Create Database

```bash
sudo -u postgres psql
```

```sql
CREATE DATABASE disaster_data;
CREATE USER disaster_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE disaster_data TO disaster_user;
\q
```

### Step 3: Install Kafka

```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_3.6.0-2.13.tgz
tar -xzf kafka_3.6.0-2.13.tgz
cd kafka_3.6.0-2.13

# Start Kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

### Step 4: Create Kafka Topic

```bash
bin/kafka-topics.sh --create \
  --topic disaster-data-ingestion \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Step 5: Set Environment Variables

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=disaster_data
export POSTGRES_USER=disaster_user
export POSTGRES_PASSWORD=your_password

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=disaster-data-ingestion
```

### Step 6: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 7: Initialize and Run

```bash
# Initialize database
python -c "from agent import initialize_database; initialize_database()"

# Run tests
python test_consumer.py

# Run consumer
python agent.py
```

---

## Option 3: Test with Mock Data Only (No Infrastructure)

Perfect for testing without Kafka or when infrastructure is not available.

```bash
# Install only Python dependencies
pip install langgraph psycopg2-binary

# Run with mock data
python -c "
from agent import consume_kafka_messages
messages = consume_kafka_messages(use_mock=True)
print(f'Mock messages: {len(messages)}')
for msg in messages[:2]:
    print(f'  - {msg[\"packet_id\"]}: {msg[\"metadata\"][\"disaster_type\"]}')
"
```

---

## Verify Installation

### Check PostgreSQL

```bash
psql -h localhost -U postgres -d disaster_data -c "SELECT COUNT(*) FROM disaster_events;"
```

### Check Kafka

```bash
# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Check consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 --group disaster-consumer-group --describe
```

### Check Data

```python
from agent import get_statistics_summary

stats = get_statistics_summary()
print(f"Events in database: {stats['total_events']}")
```

---

## Example Workflow

### 1. Producer Side (Disaster Data Collection Agent)

```python
# In google_adk/5_disaster_data_agent
from agent import collect_and_process_disaster_data

# Collect disaster data and generate Kafka packets
result = collect_and_process_disaster_data(
    disaster_type="floods",
    max_urls=5
)

# This produces packets to Kafka topic: disaster-data-ingestion
```

### 2. Consumer Side (This Agent)

```python
# In langraph/1_disaster_kafka_consumer
from agent import run_disaster_consumer

# Consume and store in PostgreSQL
result = run_disaster_consumer(batch_size=10)
print(f"Stored {result['stored_count']} events")
```

### 3. Query Data

```python
from agent import query_disasters_by_location

# Query events in Kerala
events = query_disasters_by_location("Kerala")
print(f"Found {len(events)} events in Kerala")
```

---

## Troubleshooting

### Issue: Cannot connect to PostgreSQL

```bash
# Check if PostgreSQL is running
docker ps | grep postgres  # If using Docker
sudo systemctl status postgresql  # If installed locally

# Test connection
psql -h localhost -U postgres -d disaster_data
```

### Issue: Cannot connect to Kafka

```bash
# Check if Kafka is running
docker ps | grep kafka  # If using Docker

# Test connection
telnet localhost 9092
```

### Issue: Database tables not created

```bash
python -c "from agent import initialize_database; initialize_database()"
```

### Issue: No messages in Kafka

Use mock mode for testing:
```python
from agent import run_disaster_consumer
# This will use mock data automatically if Kafka has no messages
result = run_disaster_consumer(batch_size=10)
```

---

## Next Steps

1. **Run Continuous Consumer**: Keep the consumer running to process incoming Kafka messages
2. **Set Up Producer**: Configure the disaster data collection agent to produce messages
3. **Create Dashboards**: Build visualization dashboards using the PostgreSQL data
4. **Add Monitoring**: Set up alerts for critical disaster events
5. **Scale Up**: Deploy multiple consumers for parallel processing

---

## Useful Commands

### Docker Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Restart a service
docker-compose restart postgres
docker-compose restart kafka
```

### Database Commands

```sql
-- View all events
SELECT * FROM disaster_events ORDER BY event_start_date DESC LIMIT 10;

-- Count by disaster type
SELECT disaster_type, COUNT(*) FROM disaster_events GROUP BY disaster_type;

-- Recent high-severity events
SELECT * FROM disaster_events WHERE severity = 'high' ORDER BY event_start_date DESC LIMIT 5;

-- Events with casualties
SELECT disaster_type, primary_location, deaths, injured
FROM disaster_events
WHERE deaths > 0 OR injured > 0
ORDER BY deaths DESC;
```

### Kafka Commands

```bash
# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics --describe --topic disaster-data-ingestion --bootstrap-server localhost:9092

# Consume messages (for debugging)
kafka-console-consumer --topic disaster-data-ingestion --from-beginning --bootstrap-server localhost:9092

# Consumer group info
kafka-consumer-groups --bootstrap-server localhost:9092 --group disaster-consumer-group --describe
```

---

## Support

For issues or questions:
- Check logs: `docker-compose logs -f`
- Run tests: `python test_consumer.py`
- Review README.md for detailed documentation
- Use mock mode for testing without infrastructure

Happy consuming! 🚀
