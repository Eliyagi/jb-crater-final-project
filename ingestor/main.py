"""Python Ingestor pipeline for the Crater streaming project.

Polls the upstream gh-archive-vendor mock, handles chaotic network behaviors
(503 outages, 404 delays, truncated gzip payloads), and streams verified 
JSONL lines into Kafka.
"""
from __future__ import annotations

import os
import time
import sys
import gzip
import json
from datetime import datetime, timedelta, timezone
from io import BytesIO
import requests
from kafka import KafkaProducer

# ───────────────────────── Configuration ─────────────────────────────────────

VENDOR_URL = os.environ.get("VENDOR_URL", "http://gh-archive-vendor:8000")
KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "gh-archive-events")

# State management: base timeline tracking
START_DATE = datetime(2024, 1, 15, 0, tzinfo=timezone.utc)
STATE_FILE = "/tmp/ingestor_high_water_mark.json"
HISTORY_FILE = "/tmp/ingestor_history.jsonl"

def log(msg: str, level: str = "INFO") -> None:
    print(f"[{datetime.utcnow().isoformat()}Z] [{level}] {msg}", flush=True)

# ───────────────────────── High Water Mark State ─────────────────────────────

def load_current_hour() -> datetime:
    """Load the progress timestamp or fallback to baseline start."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                return datetime.fromisoformat(data["current_hour"]).replace(tzinfo=timezone.utc)
        except Exception as e:
            log(f"Failed to load state file, resetting to baseline: {e}", "WARN")
    return START_DATE

def save_current_hour(current_hour: datetime) -> None:
    """Persist high-water mark across container restarts."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"current_hour": current_hour.isoformat()}, f)
    except Exception as e:
        log(f"Failed to persist high-water mark: {e}", "ERROR")

def save_hour_history(filename: str, status: str, error: str = None) -> None:
    """Stores a record of each file attempt in a JSONL history file."""
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "filename": filename,
        "status": status,
        "error": error
    }
    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

# ───────────────────────── Chaos-Resilient Processor ──────────────────────────

def process_and_stream(file_bytes: bytes, producer: KafkaProducer) -> bool:
    """Decompresses the gzipped payload, validates lines, and pushes to Kafka.
    
    Returns True only if the entire file was successfully processed without truncation.
    """
    try:
        # Wrap bytes in memory stream to unpack Gzip safely
        with gzip.GzipFile(fileobj=BytesIO(file_bytes)) as gz:
            lines_sent = 0
            
            for line in gz:
                # Truncated chaos check: If a line is cut off, json.loads will fail.
                # Real production engines trap this to guarantee stream line integrity.
                try:
                    event = json.loads(line.decode("utf-8"))
                    
                    # Push directly into the architecture's message bus
                    producer.send(
                        KAFKA_TOPIC, 
                        key=str(event.get("id")).encode("utf-8"),
                        value=line # Send original raw bytes to minimize transformation overhead
                    )
                    lines_sent += 1
                except json.JSONDecodeError:
                    log("Detected truncated or corrupted line. File payload was cut by chaos.", "ERROR")
                    return False # Tell loop to retry this hour due to incomplete read
                except Exception as e:
                    log(f"Internal messaging failure: {e}", "CRITICAL")
                    return False
            
            producer.flush()
            log(f"Successfully streamed {lines_sent:,} clean events to Kafka.")
            return True
            
    except gzip.BadGzipFile:
        log("Gzip header corruption detected. File is unreadable due to upstream chaos.", "ERROR")
        return False
    except Exception as e:
        log(f"Stream decompress error: {e}", "ERROR")
        return False

# ───────────────────────── Ingestion Loop ─────────────────────────────────────

def main() -> None:
    log("Initializing Crater Ingestor pipeline...")
    
    # Wait for Kafka Broker to be accessible before initiating loops
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                acks="all", # Guarantee durability under heavy analytical loads
                max_in_flight_requests_per_connection=1,
                retries=5
            )
            log("Connected to Kafka Broker infrastructure successfully.")
        except Exception:
            log("Kafka cluster not ready yet, retrying in 3 seconds...", "WARN")
            time.sleep(3)

    current_hour = load_current_hour()
    retries = 0
    MAX_RETRIES = 5

    while True:
        # Format the file path to match the teacher's API route contract: {YYYY-MM-DD-H}.json.gz
        filename = f"{current_hour.strftime('%Y-%m-%d')}-{current_hour.hour}.json.gz"
        target_url = f"{VENDOR_URL}/{filename}"
        
        log(f"Probing upstream for target block: {filename}")
        
        try:
            response = requests.get(target_url, timeout=30)
            
            # Scenario A: 200 OK — Data retrieved successfully
            if response.status_code == 200:
                payload = response.content
                
                # Try to clean, unpack, and push to Kafka.
                if process_and_stream(payload, producer):
                    # Only move high-water mark forward if file was 100% clean and fully read
                    current_hour += timedelta(hours=1)
                    save_current_hour(current_hour)
                    save_hour_history(filename, "SUCCESS")
                    retries = 0 # Reset retry counter after successful processing
                else:
                    raise Exception("Truncated or corrupted payload detected.")
            # Scenarios B & C & Others -> Forward to except block for Backoff
            else:
                raise Exception(f"Upstream returned status code: {response.status_code}")


        except Exception as e:
            retries += 1
            
            # 404 is expected behavior (simulation clock), others are true errors
            if "404" in str(e):
                log(f"Target hour {filename} not generated yet (404). Retrying in 5s.")
                wait_time = 5
            else:
                wait_time = min(60, (2 ** retries))
                log(f"Attempt {retries} failed: {e}. Retrying in {wait_time}s...", "WARN")
            
            save_hour_history(filename, "FAILURE", str(e))
            time.sleep(wait_time)
            
            if retries >= MAX_RETRIES:
                log("Max retries reached. Resetting counter and waiting...", "ERROR")
                retries = 0
                time.sleep(60) 


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Ingestor shutdown sequence completed gracefully.")
        sys.exit(0)