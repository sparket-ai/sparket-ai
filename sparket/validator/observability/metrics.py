"""Central Prometheus metric definitions for the validator.

All instruments live here so they can be imported individually where needed.
Uses a dedicated CollectorRegistry to avoid polluting the default registry.
"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry()

# ---------------------------------------------------------------------------
# Validator Loop
# ---------------------------------------------------------------------------

STEP_DURATION = Histogram(
    "validator_step_duration_seconds",
    "Duration of each validator loop phase",
    labelnames=["phase"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120],
    registry=REGISTRY,
)

STEP_TOTAL = Counter(
    "validator_step_total",
    "Total validator loop iterations",
    registry=REGISTRY,
)

UPTIME = Gauge(
    "validator_uptime_seconds",
    "Seconds since validator started",
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Scoring Pipeline
# ---------------------------------------------------------------------------

SCORING_CYCLE_DURATION = Histogram(
    "scoring_cycle_duration_seconds",
    "Wall time for a full scoring cycle",
    buckets=[1, 5, 10, 30, 60, 120, 300],
    registry=REGISTRY,
)

SCORING_JOB_DURATION = Histogram(
    "scoring_job_duration_seconds",
    "Duration of individual scoring jobs",
    labelnames=["job"],
    buckets=[0.5, 1, 5, 10, 30, 60],
    registry=REGISTRY,
)

SCORING_ERRORS = Counter(
    "scoring_cycle_errors_total",
    "Scoring cycles that failed with an error",
    registry=REGISTRY,
)

SUBMISSIONS_SCORED = Counter(
    "scoring_submissions_scored_total",
    "Total submissions scored (CLV + outcome)",
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Miner Fleet
# ---------------------------------------------------------------------------

MINER_SKILL_SCORE = Gauge(
    "miner_skill_score",
    "Current skill score for a miner",
    labelnames=["uid", "hotkey"],
    registry=REGISTRY,
)

MINER_WEIGHT = Gauge(
    "miner_weight",
    "Current chain weight for a miner",
    labelnames=["uid"],
    registry=REGISTRY,
)

ACTIVE_MINERS = Gauge(
    "active_miners_total",
    "Number of active miners in the metagraph",
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Submission Ingestion
# ---------------------------------------------------------------------------

SUBMISSIONS_RECEIVED = Counter(
    "submissions_received_total",
    "Total submission synapses received",
    registry=REGISTRY,
)

SUBMISSIONS_ACCEPTED = Counter(
    "submissions_accepted_total",
    "Submissions accepted and stored",
    registry=REGISTRY,
)

SUBMISSIONS_REJECTED = Counter(
    "submissions_rejected_total",
    "Submissions rejected (invalid, duplicate, etc.)",
    registry=REGISTRY,
)

SUBMISSION_PROCESSING = Histogram(
    "submission_processing_seconds",
    "Time to process a submission synapse",
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 5],
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Provider / Ingest
# ---------------------------------------------------------------------------

PROVIDER_REQUESTS = Counter(
    "provider_api_requests_total",
    "Provider API requests",
    labelnames=["endpoint"],
    registry=REGISTRY,
)

PROVIDER_ERRORS = Counter(
    "provider_api_errors_total",
    "Provider API errors",
    labelnames=["endpoint"],
    registry=REGISTRY,
)

PROVIDER_LATENCY = Histogram(
    "provider_api_latency_seconds",
    "Provider API call latency",
    labelnames=["endpoint"],
    buckets=[0.1, 0.5, 1, 2, 5, 10],
    registry=REGISTRY,
)

EVENTS_ACTIVE = Gauge(
    "events_active_total",
    "Number of currently active events",
    registry=REGISTRY,
)

MARKETS_ACTIVE = Gauge(
    "markets_active_total",
    "Number of currently active markets",
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Database Pool
# ---------------------------------------------------------------------------

DB_POOL_SIZE = Gauge(
    "db_pool_size",
    "Current database connection pool size",
    registry=REGISTRY,
)

DB_POOL_CHECKED_OUT = Gauge(
    "db_pool_checked_out",
    "Number of connections currently checked out",
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Chain Ops
# ---------------------------------------------------------------------------

WEIGHT_SET_DURATION = Histogram(
    "chain_weight_set_duration_seconds",
    "Time to set weights on chain",
    buckets=[1, 5, 10, 30, 60],
    registry=REGISTRY,
)

METAGRAPH_SYNC_DURATION = Histogram(
    "metagraph_sync_duration_seconds",
    "Time to sync metagraph",
    buckets=[0.5, 1, 5, 10, 30],
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# Worker Health
# ---------------------------------------------------------------------------

WORKER_MEMORY = Gauge(
    "scoring_worker_memory_mb",
    "Scoring worker memory usage in MB",
    labelnames=["worker_id"],
    registry=REGISTRY,
)

WORKER_JOBS_COMPLETED = Counter(
    "scoring_worker_jobs_completed_total",
    "Jobs completed by scoring workers",
    labelnames=["worker_id"],
    registry=REGISTRY,
)

# ---------------------------------------------------------------------------
# DataSyncer
# ---------------------------------------------------------------------------

SYNC_PUSH_DURATION = Histogram(
    "sync_push_duration_seconds",
    "Time to push data to dashboard API",
    labelnames=["payload_type"],
    buckets=[0.5, 1, 2, 5, 10],
    registry=REGISTRY,
)

SYNC_PUSH_ERRORS = Counter(
    "sync_push_errors_total",
    "Failed sync pushes to dashboard API",
    labelnames=["payload_type"],
    registry=REGISTRY,
)
