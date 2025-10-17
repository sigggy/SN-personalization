"""Runtime configuration for the Manifold ETL pipeline."""

# Logging
LOG_DIR = "logs"

# User ingestion
USER_LIMIT = None  # Set to an int to cap ingested users
USER_PAGE_SIZE = 500
CHUNK_SIZE = 200

# Bets ingestion
BET_USER_CHUNK_SIZE = 500
BET_WORKER_COUNT = 4
BET_MAX_PARAMS_PER_STATEMENT = 60000

API_URL = "https://api.manifold.markets/v0/bets"
THRESHOLD = 50
LIMIT = 1000

# Contract/comment update configuration
CONTRACT_USER_LIMIT = 25
COMMENT_USER_LIMIT = 25
CONTRACT_PAGE_LIMIT = 1000
COMMENT_PAGE_LIMIT = 1000

# HTTP client
API_TIMEOUT = 30  # seconds
API_MAX_RETRIES = 7
API_BACKOFF_FACTOR = 1.75
