BASE = "https://www.povertyactionlab.org"
LIST_URL = "https://www.povertyactionlab.org/evaluations?page={page}"  # si no funciona, cambiamos a "Load more"
START_PAGE = 0
END_PAGE = 1
#END_PAGE = 129  # inclusive
MAX_RETRIES = 4

# timeouts (ms)
NAV_TIMEOUT = 30000
WAIT_TIMEOUT = 20000
NETWORKIDLE_TIMEOUT = 20000

HEADLESS = False
DATA_DIR = "data"