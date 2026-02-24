import time
from playwright.sync_api import TimeoutError as PWTimeoutError
from config import MAX_RETRIES

RETRY_EVENTS = 0

# ejecución de función con retries (network timing)
def run_function_with_retries(callable_fn, *, tries=MAX_RETRIES, base_sleep=1.0, label="op"):
    global RETRY_EVENTS
    last_err = None
    for attempt in range(1, tries + 1):
        try:
            return callable_fn()
        except (PWTimeoutError, Exception) as e:
            last_err = e
            RETRY_EVENTS += 1
            sleep_s = base_sleep * (2 ** (attempt - 1))
            print(f"[retry] {label} failed (attempt {attempt}/{tries}): {type(e).__name__} -> sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise last_err

# busqueda de texto con timeout
def safe_text(locator) -> str:
    try:
        t = locator.first.inner_text(timeout=2000)
        return " ".join(t.split())
    except Exception:
        return ""

def collapse_ws(s: str) -> str:
    return " ".join(s.split())

def normalize_multival(s: str) -> str:
    parts = [p.strip() for p in s.split(";") if p.strip()]
    parts.sort()
    return "; ".join(parts)