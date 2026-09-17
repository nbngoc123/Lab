# lake/http.py
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "football-lake/0.1 (educational project)"})


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.RequestException,)),
    reraise=True,
)
def get(url, **kw):
    r = SESSION.get(url, timeout=kw.pop("timeout", 30), **kw)
    r.raise_for_status()
    return r
