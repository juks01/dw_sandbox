import time
import urllib.request
from datetime import datetime, timezone

URL = "https://dummyjson.com/products"

request = urllib.request.Request(
    URL,
    headers={
        "User-Agent": "dw-dev/1.0",
    },
)

data = None
last_error = None

for attempt in range(1, 4):
    try:
        with urllib.request.urlopen(
            request,
            timeout=30,
        ) as response:
            data = response.read()

        break

    except Exception as exc:
        last_error = exc
        print(
            f"EXTRACT attempt {attempt} failed: {exc}",
            flush=True,
        )
        time.sleep(2 * attempt)

if data is None:
    raise SystemExit(
        f"EXTRACT FAILED after 3 attempts: {last_error}"
    )

filename = datetime.now(
    timezone.utc
).strftime(
    "%Y%m%dT%H%M%S%fZ.json"
)

with open(
    "/landing/" + filename,
    "wb",
) as handle:
    handle.write(data)

print(
    "EXTRACT OK:",
    filename,
    flush=True,
)
