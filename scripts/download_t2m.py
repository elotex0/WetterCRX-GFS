import os
import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

DATE = os.environ.get("DATE")
RUN = os.environ.get("RUN")

os.makedirs("data/t2m", exist_ok=True)

FIELD_REGEX = r":\s*T2M:2 m above ground"
MAX_WORKERS = 6  # Anzahl paralleler Downloads (anpassbar)
RETRY_LIMIT = 3  # erneute Versuche falls Fehler


def fetch_t2m(fh):
    fh_padded = f"{fh:03d}"

    base = f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{DATE}/{RUN}/atmos"
    idx_url = f"{base}/gfs.t{RUN}z.pgrb2.0p25.f{fh_padded}.idx"
    grib_url = f"{base}/gfs.t{RUN}z.pgrb2.0p25.f{fh_padded}"

    out = f"data/t2m/t2m_{fh_padded}.grib2"

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            print(f"📥 [{fh_padded}] Index laden…")
            r = requests.get(idx_url, timeout=20)
            if r.status_code != 200:
                return f"⚠️ [{fh_padded}] idx fehlt → übersprungen"
            idx_data = r.text.splitlines()

            all_offsets = []
            t2m_offsets = []

            for line in idx_data:
                if ":" not in line:
                    continue
                parts = line.split(":")
                if len(parts) < 3 or not parts[1].isdigit():
                    continue

                offset = int(parts[1])
                all_offsets.append(offset)

                if re.search(FIELD_REGEX, line):
                    t2m_offsets.append(offset)

            if not t2m_offsets:
                return f"⚠️ [{fh_padded}] Kein T2M gefunden"

            head = requests.head(grib_url, timeout=10)
            filesize = int(head.headers.get("Content-Length", 0))

            ranges = []
            for start in t2m_offsets:
                nxt = [o for o in all_offsets if o > start]
                end = min(nxt) - 1 if nxt else filesize - 1
                ranges.append((start, end))

            print(f"→ [{fh_padded}] {len(ranges)} Felder → Download…")

            with open(out, "wb") as f:
                for start, end in ranges:
                    headers = {"Range": f"bytes={start}-{end}"}
                    rr = requests.get(grib_url, headers=headers, stream=True, timeout=60)
                    rr.raise_for_status()
                    for chunk in rr.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)

            return f"✔ [{fh_padded}] Gespeichert"

        except Exception as e:
            print(f"❌ [{fh_padded}] Fehler (Versuch {attempt}/{RETRY_LIMIT}): {e}")
            time.sleep(5 * attempt)

    return f"💥 [{fh_padded}] endgültig fehlgeschlagen"


# Forecast-Stunden: 0–120 alle 1h, danach 3h-Raster
forecast_hours = list(range(0, 121)) + list(range(123, 385, 3))

print(f"🚀 Starte parallele AWS-Downloads ({MAX_WORKERS} Worker)…")

tasks = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for fh in forecast_hours:
        tasks.append(executor.submit(fetch_t2m, fh))

    for future in as_completed(tasks):
        print(future.result())

print("\n🎉 COMPLETED: T2M-Downloads abgeschlossen!")
