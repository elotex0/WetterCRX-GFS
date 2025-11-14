import os
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

DATE = os.environ.get("DATE")
RUN = os.environ.get("RUN")

os.makedirs("data/t2m", exist_ok=True)

FIELD_REGEX = r":TMP:2 m above ground:"
MAX_WORKERS = 6  # Anzahl paralleler Downloads (bei Bedarf anpassen)


def fetch_tmp_field(fh):
    fh_padded = f"{fh:03d}"
    base = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{DATE}/{RUN}/atmos"

    idx_url = f"{base}/gfs.t{RUN}z.pgrb2.0p25.f{fh_padded}.idx"
    grib_url = f"{base}/gfs.t{RUN}z.pgrb2.0p25.f{fh_padded}"
    out = f"data/t2m/t2m_{fh_padded}.grib2"

    try:
        print(f"📥 [{fh_padded}] Lade Index …")
        r = requests.get(idx_url, timeout=60)
        r.raise_for_status()
        idx_data = r.text.splitlines()

        all_offsets = []
        tmp_offsets = []
        for line in idx_data:
            parts = line.split(":")
            offset = int(parts[1])
            all_offsets.append(offset)
            if re.search(FIELD_REGEX, line):
                tmp_offsets.append(offset)

        if not tmp_offsets:
            return f"⚠ Keine TMP-Felder in f{fh_padded}"

        head = requests.head(grib_url)
        filesize = int(head.headers["Content-Length"])

        ranges = []
        for start in tmp_offsets:
            candidates = [o for o in all_offsets if o > start]
            end = min(candidates) - 1 if candidates else filesize - 1
            ranges.append((start, end))

        print(f"→ [{fh_padded}] {len(ranges)} Felder → Download …")

        with open(out, "wb") as f:
            for start, end in ranges:
                headers = {"Range": f"bytes={start}-{end}"}
                rr = requests.get(grib_url, headers=headers, stream=True, timeout=120)
                rr.raise_for_status()
                for chunk in rr.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        return f"✔ Fertig f{fh_padded}"

    except Exception as e:
        return f"❌ Fehler f{fh_padded}: {e}"


forecast_hours = list(range(0, 121)) + list(range(123, 385, 3))

print("🚀 Starte parallele Downloads …")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    tasks = {executor.submit(fetch_tmp_field, fh): fh for fh in forecast_hours}

    for future in as_completed(tasks):
        print(future.result())

print("\n🎉 ALLE Downloads fertig!")
