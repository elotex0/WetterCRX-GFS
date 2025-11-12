# scripts/download_geo.py
import os, requests

DATE = os.environ.get("DATE")
RUN = os.environ.get("RUN")

os.makedirs("data/geo", exist_ok=True)

for i in list(range(0, 120)) + list(range(120, 385, 3)):
    i_padded = f"{i:03d}"
    url = (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
        f"?dir=%2Fgfs.{DATE}%2F{RUN}%2Fatmos"
        f"&file=gfs.t{RUN}z.pgrb2.0p25.f{i_padded}"
        f"&var_HGT=on&lev_500_mb=on"
    )
    out = f"data/geo/geo_{i_padded}.grib2"
    print(f"→ Lade {url}")
    try:
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        print(f"✅ Gespeichert: {out}")
    except Exception as e:
        print(f"⚠️ Fehler bei f{i_padded}: {e}")
