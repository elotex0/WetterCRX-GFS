import os
import time
import requests

DATE = os.environ.get("DATE", "20251113")  # z.B. "20251113"
RUN  = os.environ.get("RUN", "00")        # z.B. "00"

os.makedirs("data/pmsl", exist_ok=True)

def download_with_retry(url, out, retries=5, wait=5):
    for attempt in range(1, retries + 1):
        print(f"   Versuch {attempt}/{retries} …")

        try:
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()

            # Datei speichern
            with open(out, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)

            # Prüfen ob Datei nicht leer ist
            if os.path.getsize(out) < 5000:  # 5 KB als Minimum
                print("   ⚠️ Datei ist verdächtig klein – neuer Versuch …")
                continue

            print(f"   ✅ Erfolgreich gespeichert: {out}")
            return True

        except Exception as e:
            print(f"   ⚠️ Fehler: {e}")

        time.sleep(wait)

    print(f"   ❌ Aufgabe gescheitert nach {retries} Versuchen: {out}")
    return False


for i in list(range(0, 120)) + list(range(120, 385, 3)):
    i_padded = f"{i:03d}"
    url = (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
        f"?dir=%2Fgfs.{DATE}%2F{RUN}%2Fatmos"
        f"&file=gfs.t{RUN}z.pgrb2.0p25.f{i_padded}"
        f"&var_PRMSL=on&lev_mean_sea_level=on"
    )
    out = f"data/pmsl/pmsl_{i_padded}.grib2"

    print(f"→ Lade {url}")

    download_with_retry(url, out)
print("✅ Alle Downloads abgeschlossen.")
