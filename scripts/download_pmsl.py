import os
import time
import requests

DATE = os.environ.get("DATE")
RUN  = os.environ.get("RUN")

os.makedirs("data/pmsl", exist_ok=True)

# --------------------------------------------------------
# Robuster Download mit Retry
# --------------------------------------------------------
def download_with_retry(url, out, retries=5, wait=5):
    for attempt in range(1, retries + 1):
        print(f"   Versuch {attempt}/{retries} …")

        try:
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()

            # Datei schreiben
            with open(out, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)

            # GFS liefert manchmal 0–1 KB -> fehlerhaft
            if os.path.getsize(out) < 5000:
                print("   ⚠️ Datei verdächtig klein – neuer Versuch …")
                continue

            print(f"   ✅ Erfolgreich gespeichert: {out}")
            return True

        except Exception as e:
            print(f"   ⚠️ Fehler: {e}")

        time.sleep(wait)

    print(f"   ❌ Fehlgeschlagen nach {retries} Versuchen: {out}")
    return False


# --------------------------------------------------------
# Liste aller Dateien erzeugen
# --------------------------------------------------------
files = list(range(0, 120)) + list(range(120, 385, 3))

failed = []


# --------------------------------------------------------
# ERSTE RUNDE: normal downloaden
# --------------------------------------------------------
print("\n🚀 Starte Downloads …\n")

for i in files:
    i_padded = f"{i:03d}"
    url = (
        f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
        f"?dir=%2Fgfs.{DATE}%2F{RUN}%2Fatmos"
        f"&file=gfs.t{RUN}z.pgrb2.0p25.f{i_padded}"
        f"&var_PRMSL=on&lev_mean_sea_level=on"
    )
    out = f"data/pmsl/pmsl_{i_padded}.grib2"

    print(f"→ Lade {url}")

    ok = download_with_retry(url, out)
    if not ok:
        failed.append((url, out))

# --------------------------------------------------------
# ZWEITE RUNDE: Nochmals probieren
# --------------------------------------------------------
if failed:
    print("\n🔄 Zweiter Versuch für fehlgeschlagene Dateien …\n")
    retry_fail = []

    for url, out in failed:
        print(f"→ Zweiter Versuch für {out}")
        ok = download_with_retry(url, out)
        if not ok:
            retry_fail.append((url, out))

    failed = retry_fail


# --------------------------------------------------------
# ENDERGEBNIS
# --------------------------------------------------------
print("\n---------------------------------------------")
if failed:
    print("❌ Manche Dateien konnten NICHT geladen werden:")
    for _, out in failed:
        print("    -", out)
else:
    print("✅ Alle Downloads erfolgreich!")
print("---------------------------------------------\n")
