import os
import requests
import concurrent.futures
from subprocess import Popen, PIPE

DATE = os.environ["DATE"]
RUN = os.environ["RUN"]

os.makedirs("data/pmsl", exist_ok=True)

FORECASTS = list(range(0, 120)) + list(range(120, 385, 3))


def download_and_extract(i):
    """Streamt GFS direkt in wgrib2 und extrahiert PRMSL ohne Zwischenspeichern."""
    i_padded = f"{i:03d}"

    url = (
        f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/"
        f"gfs.{DATE}/{RUN}/atmos/gfs.t{RUN}z.pgrb2.0p25.f{i_padded}"
    )

    out_path = f"data/pmsl/pmsl_{i_padded}.grib2"

    if os.path.exists(out_path):
        return f"⏩ {i_padded}: existiert schon"

    try:
        r = requests.get(url, stream=True, timeout=120)
        if r.status_code != 200:
            return f"❌ {i_padded}: nicht vorhanden"
    except Exception as e:
        return f"⚠️ {i_padded}: Download-Fehler {e}"

    # wgrib2-Prozess starten (liest von STDIN!)
    p = Popen(
        ["wgrib2", "-", "-match", "PRMSL", "-grib", out_path],
        stdin=PIPE, stdout=PIPE, stderr=PIPE
    )

    # Chunk für Chunk direkt in wgrib2 feeden
    try:
        for chunk in r.iter_content(1024 * 128):  # 128 KB Blöcke
            p.stdin.write(chunk)
        p.stdin.close()
        p.wait()
    except Exception as e:
        return f"⚠️ {i_padded}: Pipe-Fehler {e}"

    # prüfen, ob Datei Inhalt hat
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        if os.path.exists(out_path):
            os.remove(out_path)
        return f"⚠️ {i_padded}: Keine PRMSL-Daten"

    return f"✅ {i_padded}: PRMSL gespeichert"


if __name__ == "__main__":
    print("Starte PRMSL-Downloads per Streaming Pipe …")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        results = ex.map(download_and_extract, FORECASTS)

    for r in results:
        print(r)

    print("✨ Fertig.")