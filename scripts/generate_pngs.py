import sys
import cfgrib
import pandas as pd
import os
import struct
import zlib
from zoneinfo import ZoneInfo
from scipy.interpolate import RegularGridInterpolator
import numpy as np
import gc
import matplotlib
matplotlib.use("Agg")  # headless, kein Display nötig
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm, LinearSegmentedColormap
import matplotlib.colors as mcolors
from PIL import Image
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# ------------------------------
# Eingabe-/Ausgabe
# ------------------------------
data_dir = sys.argv[1]        # z.B. "output"
output_dir = sys.argv[2]      # z.B. "output/maps"
var_type = sys.argv[3]        # 't2m', 'geo', 'pmsl', 'snow', 'tp_acc'
os.makedirs(output_dir, exist_ok=True)

# ------------------------------
# Temperatur-Farben (unverändert aus Dokument 1)
# ------------------------------
t2m_bounds = list(range(-36, 50, 2))
t2m_colors = LinearSegmentedColormap.from_list(
    "t2m_smoooth",
    [
        "#F675F4", "#F428E9", "#B117B5", "#950CA2", "#640180",
        "#3E007F", "#00337E", "#005295", "#1292FF", "#49ACFF",
        "#8FCDFF", "#B4DBFF", "#B9ECDD", "#88D4AD", "#07A125",
        "#3FC107", "#9DE004", "#E7F700", "#F3CD0A", "#EE5505",
        "#C81904", "#AF0E14", "#620001", "#C87879", "#FACACA",
        "#E1E1E1", "#6D6D6D"
    ],
    N=len(t2m_bounds)
)
t2m_norm = BoundaryNorm(t2m_bounds, ncolors=len(t2m_bounds))

# ------------------------------
# Aufsummierter Niederschlag (tp_acc, unverändert aus Dokument 1)
# ------------------------------
tp_acc_bounds = [0.0, 0.1, 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100,
                  125, 150, 175, 200, 250, 300, 400, 500]
tp_acc_colors = ListedColormap([
    "#B4D7FF", "#75BAFF", "#349AFF", "#0582FF", "#0069D2",
    "#003680", "#148F1B", "#1ACF06", "#64ED07", "#FFF32B",
    "#E9DC01", "#F06000", "#FF7F26", "#FFA66A", "#F94E78",
    "#F71E53", "#BE0000", "#880000", "#64007F", "#C201FC",
    "#DD66FE", "#EBA6FF", "#F9E7FF", "#D4D4D4", "#969696"
])
tp_acc_colors.set_under(alpha=0)
tp_acc_norm = mcolors.BoundaryNorm(tp_acc_bounds, tp_acc_colors.N)

# ------------------------------
# Windböen-Farben
# ------------------------------
wind_bounds = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 180, 200, 220, 240, 260, 280, 300]
wind_colors = ListedColormap([
    "#68AD05", "#8DC00B", "#B1D415", "#D5E81C", "#FBFC22",
    "#FAD024", "#F9A427", "#FC7929", "#FB4D2B", "#EA2B57",
    "#FB22A5", "#FC22CE", "#FC22F5", "#FC62F8", "#FD80F8",
    "#FFBFFC", "#FEDFFE", "#FEFFFF", "#E1E0FF", "#C3C3FF",
    "#A5A5FF", "#A5A5FF", "#6868FE"
])
wind_norm = mcolors.BoundaryNorm(wind_bounds, wind_colors.N)



# ------------------------------
# Kartendomäne (Deutschland-Extent aus Dokument 1, keine _eu-Variante mehr)
# ------------------------------
extent = [-3.94, 20.34, 43.18, 58.08]  # lon_min, lon_max, lat_min, lat_max

FOOTER_TEXTS = {
    "t2m": "Temperatur 2m (°C)",
    "geo": "Geopotentielle Höhe 500hPa (m)",
    "pmsl": "Luftdruck auf Meereshöhe (hPa)",
    "snow": "Schneehöhe (cm)",
    "tp_acc": "Akkumulierter Niederschlag (mm)",
    "wind": "Windböen (km/h)",
}

# Einheit je Variable - für die Wertanzeige im Frontend
VALUE_UNITS = {
    "t2m": "°C",
    "geo": "m",
    "pmsl": "hPa",
    "snow": "cm",
    "tp_acc": "mm",
    "wind": "km/h"
}

# Nachkommastellen je Variable für die Wertanzeige
VALUE_DECIMALS = {
    "t2m": 1,
    "geo": 0,
    "pmsl": 0,
    "snow": 1,
    "tp_acc": 1,
    "wind": 0,
}

VALUE_NODATA = -9999.0

# ------------------------------
# EPSG:4326 -> EPSG:3857 (Web Mercator)
# ------------------------------
EARTH_RADIUS = 6378137.0  # Meter, WGS84/Web-Mercator-Kugelradius
WEBMERCATOR_WIDTH = 1024   # Ziel-Bildbreite in Pixeln für die Reprojektion


def lonlat_to_webmercator(lon_deg, lat_deg):
    x = EARTH_RADIUS * np.radians(lon_deg)
    y = EARTH_RADIUS * np.log(np.tan(np.pi / 4 + np.radians(lat_deg) / 2))
    return x, y


def webmercator_target_grid(extent, out_width=WEBMERCATOR_WIDTH):
    lon_min, lon_max, lat_min, lat_max = extent
    x_min, y_min = lonlat_to_webmercator(lon_min, lat_min)
    x_max, y_max = lonlat_to_webmercator(lon_max, lat_max)
    aspect = (y_max - y_min) / (x_max - x_min)
    out_height = max(int(round(out_width * aspect)), 1)
    x_new = np.linspace(x_min, x_max, out_width)
    y_new = np.linspace(y_min, y_max, out_height)  # aufsteigend: Süd -> Nord
    return x_new, y_new


def warp_equirect_to_webmercator(data, lon, lat, extent, method="linear",
                                  out_width=WEBMERCATOR_WIDTH):
    """data/lon/lat: reguläres EPSG:4326-Gitter, lon und lat aufsteigend
    sortiert. Gibt das Datenfeld auf einem regulären EPSG:3857-Pixelraster
    zurück (ebenfalls Süd -> Nord aufsteigend), zugeschnitten auf extent."""
    x_new, y_new = webmercator_target_grid(extent, out_width=out_width)
    xx, yy = np.meshgrid(x_new, y_new)
    lon_grid = np.degrees(xx / EARTH_RADIUS)
    lat_grid = np.degrees(2 * np.arctan(np.exp(yy / EARTH_RADIUS)) - np.pi / 2)

    interp_func = RegularGridInterpolator(
        (lat, lon), data,
        method=method,
        bounds_error=False,
        fill_value=np.nan
    )
    pts = np.array([lat_grid.ravel(), lon_grid.ravel()]).T
    warped = interp_func(pts).reshape(lat_grid.shape)
    return warped


def data_to_rgba(data, cmap, norm):
    """Wandelt ein 2D-Datenarray in ein RGBA-uint8-Array um.
    NaN-Werte werden komplett transparent."""
    rgba = cmap(norm(data))
    rgba = (rgba * 255).astype(np.uint8)
    nan_mask = ~np.isfinite(data)
    rgba[nan_mask, 3] = 0
    return rgba


def composite_over(top_rgba, bottom_rgba):
    """Alpha-Compositing (Porter-Duff 'over'): top_rgba über bottom_rgba legen."""
    top = top_rgba.astype(np.float32) / 255.0
    bottom = bottom_rgba.astype(np.float32) / 255.0
    a_top = top[..., 3:4]
    a_bottom = bottom[..., 3:4]
    out_alpha = a_top + a_bottom * (1 - a_top)
    out_rgb = np.where(
        out_alpha > 1e-6,
        (top[..., :3] * a_top + bottom[..., :3] * a_bottom * (1 - a_top)) / np.maximum(out_alpha, 1e-6),
        0.0
    )
    out = np.concatenate([out_rgb, out_alpha], axis=-1)
    return (out * 255).astype(np.uint8)


def render_contour_overlay_rgba(x_new, y_new, data_grid, main_levels, fine_levels):
    """Zeichnet Isolinien (Isobaren / Geopotenzial-Höhenlinien) mit
    Beschriftung auf einen transparenten Hintergrund, exakt im
    Pixelraster von (x_new, y_new) (Web-Mercator-Meter)."""
    out_width, out_height = len(x_new), len(y_new)

    if not np.any(np.isfinite(data_grid)):
        return np.zeros((out_height, out_width, 4), dtype=np.uint8)

    d_min = np.nanmin(data_grid)
    d_max = np.nanmax(data_grid)
    main_lv = [lv for lv in main_levels if d_min <= lv <= d_max]
    fine_lv = [lv for lv in fine_levels if d_min <= lv <= d_max]

    dpi = 100
    fig = plt.figure(figsize=(out_width / dpi, out_height / dpi), dpi=dpi)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(x_new[0], x_new[-1])
    ax.set_ylim(y_new[0], y_new[-1])
    ax.axis("off")
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    xx, yy = np.meshgrid(x_new, y_new)

    if fine_lv:
        ax.contour(xx, yy, data_grid, levels=fine_lv, colors="gray", linewidths=0.5, alpha=0.4)
    if main_lv:
        cs_main = ax.contour(xx, yy, data_grid, levels=main_lv, colors="white", linewidths=0.9, alpha=0.95)
        ax.clabel(cs_main, inline=True, fmt='%d', fontsize=9, colors='black')

    fig.canvas.draw()
    buf = np.asarray(fig.canvas.buffer_rgba()).copy()
    plt.close(fig)

    if buf.shape[1] != out_width or buf.shape[0] != out_height:
        buf = np.array(Image.fromarray(buf, mode="RGBA").resize((out_width, out_height), Image.LANCZOS))

    # Canvas-Zeile 0 = Norden (oben) -> spiegeln, damit Zeile 0 = Süden
    return buf[::-1]


def save_transparent_webp(data, cmap, norm, out_path, contour_rgba=None):
    rgba = data_to_rgba(data, cmap, norm)
    if contour_rgba is not None:
        rgba = composite_over(contour_rgba, rgba)
    img = Image.fromarray(rgba[::-1, :, :], mode="RGBA")
    img.save(out_path, format="WEBP", lossless=True, method=4)


_dom_x_min, _dom_y_min = lonlat_to_webmercator(extent[0], extent[2])
_dom_x_max, _dom_y_max = lonlat_to_webmercator(extent[1], extent[3])
DOMAIN_EXTENT_3857 = [float(_dom_x_min), float(_dom_y_min), float(_dom_x_max), float(_dom_y_max)]

# ------------------------------
# Eingebettete Rohdaten (DVAL-Chunk) im WebP
# ------------------------------
# Nur t2m aus Dokument 1 wird als Rohwert-Overlay fürs Frontend gebraucht
# (analog zu t2m/wind in Dokument 2). Die Kartendomäne entspricht hier
# bereits Deutschland, daher wird kein zusätzlicher Crop mehr benötigt -
# es wird direkt die volle Domäne eingebettet.
EMBED_DATA_VARS = {"t2m", "wind"}

QUANTUM_STEP = {
    "t2m": 0.05,   # °C, Anzeige mit 1 Dezimalstelle -> 0.05 ist mehr als genug
    "wind": 0.2,
}
NAN_SENTINEL_I16 = -32768
DVAL_FOURCC = b"DVAL"


def embed_data_chunk(webp_path, data, extent_3857, quantum, fourcc=DVAL_FOURCC):
    """Hängt ein rohes Datenfeld als privaten, int16-quantisierten RIFF-Chunk
    an ein WebP an.

    data: 2D-Array (float), row0 = Norden (also bereits wie fürs Bild
          gespiegelt).
    extent_3857: [x_min, y_min, x_max, y_max] in Web-Mercator-Metern.
    quantum: Rasterschritt in den Originaleinheiten (z.B. 0.05 für °C).
    """
    height, width = data.shape

    nan_mask = ~np.isfinite(data)
    data_filled = np.where(nan_mask, 0.0, data)
    quant = np.round(data_filled / quantum)
    quant = np.clip(quant, -32767, 32767).astype(np.int16)
    quant[nan_mask] = NAN_SENTINEL_I16

    header = struct.pack("<BBII", 2, 1, width, height)
    header += struct.pack("<4d", *extent_3857)
    header += struct.pack("<d", quantum)
    compressed = zlib.compress(np.ascontiguousarray(quant, dtype="<i2").tobytes(), level=9)
    payload = header + compressed

    size = len(payload)
    chunk = fourcc + struct.pack("<I", size) + payload
    if size % 2 == 1:
        chunk += b"\x00"

    with open(webp_path, "rb") as f:
        content = f.read()

    if content[0:4] != b"RIFF" or content[8:12] != b"WEBP":
        raise ValueError(f"{webp_path} ist keine gültige WebP-Datei (RIFF/WEBP-Header fehlt)")

    riff_size = struct.unpack("<I", content[4:8])[0]
    new_riff_size = riff_size + len(chunk)

    with open(webp_path, "wb") as f:
        f.write(content[:4])
        f.write(struct.pack("<I", new_riff_size))
        f.write(content[8:])
        f.write(chunk)


def fix_longitude(lon, data):
    """Falls Longitude im Bereich 0..360 vorliegt (z.B. GFS/NOAA nativ),
    auf -180..180 umklappen und aufsteigend sortieren."""
    if np.nanmax(lon) > 180:
        lon_wrapped = ((lon + 180) % 360) - 180
        order = np.argsort(lon_wrapped)
        lon = lon_wrapped[order]
        if data.ndim == 2:
            data = data[:, order]
        elif data.ndim == 3:
            data = data[:, :, order]
    return lon, data


def ensure_ascending(lon, lat, data):
    if lat[0] > lat[-1]:
        lat = lat[::-1]
        data = data[::-1, :]
    if lon[0] > lon[-1]:
        lon = lon[::-1]
        data = data[:, ::-1]
    return lon, lat, data


def render_and_save(var_type, data, lon, lat, valid_time_local, output_dir):
    """Gemeinsame Pipeline: Web-Mercator-Warp -> (Isolinien) -> WebP ->
    (DVAL-Chunk) für t2m/geo/pmsl/snow."""
    cmap, norm = {
        "t2m": (t2m_colors, t2m_norm),
        "wind": (wind_colors, wind_norm),
    }[var_type]

    render_data_merc = warp_equirect_to_webmercator(data, lon, lat, extent, method="linear")

    contour_rgba = None
    if var_type == "pmsl":
        x_new_m, y_new_m = webmercator_target_grid(extent)
        contour_rgba = render_contour_overlay_rgba(
            x_new_m, y_new_m, render_data_merc,
            main_levels=list(range(912, 1070, 4)),
            fine_levels=list(range(912, 1070, 1)),
        )
    elif var_type == "geo":
        x_new_m, y_new_m = webmercator_target_grid(extent)
        contour_rgba = render_contour_overlay_rgba(
            x_new_m, y_new_m, render_data_merc,
            main_levels=list(range(4800, 6000, 40)),
            fine_levels=list(range(4800, 6000, 20)),
        )

    outname = f"{var_type}_{valid_time_local:%Y%m%d_%H%M}.webp"
    out_path = os.path.join(output_dir, outname)
    save_transparent_webp(render_data_merc, cmap, norm, out_path, contour_rgba=contour_rgba)

    if var_type in EMBED_DATA_VARS:
        quantum = QUANTUM_STEP.get(var_type, 0.1)
        embed_data_chunk(out_path, render_data_merc[::-1], DOMAIN_EXTENT_3857, quantum)

    return outname


# ------------------------------
# tp_acc: Akkumulation über alle Dateien eines Modellruns
# ------------------------------
def process_tp_acc_files(data_dir, output_dir):
    grib_files = sorted([f for f in os.listdir(data_dir) if f.endswith(".grib2")])

    if not grib_files:
        print("Keine GRIB2-Dateien gefunden!")
        return

    file_info = []
    for filename in grib_files:
        path = os.path.join(data_dir, filename)
        try:
            ds = cfgrib.open_dataset(path)
            if "tp" not in ds:
                ds.close()
                continue

            run_time_utc = pd.to_datetime(ds["time"].values) if "time" in ds else None

            if "valid_time" in ds:
                valid_time_raw = ds["valid_time"].values
                valid_time_utc = pd.to_datetime(valid_time_raw[0]) if np.ndim(valid_time_raw) > 0 else pd.to_datetime(valid_time_raw)
            else:
                step = pd.to_timedelta(ds["step"].values[0])
                valid_time_utc = run_time_utc + step

            forecast_hour = int((valid_time_utc - run_time_utc).total_seconds() / 3600)

            file_info.append({
                'filename': filename,
                'path': path,
                'run_time': run_time_utc,
                'valid_time': valid_time_utc,
                'forecast_hour': forecast_hour
            })
            ds.close()
        except Exception as e:
            print(f"Fehler beim Lesen von {filename}: {e}")
            continue

    if not file_info:
        print("Keine gültigen tp-Dateien gefunden!")
        return

    file_info_df = pd.DataFrame(file_info)
    file_info_df = file_info_df.sort_values(['run_time', 'forecast_hour'])

    for run_time, group in file_info_df.groupby('run_time'):
        print(f"\n=== Verarbeite Modellrun: {run_time} ===")

        accumulated_tp = None
        previous_tp = None

        for idx, row in group.iterrows():
            ds = cfgrib.open_dataset(row['path'])
            current_tp = ds["tp"].values
            if current_tp.ndim == 3:
                current_tp = current_tp[0]
            current_tp[current_tp < 0] = 0

            lon = ds["longitude"].values
            lat = ds["latitude"].values
            lon, current_tp = fix_longitude(lon, current_tp)
            lon, lat, current_tp = ensure_ascending(lon, lat, current_tp)

            if accumulated_tp is None:
                accumulated_tp = current_tp.copy()
            else:
                delta_tp = current_tp - previous_tp
                delta_tp[delta_tp < 0] = 0
                accumulated_tp = accumulated_tp + delta_tp

            previous_tp = current_tp.copy()

            valid_time_local = row['valid_time'].tz_localize("UTC").astimezone(ZoneInfo("Europe/Berlin"))

            render_data_merc = warp_equirect_to_webmercator(accumulated_tp, lon, lat, extent, method="linear")
            outname = f"tp_acc_{valid_time_local:%Y%m%d_%H%M}.webp"
            out_path = os.path.join(output_dir, outname)
            save_transparent_webp(render_data_merc, tp_acc_colors, tp_acc_norm, out_path)

            print(f"  Forecast hour {row['forecast_hour']:03d}: "
                  f"Max akkumuliert = {np.nanmax(accumulated_tp):.1f} mm -> {outname}")

            ds.close()
            del current_tp, render_data_merc
            gc.collect()


# ------------------------------
# Dateien durchgehen
# ------------------------------
if var_type == "tp_acc":
    process_tp_acc_files(data_dir, output_dir)
elif var_type in ("t2m", "wind"):
    for filename in sorted(os.listdir(data_dir)):
        if not filename.endswith(".grib2"):
            continue
        path = os.path.join(data_dir, filename)
        ds = cfgrib.open_dataset(path)

        if var_type == "t2m":
            if "t2m" not in ds:
                print(f"Keine t2m in {filename}")
                ds.close()
                continue
            data = ds["t2m"].values - 273.15
        elif var_type == "wind":
            if "gust" not in ds:
                print(f"Keine 'gust' in {filename} - vorhandene Variablen: {list(ds.data_vars)}")
                ds.close()
                continue
            data = ds["gust"].values * 3.6
            data[data < 0] = 0

        if data.ndim == 3:
            data = data[0]

        if var_type == "snow":
            data = np.nan_to_num(data, nan=0.0)

        lon = ds["longitude"].values
        lat = ds["latitude"].values
        lon, data = fix_longitude(lon, data)
        lon, lat, data = ensure_ascending(lon, lat, data)

        run_time_utc = pd.to_datetime(ds["time"].values) if "time" in ds else None

        if "valid_time" in ds:
            valid_time_raw = ds["valid_time"].values
            valid_time_utc = pd.to_datetime(valid_time_raw[0]) if np.ndim(valid_time_raw) > 0 else pd.to_datetime(valid_time_raw)
        else:
            step = pd.to_timedelta(ds["step"].values[0])
            valid_time_utc = run_time_utc + step
        valid_time_local = valid_time_utc.tz_localize("UTC").astimezone(ZoneInfo("Europe/Berlin"))

        outname = render_and_save(var_type, data, lon, lat, valid_time_local, output_dir)
        print(f"{filename} -> {outname}")

        ds.close()
        del data
        gc.collect()
else:
    print(f"Unbekannter var_type {var_type}")
