import sys
import cfgrib
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import pandas as pd
import os
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.patheffects as path_effects
from scipy.interpolate import RegularGridInterpolator
from zoneinfo import ZoneInfo
import numpy as np
from scipy.ndimage import gaussian_filter
from matplotlib.colors import ListedColormap, BoundaryNorm, LinearSegmentedColormap
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# ------------------------------
# Eingabe-/Ausgabe
# ------------------------------
data_dir = sys.argv[1]        # z.B. "output"
output_dir = sys.argv[2]      # z.B. "output/maps"
var_type = sys.argv[3]        # 't2m', 'ww', 'tp', 'tp_acc', 'cape_ml', 'dbz_cmax'
os.makedirs(output_dir, exist_ok=True)

# ------------------------------
# Geo-Daten
# ------------------------------
cities = pd.DataFrame({
    'name': ['Berlin', 'Hamburg', 'München', 'Köln', 'Frankfurt', 'Dresden', 'Stuttgart', 'Düsseldorf',
             'Nürnberg', 'Erfurt', 'Leipzig', 'Bremen', 'Saarbrücken', 'Hannover'],
    'lat': [52.52, 53.55, 48.14, 50.94, 50.11, 51.05, 48.78, 51.23,
            49.45, 50.98, 51.34, 53.08, 49.24, 52.37],
    'lon': [13.40, 9.99, 11.57, 6.96, 8.68, 13.73, 9.18, 6.78,
            11.08, 11.03, 12.37, 8.80, 6.99, 9.73]
})

eu_cities = pd.DataFrame({
    'name': [
        'Berlin', 'Oslo', 'Warschau',
        'Lissabon', 'Madrid', 'Rom',
        'Ankara', 'Helsinki', 'Reykjavik',
        'London', 'Paris'
    ],
    'lat': [
        52.52, 59.91, 52.23,
        38.72, 40.42, 41.90,
        39.93, 60.17, 64.13,
        51.51, 48.85
    ],
    'lon': [
        13.40, 10.75, 21.01,
        -9.14, -3.70, 12.48,
        32.86, 24.94, -21.82,
        -0.13, 2.35
    ]
})

# ------------------------------
# Temperatur-Farben
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
# Aufsummierter Niederschlag (tp_acc)
# ------------------------------
tp_acc_bounds = [0.1, 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100,
                 125, 150, 175, 200, 250, 300, 400, 500]
tp_acc_colors = ListedColormap([
    "#B4D7FF","#75BAFF","#349AFF","#0582FF","#0069D2",
    "#003680","#148F1B","#1ACF06","#64ED07","#FFF32B",
    "#E9DC01","#F06000","#FF7F26","#FFA66A","#F94E78",
    "#F71E53","#BE0000","#880000","#64007F","#C201FC",
    "#DD66FE","#EBA6FF","#F9E7FF","#D4D4D4","#969696"
])
tp_acc_norm = mcolors.BoundaryNorm(tp_acc_bounds, tp_acc_colors.N)

# ------------------------------
# DBZ-CMAX Farben
# ------------------------------
dbz_bounds = [0, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 63, 67, 70]
dbz_colors = ListedColormap([
    "#676767", "#FFFFFF", "#B3EFED", "#8CE7E2", "#00F5ED",
    "#00CEF0", "#01AFF4", "#028DF6", "#014FF7", "#0000F6",
    "#00FF01", "#01DF00", "#00D000", "#00BF00", "#00A701",
    "#019700", "#FFFF00", "#F9F000", "#EDD200", "#E7B500",
    "#FF5000", "#FF2801", "#F40000", "#EA0001", "#CC0000",
    "#FFC8FF", "#E9A1EA", "#D379D3", "#BE55BE", "#960E96"
])
dbz_norm = mcolors.BoundaryNorm(dbz_bounds, dbz_colors.N)

# ------------------------------
# Luftdruck
# ------------------------------

# Luftdruck-Farben (kontinuierlicher Farbverlauf für 45 Bins)
pmsl_bounds_colors = list(range(912, 1070, 4))  # Alle 4 hPa (45 Bins)
pmsl_colors = LinearSegmentedColormap.from_list(
    "pmsl_smooth",
    [
       "#FF6DFF", "#C418C4", "#950CA2", "#5A007D", "#3D007F",
       "#00337E", "#0472CB", "#4FABF8", "#A3D4FF", "#79DAAD",
       "#07A220", "#3EC008", "#9EE002", "#F3FC01", "#F19806",
       "#F74F11", "#B81212", "#8C3234", "#C87879", "#F9CBCD",
       "#E2E2E2"

    ],
    N=len(pmsl_bounds_colors)  # Genau 45 Farben für 45 Bins
)
pmsl_norm = BoundaryNorm(pmsl_bounds_colors, ncolors=len(pmsl_bounds_colors))


# ------------------------------
# Geopotenzial
# ------------------------------

geo_bounds = list(range(4800, 6000, 40))
geo_colors = LinearSegmentedColormap.from_list(
    "geo_smooth",
    [
        "#530155", "#6F1171", "#871D89", "#9E2C9E", "#B73AB2", "#CB49CD", "#9D3AD2",
        "#6C2ECF", "#3B20C5", "#0B12B8", "#0D2FC4", "#124FC4", "#136AB7", "#1889C1",
        "#149A99", "#06B16F", "#10BA4D", "#09CC28", "#FECC0B", "#FEB906", "#F5A40A",
        "#F09006", "#E38500", "#EB6C01", "#E45C04", "#DC4A01", "#DB3600", "#D42601",
        "#C31700", "#CB0003", "#4E0703"
    ],
    N=len(geo_bounds)
)
geo_norm = BoundaryNorm(geo_bounds, ncolors=len(geo_bounds))



# ------------------------------
# Kartenparameter
# ------------------------------
FIG_W_PX, FIG_H_PX = 880, 830
BOTTOM_AREA_PX = 179
TOP_AREA_PX = FIG_H_PX - BOTTOM_AREA_PX
TARGET_ASPECT = FIG_W_PX / TOP_AREA_PX

# Bounding Box Deutschland (fix, keine GeoJSON nötig)
extent = [5, 16, 47, 56]

extent_eu = [-23.5, 45.0, 29.5, 68.4]


# ------------------------------
# Dateien durchgehen
# ------------------------------
for filename in sorted(os.listdir(data_dir)):
    if not filename.endswith(".grib2"):
        continue
    path = os.path.join(data_dir, filename)
    ds = cfgrib.open_dataset(path)

    # Daten je Typ
    if var_type == "t2m":
        if "t2m" not in ds:
            print(f"Keine t2m in {filename}")
            continue
        data = ds["t2m"].values - 273.15
    elif var_type == "t2m_eu":
        if "t2m" not in ds:
            print(f"Keine t2m in {filename}")
            continue
        data = ds["t2m"].values - 273.15
    elif var_type == "geo":
        if "gh" not in ds:
            print(f"Keine z-Variable in {filename}  ds.keys(): {list(ds.keys())}")
            continue
        data = ds["gh"].values
        data[data < 0] = np.nan
    elif var_type == "geo_eu":
        if "gh" not in ds:
            print(f"Keine z-Variable in {filename}  ds.keys(): {list(ds.keys())}")
            continue
        data = ds["gh"].values
        data[data < 0] = np.nan
    elif var_type == "pmsl":
        if "prmsl" not in ds:
            print(f"Keine prmsl-Variable in {filename} ds.keys(): {list(ds.keys())}")
            continue
        data = ds["prmsl"].values / 100
        data[data < 0] = np.nan
    elif var_type == "pmsl_eu":
        if "prmsl" not in ds:
            print(f"Keine prmsl-Variable in {filename} ds.keys(): {list(ds.keys())}")
            continue
        data = ds["prmsl"].values / 100
        data[data < 0] = np.nan
    else:
        print(f"Unbekannter var_type {var_type}")
        continue

    if data.ndim==3:
        data=data[0]

    lon = ds["longitude"].values
    lat = ds["latitude"].values

    # --- LONGITUDE FIX: 0..360 -> -180..180 und entlang der Lon-Achse sortieren ---
    if np.nanmax(lon) > 180:                    # z.B. GFS / NOAA
        lon_wrapped = ((lon + 180) % 360) - 180  # in Bereich [-180,180)
        order = np.argsort(lon_wrapped)          # monotone Achse für Interpolator
        lon = lon_wrapped[order]

        # data kann 2D (ny,nx) oder 3D (t,ny,nx) sein – hier abfangen
        if data.ndim == 2:
            data = data[:, order]
        elif data.ndim == 3:
            data = data[:, :, order]

    run_time_utc = pd.to_datetime(ds["time"].values) if "time" in ds else None

    if "valid_time" in ds:
        valid_time_raw = ds["valid_time"].values
        valid_time_utc = pd.to_datetime(valid_time_raw[0]) if np.ndim(valid_time_raw) > 0 else pd.to_datetime(valid_time_raw)
    else:
        step = pd.to_timedelta(ds["step"].values[0])
        valid_time_utc = run_time_utc + step
    valid_time_local = valid_time_utc.tz_localize("UTC").astimezone(ZoneInfo("Europe/Berlin"))

    # --------------------------
    # Figure (Deutschland oder Europa)
    # --------------------------
    if var_type in ["pmsl_eu", "geo_eu", "t2m_eu"]:
        scale = 0.9
        fig = plt.figure(figsize=(FIG_W_PX/100*scale, FIG_H_PX/100*scale), dpi=100)
        shift_up = 0.02
        ax = fig.add_axes([0.0, BOTTOM_AREA_PX / FIG_H_PX + shift_up, 1.0, TOP_AREA_PX / FIG_H_PX],
                        projection=ccrs.PlateCarree())
        ax.set_extent(extent_eu)
        ax.set_axis_off()
        ax.set_aspect('auto')

        print("\n--- DEBUG: pmsl_eu-Kartenbereich ---")
        print(f"Extent EU: {extent_eu}")
    else:
        scale = 0.9
        fig = plt.figure(figsize=(FIG_W_PX/100*scale, FIG_H_PX/100*scale), dpi=100)
        shift_up = 0.02
        ax = fig.add_axes([0.0, BOTTOM_AREA_PX / FIG_H_PX + shift_up, 1.0, TOP_AREA_PX / FIG_H_PX],
                        projection=ccrs.PlateCarree())
        ax.set_extent(extent)
        ax.set_axis_off()
        ax.set_aspect('auto')


    if var_type in ["pmsl_eu", "geo_eu", "t2m_eu"]:
        target_res = 0.13   # gröber für Europa (~11 km)
        lon_min, lon_max, lat_min, lat_max = extent_eu
        buffer = target_res * 20  # Puffer für Interpolation
        nx = int(round(lon_max - lon_min) / target_res) + 1
        ny = int(round(lat_max - lat_min) / target_res) + 1
        lon_new = np.linspace(lon_min - buffer, lon_max + buffer, nx + 15)
        lat_new = np.linspace(lat_min - buffer, lat_max + buffer, ny + 15)
        lon2d_new, lat2d_new = np.meshgrid(lon_new, lat_new)
    else:
        target_res = 0.025  # feiner für Deutschland (~2.8 km)
        lon_min, lon_max, lat_min, lat_max = extent
        lon_new = np.arange(lon_min, lon_max + target_res, target_res)
        lat_new = np.arange(lat_min, lat_max + target_res, target_res)
        lon2d_new, lat2d_new = np.meshgrid(lon_new, lat_new)

    print(f"\n--- DEBUG: Neues Interpolationsgitter ---")
    print(f"lon_new range = {lon_new.min():.2f} .. {lon_new.max():.2f}, len = {len(lon_new)}")
    print(f"lat_new range = {lat_new.min():.2f} .. {lat_new.max():.2f}, len = {len(lat_new)}")


    # Nur interpolieren, wenn Daten reguläres 2D-Gitter haben
    if lon.ndim == 1 and lat.ndim == 1 and data.ndim == 2:
        try:
            if var_type == "ww":
                # 🧱 Kategorische Interpolation: nearest-neighbor
                interp_func = RegularGridInterpolator(
                    (lat[::-1], lon),
                    data[::-1, :],
                    method="nearest",          # <--- WICHTIG
                    bounds_error=False,
                    fill_value=np.nan
                )
            else:
                # 🌈 Kontinuierliche Interpolation: linear
                interp_func = RegularGridInterpolator(
                    (lat[::-1], lon),
                    data[::-1, :],
                    method="linear",
                    bounds_error=False,
                    fill_value=np.nan
                )

            pts = np.array([lat2d_new.ravel(), lon2d_new.ravel()]).T
            data = interp_func(pts).reshape(lat2d_new.shape)
            lon, lat = lon_new, lat_new
            lon2d, lat2d = lon2d_new, lat2d_new
            print("Interpolation erfolgreich ✅")
            print(f"Interpoliertes data.shape = {data.shape}")
            print(f"Interpolierte Werte: {np.nanmin(data):.1f} .. {np.nanmax(data):.1f}")
        except Exception as e:
            print(f"Interpolation übersprungen ({e})")

    # Plot
    if var_type == "t2m":
        smooth_data = gaussian_filter(data, sigma=0.8)
        im = ax.pcolormesh(lon, lat, smooth_data, cmap=t2m_colors, norm=t2m_norm, shading="auto")
    elif var_type == "t2m_eu":
        smooth_data = gaussian_filter(data, sigma=0.8)
        im = ax.pcolormesh(lon, lat, smooth_data, cmap=t2m_colors, norm=t2m_norm, shading="auto")
    elif var_type == "pmsl":
        # --- Luftdruck auf Meereshöhe (Deutschland) ---
        im = ax.pcolormesh(lon, lat, data, cmap=pmsl_colors, norm=pmsl_norm, shading="auto")
        data_hpa = data  # Daten liegen bereits in hPa vor

        # Haupt-Isobaren (alle 4 hPa)
        main_levels = list(range(912, 1070, 4))
        # Feine Isobaren (alle 1 hPa)
        fine_levels = list(range(912, 1070, 1))

        # Nur Levels zeichnen, die im Datenbereich liegen
        main_levels = [lev for lev in main_levels if data_hpa.min() <= lev <= data_hpa.max()]
        fine_levels = [lev for lev in fine_levels if data_hpa.min() <= lev <= data_hpa.max()]

        # Feine Isobaren (weiß, dünn, leicht transparent)
        ax.contour(
            lon, lat, data_hpa,
            levels=fine_levels,
            colors='gray', linewidths=0.5, alpha=0.4
        )

        # Haupt-Isobaren (weiß, etwas dicker)
        cs_main = ax.contour(
            lon, lat, data_hpa,
            levels=main_levels,
            colors='white', linewidths=0.8, alpha=0.9
        )

        # Isobaren-Beschriftung (Zahlen direkt auf Linien)
        ax.clabel(cs_main, inline=True, fmt='%d', fontsize=9, colors='black')

        # --- Extremwerte (Tief & Hoch) markieren, aber nur wenn im Extent ---
        min_idx = np.unravel_index(np.nanargmin(data_hpa), data_hpa.shape)
        max_idx = np.unravel_index(np.nanargmax(data_hpa), data_hpa.shape)
        min_val = data_hpa[min_idx]
        max_val = data_hpa[max_idx]

        lon_min, lon_max, lat_min, lat_max = extent

        # Tiefdruckzentrum (blauer Wert)
        lon_minpt, lat_minpt = lon[min_idx[1]], lat[min_idx[0]]
        if lon_min <= lon_minpt <= lon_max and lat_min <= lat_minpt <= lat_max:
            ax.text(
                lon_minpt, lat_minpt,
                f"{min_val:.0f}",
                color='white', fontsize=12, fontweight='bold',
                ha='center', va='center',
                transform=ccrs.PlateCarree(),
                clip_on=True,
                path_effects=[path_effects.withStroke(linewidth=1.5, foreground='black')]
            )

        # Hochdruckzentrum (roter Wert)
        lon_maxpt, lat_maxpt = lon[max_idx[1]], lat[max_idx[0]]
        if lon_min <= lon_maxpt <= lon_max and lat_min <= lat_maxpt <= lat_max:
            ax.text(
                lon_maxpt, lat_maxpt,
                f"{max_val:.0f}",
                color='white', fontsize=12, fontweight='bold',
                ha='center', va='center',
                transform=ccrs.PlateCarree(),
                clip_on=True,
                path_effects=[path_effects.withStroke(linewidth=1.5, foreground='black')]
            )



    elif var_type == "pmsl_eu":
            # Schnellere Variante ohne adjust_text und smoothing
            im = ax.pcolormesh(lon, lat, data, cmap=pmsl_colors, norm=pmsl_norm, shading="auto")
            data_hpa = data  # data schon in hPa
            main_levels = list(range(912, 1070, 4))
            cs = ax.contour(lon, lat, data_hpa, levels=main_levels,
                            colors='white', linewidths=0.8, alpha=0.9)
            ax.clabel(cs, inline=True, fmt='%d', fontsize=9, colors='black')

            low_levels = list(range(912, 1070, 1))
            cs2 = ax.contour(lon, lat, data_hpa, levels=low_levels,
                             colors='gray', linewidths=0.5, alpha=0.4)

            # Min/Max-Druck markieren (optional)
            min_idx = np.unravel_index(np.nanargmin(data_hpa), data_hpa.shape)
            max_idx = np.unravel_index(np.nanargmax(data_hpa), data_hpa.shape)

            ax.text(
                lon[min_idx[1]], lat[min_idx[0]],
                f"{data_hpa[min_idx]:.0f}",
                color='white', fontsize=11, fontweight='bold',
                ha='center', va='center',
                path_effects=[path_effects.withStroke(linewidth=1.5, foreground='black')]
            )

            ax.text(
                lon[max_idx[1]], lat[max_idx[0]],
                f"{data_hpa[max_idx]:.0f}",
                color='white', fontsize=11, fontweight='bold',
                ha='center', va='center',
                path_effects=[path_effects.withStroke(linewidth=1.5, foreground='black')]
            )
    elif var_type == "geo":
            im = ax.pcolormesh(lon, lat, data, cmap=geo_colors, norm=geo_norm, shading="auto")
            data_geo = data  # in m # data schon in hPa
            main_levels = list(range(4800, 6000, 40))
            cs = ax.contour(lon, lat, data_geo, levels=main_levels,
                            colors='white', linewidths=0.8, alpha=0.9)
            ax.clabel(cs, inline=True, fmt='%d', fontsize=9, colors='black')

            low_levels = list(range(4800, 6000, 20))
            ax.contour(lon, lat, data_geo, levels=low_levels,
                            colors='gray', linewidths=0.5, alpha=0.4)

            # Min/Max-Druck markieren (optional)
            min_idx = np.unravel_index(np.nanargmin(data_geo), data_geo.shape)
            max_idx = np.unravel_index(np.nanargmax(data_geo), data_geo.shape)

            ax.text(
                lon[min_idx[1]], lat[min_idx[0]],
                f"{data_geo[min_idx]:.0f}",
                color='white', fontsize=11, fontweight='bold',
                ha='center', va='center',
                transform=ccrs.PlateCarree(),
                clip_on=True,
                path_effects=[path_effects.withStroke(linewidth=1.5, foreground='black')]
            )

            ax.text(
                lon[max_idx[1]], lat[max_idx[0]],
                f"{data_geo[max_idx]:.0f}",
                color='white', fontsize=11, fontweight='bold',
                ha='center', va='center',
                transform=ccrs.PlateCarree(),
                clip_on=True,
                path_effects=[path_effects.withStroke(linewidth=1.5, foreground='black')]
            )

    elif var_type == "geo_eu":
            im = ax.pcolormesh(lon, lat, data, cmap=geo_colors, norm=geo_norm, shading="auto")
            data_geo = data  # in m # data schon in hPa
            main_levels = list(range(4800, 6000, 40))
            cs = ax.contour(lon, lat, data_geo, levels=main_levels,
                            colors='white', linewidths=0.8, alpha=0.9)
            ax.clabel(cs, inline=True, fmt='%d', fontsize=9, colors='black')

            low_levels = list(range(4800, 6000, 20))
            ax.contour(lon, lat, data_geo, levels=low_levels,
                            colors='gray', linewidths=0.5, alpha=0.4)

            # Min/Max-Druck markieren (optional)
            min_idx = np.unravel_index(np.nanargmin(data_geo), data_geo.shape)
            max_idx = np.unravel_index(np.nanargmax(data_geo), data_geo.shape)

            ax.text(
                lon[min_idx[1]], lat[min_idx[0]],
                f"{data_geo[min_idx]:.0f}",
                color='white', fontsize=11, fontweight='bold',
                ha='center', va='center',
                transform=ccrs.PlateCarree(),
                clip_on=True,
                path_effects=[path_effects.withStroke(linewidth=1.5, foreground='black')]
            )

            ax.text(
                lon[max_idx[1]], lat[max_idx[0]],
                f"{data_geo[max_idx]:.0f}",
                color='white', fontsize=11, fontweight='bold',
                ha='center', va='center',
                transform=ccrs.PlateCarree(),
                clip_on=True,
                path_effects=[path_effects.withStroke(linewidth=2, foreground='black')]
            )

    # ------------------------------
    # Grenzen & Städte
    # ------------------------------

    if var_type in ["pmsl_eu", "geo_eu", "t2m_eu"]:
        # 🌍 Europa: nur Ländergrenzen + europäische Städte
        ax.add_feature(cfeature.BORDERS.with_scale("10m"), edgecolor="black", linewidth=0.7)
        ax.add_feature(cfeature.COASTLINE.with_scale("10m"), edgecolor="black", linewidth=0.7)

        for _, city in eu_cities.iterrows():
            ax.plot(city["lon"], city["lat"], "o", markersize=6,
                    markerfacecolor="black", markeredgecolor="white",
                    markeredgewidth=1.5, zorder=5)
            txt = ax.text(city["lon"] + 0.3, city["lat"] + 0.3, city["name"],
                          fontsize=9, color="black", weight="bold", zorder=6)
            txt.set_path_effects([path_effects.withStroke(linewidth=1.5, foreground="white")])

    else:
        # 🇩🇪 Deutschland: Bundesländer, Grenzen und Städte
        ax.add_feature(cfeature.STATES.with_scale("10m"), edgecolor="#2C2C2C", linewidth=1)
        ax.add_feature(cfeature.BORDERS, linestyle=":", edgecolor="#2C2C2C", linewidth=1)
        ax.add_feature(cfeature.COASTLINE, linewidth=1.0, edgecolor="black")

        for _, city in cities.iterrows():
            ax.plot(city["lon"], city["lat"], "o", markersize=6,
                    markerfacecolor="black", markeredgecolor="white",
                    markeredgewidth=1.5, zorder=5)
            txt = ax.text(city["lon"] + 0.1, city["lat"] + 0.1, city["name"],
                          fontsize=9, color="black", weight="bold", zorder=6)
            txt.set_path_effects([path_effects.withStroke(linewidth=1.5, foreground="white")])

    # Rahmen um Karte
    ax.add_patch(mpatches.Rectangle((0, 0), 1, 1, transform=ax.transAxes,
                                    fill=False, color="black", linewidth=2))


    # Legende
    legend_h_px = 50
    legend_bottom_px = 45
    if var_type in ["t2m", "pmsl", "pmsl_eu", "t2m_eu", "geo", "geo_eu"]:
        bounds = t2m_bounds if var_type=="t2m" else pmsl_bounds_colors if var_type=="pmsl" else pmsl_bounds_colors if var_type=="pmsl_eu" else t2m_bounds if var_type=="t2m_eu" else geo_bounds if var_type=="geo" else geo_bounds
        cbar_ax = fig.add_axes([0.03, legend_bottom_px / FIG_H_PX, 0.94, legend_h_px / FIG_H_PX])
        cbar = fig.colorbar(im, cax=cbar_ax, orientation="horizontal", ticks=bounds)
        cbar.ax.tick_params(colors="black", labelsize=7)
        cbar.outline.set_edgecolor("black")
        cbar.ax.set_facecolor("white")

        # Für pmsl nur jeden 10. hPa Tick beschriften
        if var_type=="pmsl":
            tick_labels = [str(tick) if tick % 8 == 0 else "" for tick in bounds]
            cbar.set_ticklabels(tick_labels)
        if var_type=="pmsl_eu":
            tick_labels = [str(tick) if tick % 8 == 0 else "" for tick in bounds]
            cbar.set_ticklabels(tick_labels)
        if var_type == "t2m":
            tick_labels = [str(tick) if tick % 4 == 0 else "" for tick in bounds]
            cbar.set_ticklabels(tick_labels)
        if var_type == "t2m_eu":
            tick_labels = [str(tick) if tick % 4 == 0 else "" for tick in bounds]
            cbar.set_ticklabels(tick_labels)
        if var_type == "geo":
            tick_labels = [str(tick) if tick % 80 == 0 else "" for tick in bounds]
            cbar.set_ticklabels(tick_labels)
        if var_type == "geo_eu":
            tick_labels = [str(tick) if tick % 80 == 0 else "" for tick in bounds]
            cbar.set_ticklabels(tick_labels)
    # ------------------------------

    # Footer
    footer_ax = fig.add_axes([0.0, (legend_bottom_px + legend_h_px)/FIG_H_PX, 1.0,
                              (BOTTOM_AREA_PX - legend_h_px - legend_bottom_px)/FIG_H_PX])
    footer_ax.axis("off")
    footer_texts = {
        "t2m": "Temperatur 2m (°C)",
        "t2m_eu": "Temperatur 2m (°C), Europa",
        "pmsl": "Luftdruck auf Meereshöhe (hPa)",
        "pmsl_eu": "Luftdruck auf Meereshöhe (hPa), Europa",
        "geo": "Geopotentielle Höhe 500hPa (m)",
        "geo_eu": "Geopotentielle Höhe 500hPa (m), Europa"
    }

    left_text = footer_texts.get(var_type, var_type) + \
                f"\nGFS ({pd.to_datetime(run_time_utc).hour:02d}z), NOAA" \
                if run_time_utc is not None else \
                footer_texts.get(var_type, var_type) + "\nGFS (??z), NOAA"

    footer_ax.text(0.01, 0.85, left_text, fontsize=12, fontweight="bold", va="top", ha="left")
    footer_ax.text(0.734, 0.92, "Prognose für:", fontsize=12, va="top", ha="left", fontweight="bold")
    footer_ax.text(0.99, 0.68, f"{valid_time_local:%d.%m.%Y %H:%M} Uhr",
                   fontsize=12, va="top", ha="right", fontweight="bold")

    # Speichern
    outname = f"{var_type}_{valid_time_local:%Y%m%d_%H%M}.png"
    plt.savefig(os.path.join(output_dir, outname), dpi=100, bbox_inches=None, pad_inches=0)
    plt.close()
