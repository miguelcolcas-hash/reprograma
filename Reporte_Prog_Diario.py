from pathlib import Path
import requests, zipfile, io, math
from datetime import datetime, timedelta, date
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# -------------------- PARÁMETROS ----------------------------
anio, mes, dia = 2025, 7, 8 
ini, fin = date(2025, 7, 2), date(2025, 7, 8) 

dest_dir = Path.home() / "Desktop" / "Descargas_T"

barras      = ["SANTA ROSA 220 A", "MOQUEGUA 220", "ZORRITOS 220"]
rdo_letras  = list("ABCDE")                      # RDO A-E

# -------------------- LISTA MESES ---------------------------
MES_TXT = [
    "ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
    "JULIO","AGOSTO","SETIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"
]

# -------------------- FORMATO DE FECHA ----------------------
y, m, d   = anio, f"{mes:02d}", f"{dia:02d}"
M         = MES_TXT[mes-1]
fecha_str = f"{y}{m}{d}"           # 20230927
ddmm      = f"{d}{m}"              # 2709

# -------------------- URL PLANTILLAS ------------------------
base_pdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FPrograma%20Diario%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FYUPANA_{y}{m}{d}.zip")

base_pdi = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Mantenimiento%2FPrograma%20Diario%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FAnexo1_Intervenciones_{y}{m}{d}.zip")

base_rdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{d}{m}{letra}%2FYUPANA_{d}{m}{letra}.zip")

series = {"PDO": base_pdo, "PDI": base_pdi}
series.update({f"RDO_{l}": base_rdo for l in rdo_letras})

urls = {nom: tpl.format(y=y, m=m, d=d, M=M, letra=nom[-1]) for nom, tpl in series.items()}

# -------------------- DESCARGA & UNZIP ----------------------
dest_dir.mkdir(parents=True, exist_ok=True)
disp, nodisp = [], []

for nombre, enlace in urls.items():
    try:
        r = requests.get(enlace, timeout=40)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            if zf.testzip():
                raise zipfile.BadZipFile("ZIP corrupto")
            extr_dir = dest_dir / f"{nombre}_{fecha_str}"
            extr_dir.mkdir(exist_ok=True)
            zf.extractall(path=extr_dir)
        disp.append(nombre)
    except Exception:
        nodisp.append(nombre)

print("DESCARGADOS :", ", ".join(disp)   or "—")
print("FALTANTES   :", ", ".join(nodisp) or "—")

# -------------------- UTILIDADES GENERALES ------------------
def cargar_dataframe(folder: Path, stem: str) -> pd.DataFrame | None:
    for ext in (".csv", ".CSV", ".xlsx", ".xls"):
        f = folder / f"{stem}{ext}"
        if f.exists():
            try:
                if f.suffix.lower() in (".xlsx", ".xls"):
                    return pd.read_excel(f, engine="openpyxl")
                return pd.read_csv(f, sep=",", engine="python")
            except Exception:
                return None
    return None

def extraer_columna(df: pd.DataFrame, col: str):
    """Devuelve la columna solicitada como lista o None si no existe."""
    return df[col].tolist() if df is not None and col in df.columns else None

def rellenar_hasta_48(lst):
    if not lst:
        return None
    faltan = 48 - len(lst)
    return ([0]*faltan + lst) if faltan > 0 else lst[:48]

def recortar_ceros_inicio(vals, hrs):
    for i, v in enumerate(vals):
        if v != 0:
            return hrs[i:], vals[i:]
    return [], []

def suma_elementos(*listas):
    out = [0]*48
    for lst in listas:
        if lst:
            for i, v in enumerate(lst[:48]):
                out[i] += v
    return out

def totales_hidro(df):
    if df is None or df.empty: return None
    if df.shape[1] > 1:
        return df.iloc[:,1:].sum(axis=1, numeric_only=True).tolist()
    tot=[]
    for celda in df.iloc[:,0].astype(str):
        nums=[float(x) for x in celda.split(",")[1:] if x.strip()]
        tot.append(sum(nums))
    return tot

def totales_rer(df, nombres):
    if df is None or df.empty: return None
    if df.shape[1] > 1:
        cols=[c for c in df.columns if str(c).strip().upper() in nombres]
        return df[cols].sum(axis=1, numeric_only=True).tolist() if cols else None
    enc=[h.strip().upper() for h in str(df.iloc[0,0]).split(",")]
    idx=[i for i,h in enumerate(enc) if h in nombres]
    if not idx: return None
    tot=[]
    for fila in df.iloc[1:,0].astype(str):
        partes=[p.strip() for p in fila.split(",")]
        nums=[float(partes[i]) if i<len(partes) and partes[i] else 0 for i in idx]
        tot.append(sum(nums))
    return tot

def fila_sin_primer_valor(df):
    if df is None or df.empty: return None
    if df.shape[1] > 1:
        return df.iloc[:,1:].sum(axis=1, numeric_only=True).tolist()
    tot=[]
    for celda in df.iloc[:,0].astype(str):
        nums=[float(x) for x in celda.split(",")[1:] if x.strip()]
        tot.append(sum(nums))
    return tot

# -------------------- ETIQUETAS TIEMPO ----------------------
inicio = datetime(2000, 1, 1, 0, 30)
horas  = [(inicio + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
horas[-1]="23:59"
ticks_pos = range(0, 48, 2)
ticks_lbl = [horas[i] for i in ticks_pos]

# ============================================================
# ===============   G R Á F I C O S   C M G   ================
# ============================================================
stem_file = "CMg - Barra ($ por MWh)"
pdo_res   = dest_dir / f"PDO_{fecha_str}" / f"YUPANA_{fecha_str}" / "RESULTADOS"

for barra in barras:
    df_pdo   = cargar_dataframe(pdo_res, stem_file)
    datosPDO = rellenar_hasta_48(extraer_columna(df_pdo, barra))
    if not datosPDO:
        print(f"{barra}: sin datos en PDO, se omite.")
        continue

    series_barra = {"PDO": datosPDO}

    for letra in rdo_letras:
        rdo_res = (dest_dir / f"RDO_{letra}_{fecha_str}" /
                   f"YUPANA_{ddmm}{letra}" / "RESULTADOS")
        df_rdo   = cargar_dataframe(rdo_res, stem_file)
        datosRDO = rellenar_hasta_48(extraer_columna(df_rdo, barra))
        if datosRDO:
            series_barra[f"RDO {letra}"] = datosRDO

    plt.figure(figsize=(11, 5))
    valores_plot = []

    for nombre, valores in series_barra.items():
        x, y = recortar_ceros_inicio(valores, horas)
        if not y:
            continue
        valores_plot.extend(y)
        plt.plot(x, y, marker="o", linewidth=2, label=nombre)

    if not valores_plot:
        plt.close()
        print(f"{barra}: todas las series son ceros, figura omitida.")
        continue

    min_y = max(0, math.floor(min(valores_plot)) - 10)
    max_y = math.ceil(max(valores_plot)) + 10

    ax = plt.gca()
    ax.set_ylim(min_y, max_y)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
    plt.title(f"CMG {barra} (USD/MWh)")
    plt.xlabel("Hora")
    plt.ylabel("USD/MWh")
    plt.legend()
    plt.tight_layout()
    plt.show()

# ============================================================
# ===============   G R Á F I C O   H I D R O   ==============  (HIDRO + RER)
# ============================================================
stem_hidro = "Hidro - Despacho (MW)"
stem_rer   = "Rer y No COES - Despacho (MW)"

barras_rer = [
    "CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA",
    "NIMPERIAL","PIZARRAS","POECHOS2","CANCHAYLLO","CHANCAY","RUCUY",
    "RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO","CH MARANON",
    "YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO",
    "RENOVANDESH1","EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2",
    "TUPURI","CH HUALLIN"
]

series_h = {}

# --- PDO ---
df_pdo_h   = cargar_dataframe(pdo_res, stem_hidro)
df_pdo_rer = cargar_dataframe(pdo_res, stem_rer)

tot_hidro = rellenar_hasta_48(totales_hidro(df_pdo_h))
tot_rer   = rellenar_hasta_48(totales_rer(df_pdo_rer, barras_rer))
if tot_hidro and tot_rer:
    series_h["PDO"] = suma_elementos(tot_hidro, tot_rer)

# --- RDO A-E ---
for letra in rdo_letras:
    rdo_res = (dest_dir / f"RDO_{letra}_{fecha_str}" /
               f"YUPANA_{ddmm}{letra}" / "RESULTADOS")
    th = rellenar_hasta_48(totales_hidro(cargar_dataframe(rdo_res, stem_hidro)))
    tr = rellenar_hasta_48(totales_rer(cargar_dataframe(rdo_res, stem_rer), barras_rer))
    if th and tr:
        series_h[f"RDO {letra}"] = suma_elementos(th, tr)

# --- Graficar HIDRO + RER ---
if series_h:
    plt.figure(figsize=(11, 5))
    valores_plot = []

    for nombre, valores in series_h.items():
        x, y = recortar_ceros_inicio(valores, horas)
        if not y:
            continue
        valores_plot.extend(y)
        plt.plot(x, y, marker="o", linewidth=2, label=nombre)

    if valores_plot:
        min_y = max(0, math.floor(min(valores_plot)) - 10)
        max_y = math.ceil(max(valores_plot)) + 10

        ax = plt.gca()
        ax.set_ylim(min_y, max_y)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
        plt.title("HIDRO (MW)")
        plt.xlabel("Hora")
        plt.ylabel("MW")
        plt.legend()
        plt.tight_layout()
        plt.show()
else:
    print("No se encontraron datos para HIDRO+RER; gráfico omitido.")

# ============================================================
# ============   H I S T Ó R I C O   H I D R O   =============
# ============================================================
dias      = (fin - ini).days + 1

base_rdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{d}{m}A%2FYUPANA_{d}{m}A.zip")

stem_hidro = "Hidro - Despacho (MW)"
stem_rer   = "Rer y No COES - Despacho (MW)"

barras_rer = [
    "CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA",
    "NIMPERIAL","PIZARRAS","POECHOS2","CANCHAYLLO","CHANCAY","RUCUY",
    "RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO","CH MARANON",
    "YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO",
    "RENOVANDESH1","EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2",
    "TUPURI","CH HUALLIN"
]

series_dia = {} 

for k in range(dias):
    f = ini + timedelta(days=k)
    y, m, d = f.year, f.strftime("%m"), f.strftime("%d")
    M_TXT   = MES_TXT[f.month-1]

    url_zip = base_rdo.format(y=y, m=m, d=d, M=M_TXT)
    carpeta = dest_dir / f"RDO_A_{y}{m}{d}"
    resultados = carpeta / f"YUPANA_{d}{m}A" / "RESULTADOS"

    # Descarga y unzip si no existe
    if not resultados.exists():
        try:
            r = requests.get(url_zip, timeout=40)
            r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                zf.extractall(path=carpeta)
            print(f"Descargado RDO-A {f.isoformat()}")
        except Exception as e:
            print(f"{f.isoformat()}: error al bajar ZIP → {e}")
            continue

    th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
    tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), barras_rer))
    if th and tr:
        series_dia[f.isoformat()] = suma_elementos(th, tr)

# ---------- G R Á F I C O ---------------------------------------------
if series_dia:
    plt.figure(figsize=(11, 5))
    y_plot = []

    for fecha_lbl, valores in series_dia.items():
        x, y_vals = recortar_ceros_inicio(valores, horas)
        if not y_vals:
            continue
        y_plot.extend(y_vals)
        plt.plot(x, y_vals, marker="o", linewidth=2, label=fecha_lbl)

    if y_plot:
        min_y = max(0, math.floor(min(y_plot)) - 10)
        max_y = math.ceil(max(y_plot)) + 10

        ax = plt.gca()
        ax.set_ylim(min_y, max_y)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
        plt.title("HISTÓRICO HIDRO (MW)")
        plt.xlabel("Hora")
        plt.ylabel("MW")
        plt.legend(title="Fecha")
        plt.tight_layout()
        plt.show()
else:
    print("No se generó ninguna serie Hidro+RER.")

# ============================================================
# ===   H I S T Ó R I C O   H I D R O   P R O M E D I O   ====
# ============================================================
if series_dia:
    # 1) Calcular promedios ----------------------------------
    fechas_lbl = []          # ej. '08-20'
    promedios  = []          # promedio MW

    for fecha_lbl, vals in series_dia.items():
        _, y_vals = recortar_ceros_inicio(vals, horas)
        if y_vals:
            fechas_lbl.append(fecha_lbl) 
            promedios.append(sum(y_vals) / len(y_vals))

    # 2) Gráfico de barras ----------------------------------
    plt.figure(figsize=(9, 5))
    ax = plt.gca()
    barras = ax.bar(fechas_lbl, promedios)

    # 3) Etiquetas numéricas encima de cada barra -----------
    for rect, valor in zip(barras, promedios):
        altura = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2,
                altura + 1,                       # pequeño espacio arriba
                f"{valor:.0f}",
                ha="center", va="bottom", fontsize=9)

    # Formato de ejes
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.set_xlabel("Fecha")
    ax.set_ylabel("MW")
    ax.set_title("HISTÓRICO HIDRO (MW) (Promedio Diario)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.show()
else:
    print("Sin datos válidos para calcular promedio diario.")

# ============================================================
# ============   G R Á F I C O   D E M A N D A   =============
# ============================================================
archivos = {
    "HIDRO"   : "Hidro - Despacho (MW)",
    "TERMICA" : "Termica - Despacho (MW)",
    "RER"     : "Rer y No COES - Despacho (MW)"
}

series_dem = {}

# --- PDO ---
vals_hidro_p   = rellenar_hasta_48(
                    fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos["HIDRO"])))
vals_termica_p = rellenar_hasta_48(
                    fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos["TERMICA"])))
vals_rer_p     = rellenar_hasta_48(
                    fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos["RER"])))

series_dem["PDO"] = suma_elementos(vals_hidro_p, vals_termica_p, vals_rer_p)

# --- RDO A-E ---
for letra in rdo_letras:
    rdo_res = (dest_dir / f"RDO_{letra}_{fecha_str}" /
               f"YUPANA_{ddmm}{letra}" / "RESULTADOS")

    vals_h = rellenar_hasta_48(
                fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos["HIDRO"])))
    vals_t = rellenar_hasta_48(
                fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos["TERMICA"])))
    vals_r = rellenar_hasta_48(
                fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos["RER"])))

    if any((vals_h, vals_t, vals_r)):
        series_dem[f"RDO {letra}"] = suma_elementos(vals_h, vals_t, vals_r)
    
# --- Graficar DEMANDA ---
if series_dem:
    plt.figure(figsize=(11, 5))
    valores_plot = []

    for nombre, valores in series_dem.items():
        x, y = recortar_ceros_inicio(valores, horas)
        if not y:
            continue
        valores_plot.extend(y)
        plt.plot(x, y, marker="o", linewidth=2, label=nombre)

    if valores_plot:
        min_y = max(0, math.floor(min(valores_plot)) - 10)
        max_y = math.ceil(max(valores_plot)) + 10

        ax = plt.gca()
        ax.set_ylim(min_y, max_y)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
        plt.title("DEMANDA (MW)")
        plt.xlabel("Hora")
        plt.ylabel("MW")
        plt.legend()
        plt.tight_layout()
        plt.show()
else:
    print("No se generó DEMANDA: todas las series están vacías.")

# ============================================================
# ==========   H I S T Ó R I C O   D E M A N D A   ===========
# ============================================================
base_rdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{d}{m}A%2FYUPANA_{d}{m}A.zip")

archivos = {
    "HIDRO"   : "Hidro - Despacho (MW)",
    "TERMICA" : "Termica - Despacho (MW)",
    "RER"     : "Rer y No COES - Despacho (MW)"
}

series_dia = {} 

# ---------- Descarga + cálculo ---------------------------------------
for k in range((fin - ini).days + 1):
    f = ini + timedelta(days=k)
    y, m, d = f.year, f.strftime("%m"), f.strftime("%d")
    M_TXT   = MES_TXT[f.month-1]

    url_zip  = base_rdo.format(y=y, m=m, d=d, M=M_TXT)
    carpeta  = dest_dir / f"RDO_A_{y}{m}{d}"
    resultados = carpeta / f"YUPANA_{d}{m}A" / "RESULTADOS"

    # Descargar si falta
    if not resultados.exists():
        try:
            r = requests.get(url_zip, timeout=40); r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                zf.extractall(path=carpeta)
        except Exception:
            continue

    # Sumar Hidro + Térmica + RER
    vals_h = rellenar_hasta_48(
        fila_sin_primer_valor(cargar_dataframe(resultados, archivos["HIDRO"])))
    vals_t = rellenar_hasta_48(
        fila_sin_primer_valor(cargar_dataframe(resultados, archivos["TERMICA"])))
    vals_r = rellenar_hasta_48(
        fila_sin_primer_valor(cargar_dataframe(resultados, archivos["RER"])))

    if any((vals_h, vals_t, vals_r)):
        series_dia[f.isoformat()] = suma_elementos(vals_h, vals_t, vals_r)

# ---------- G R Á F I C O ---------------------------------------------
if series_dia:
    plt.figure(figsize=(11, 5))
    y_all = []

    for fecha_lbl, valores in series_dia.items():
        x, y_vals = recortar_ceros_inicio(valores, horas)
        if not y_vals:
            continue
        y_all.extend(y_vals)
        plt.plot(x, y_vals, marker="o", linewidth=2, label=fecha_lbl)

    if y_all:
        min_y = max(0, math.floor(min(y_all)) - 10)
        max_y = math.ceil(max(y_all)) + 10

        ax = plt.gca()
        ax.set_ylim(min_y, max_y)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
        plt.title("HISTÓRICO DEMANDA (MW)")
        plt.xlabel("Hora")
        plt.ylabel("MW")
        plt.legend(title="Fecha")
        plt.tight_layout()
        plt.show()
else:
    print("No se obtuvieron datos de DEMANDA.")

# =============================================================
# ==   H I S T Ó R I C O   D E M A N D A   P R O M E D I O   ==
# =============================================================
if series_dia:       
    
    # 1) Calcular promedios reales (sin ceros de relleno) ----
    etiquetas = []                                 # eje X (con año)
    promedios = []                                 # eje Y

    for fecha_lbl, vals in series_dia.items():
        _, y_vals = recortar_ceros_inicio(vals, horas)
        if y_vals:
            etiquetas.append(fecha_lbl) 
            promedios.append(sum(y_vals)/len(y_vals))

    # 2) Dibujar barras --------------------------------------
    plt.figure(figsize=(9, 5))
    ax = plt.gca()
    barras = ax.bar(etiquetas, promedios, color="#9467BD") 

    # 3) Número encima de cada barra -------------------------
    for rect, valor in zip(barras, promedios):
        ax.text(rect.get_x() + rect.get_width()/2,
                rect.get_height() + 1,             # margen pequeño
                f"{valor:.0f}",
                ha="center", va="bottom", fontsize=9)

    # Formato de ejes
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.set_xlabel("Fecha")
    ax.set_ylabel("MW")
    ax.set_title("HISTÓRICO DEMANDA (MW) (Promedio Diario)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.show()
else:
    print("Sin datos para promedios diarios de DEMANDA RDO-A.")

# ============================================================
# =============   G R Á F I C O   E Ó L I C O   ==============
# ============================================================
stem_rer = "Rer y No COES - Despacho (MW)"

barras_eol = [
    "PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS",
    "WAYRAI","HUAMBOS","DUNA","CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
    "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"
]

# ---------- Crear dict de series --------------------------------------
series_rer = {}

# PDO
df_pdo_rer = cargar_dataframe(pdo_res, stem_rer)
vals_pdo   = rellenar_hasta_48(totales_rer(df_pdo_rer, barras_eol))
if vals_pdo:
    series_rer["PDO"] = vals_pdo

# RDO A-E
for letra in rdo_letras:
    rdo_res = (dest_dir / f"RDO_{letra}_{fecha_str}" /
               f"YUPANA_{ddmm}{letra}" / "RESULTADOS")
    df_rdo  = cargar_dataframe(rdo_res, stem_rer)
    vals_rdo = rellenar_hasta_48(totales_rer(df_rdo, barras_eol))
    if vals_rdo:
        series_rer[f"RDO {letra}"] = vals_rdo

# ---------- Graficar ---------------------------------------------------
if series_rer:
    plt.figure(figsize=(11, 5))
    y_plot = []

    for nombre, valores in series_rer.items():
        x, y = recortar_ceros_inicio(valores, horas)
        if not y:
            continue
        y_plot.extend(y)
        plt.plot(x, y, marker="o", linewidth=2, label=nombre)

    if y_plot:
        min_y = max(0, math.floor(min(y_plot)) - 10)
        max_y = math.ceil(max(y_plot)) + 10

        ax = plt.gca()
        ax.set_ylim(min_y, max_y)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
        plt.title("EÓLICO (MW)")
        plt.xlabel("Hora")
        plt.ylabel("MW")
        plt.legend()
        plt.tight_layout()
        plt.show()
else:
    print("No se encontraron datos para RER eólicos; gráfico omitido.")

# ============================================================
# ===========   H I S T Ó R I C O   E Ó L I C O   ============
# ============================================================
base_rdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{d}{m}A%2FYUPANA_{d}{m}A.zip")

stem_rer   = "Rer y No COES - Despacho (MW)"
barras_eol = [
    "PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS",
    "WAYRAI","HUAMBOS","DUNA","CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
    "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"
]

series_dia = {}     

for k in range((fin - ini).days + 1):
    f = ini + timedelta(days=k)
    y, m, d = f.year, f.strftime("%m"), f.strftime("%d")
    M_TXT   = MES_TXT[f.month-1]

    url_zip = base_rdo.format(y=y, m=m, d=d, M=M_TXT)
    carpeta = dest_dir / f"RDO_A_{y}{m}{d}"
    result  = carpeta / f"YUPANA_{d}{m}A" / "RESULTADOS"

    # descarga si hace falta
    if not result.exists():
        try:
            r = requests.get(url_zip, timeout=40); r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                zf.extractall(path=carpeta)
        except Exception:
            continue

    # Calcular total eólico
    df_rer = cargar_dataframe(result, stem_rer)
    tot_eol = rellenar_hasta_48(totales_rer(df_rer, barras_eol))
    if tot_eol:
        series_dia[f.isoformat()] = tot_eol

# --------------------- G R Á F I C O -------------------------
if series_dia:
    plt.figure(figsize=(11, 5))
    y_all = []

    for fecha_lbl, vals in series_dia.items():
        x, y_vals = recortar_ceros_inicio(vals, horas)
        if not y_vals:
            continue
        y_all.extend(y_vals)
        plt.plot(x, y_vals, marker="o", linewidth=2, label=fecha_lbl)  # muestra año

    if y_all:
        min_y = max(0, math.floor(min(y_all)) - 10)
        max_y = math.ceil(max(y_all)) + 10

        ax = plt.gca()
        ax.set_ylim(min_y, max_y)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
        plt.title("HISTÓRICO EÓLICO (MW)")
        plt.xlabel("Hora")
        plt.ylabel("MW")
        plt.legend(title="Fecha")
        plt.tight_layout()
        plt.show()
else:
    print("No se obtuvieron datos eólicos.")

# =============================================================
# ===   H I S T Ó R I C O   E Ó L I C O   P R O M E D I O   ===
# =============================================================
if series_dia:                         # generado en el bloque previo
    etiquetas = []                     # eje X
    promedios = []                     # eje Y (MW)

    # 1) Promedio real por día (sin ceros de relleno)
    for fecha_lbl, vals in series_dia.items():
        _, y_vals = recortar_ceros_inicio(vals, horas)
        if y_vals:
            etiquetas.append(fecha_lbl) 
            promedios.append(sum(y_vals)/len(y_vals))

    # 2) Dibujar barras
    plt.figure(figsize=(9, 5))
    ax = plt.gca()
    barras = ax.bar(etiquetas, promedios, color="#2CA02C")

    # 3) Etiqueta numérica encima de cada barra
    for rect, valor in zip(barras, promedios):
        ax.text(rect.get_x() + rect.get_width()/2,
                rect.get_height() + 1,
                f"{valor:.0f}",
                ha="center", va="bottom", fontsize=9)

    # Formato de ejes y título
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.set_xlabel("Fecha")
    ax.set_ylabel("MW")
    ax.set_title("EÓLICO SEIN (MW)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.show()
else:
    print("No hay datos para calcular promedios diarios eólicos.")

# ============================================================
# ==============   G R Á F I C O   S O L A R   ===============
# ============================================================
stem_rer = "Rer y No COES - Despacho (MW)"

barras_solar = [
    "MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
    "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
    "CS MATARANI","CS SAN MARTIN"
]

series_sol = {} 

# ---------- PDO --------------------------------------------------------
df_pdo_sol = cargar_dataframe(pdo_res, stem_rer)
vals_pdo   = rellenar_hasta_48(totales_rer(df_pdo_sol, barras_solar))
if vals_pdo:                       # guarda sólo si hay valores reales
    series_sol["PDO"] = vals_pdo

# ---------- RDO A-E -----------------------------------------------------
for letra in rdo_letras:
    rdo_res = dest_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
    df_rdo_sol = cargar_dataframe(rdo_res, stem_rer)
    vals_rdo   = rellenar_hasta_48(totales_rer(df_rdo_sol, barras_solar))
    # omite la curva si TODOS sus valores son None/NaN/0
    if vals_rdo and any(v != 0 for v in vals_rdo):
        series_sol[f"RDO {letra}"] = vals_rdo

# ---------- G R Á F I C A  ---------------------------------------------
if series_sol:
    plt.figure(figsize=(11, 5))
    y_plot = []

    for nombre, raw_vals in series_sol.items():
        y_vals = []
        for i, v in enumerate(raw_vals):
            v = 0 if (pd.isna(v)) else v          # NaN → 0
            # dibuja ceros sólo si i∈[0,10] ∪ [37,47]
            if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47):
                y_vals.append(None)               # rompe la línea
            else:
                y_vals.append(v)

        if all(v is None for v in y_vals):        # todo nulo → no plotea
            continue

        y_plot.extend([v for v in y_vals if v is not None])
        plt.plot(horas, y_vals, marker="o", linewidth=2, label=nombre)

    # ---- Formato de ejes ---------------------------------------------
    min_y = max(0, math.floor(min(y_plot)) - 10)
    max_y = math.ceil(max(y_plot)) + 10

    ax = plt.gca()
    ax.set_ylim(min_y, max_y)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
    plt.title("SOLAR (MW)")
    plt.xlabel("Hora")
    plt.ylabel("MW")
    plt.legend()
    plt.tight_layout()
    plt.show()
else:
    print("No hay datos útiles para las barras solares; gráfico omitido.")

# ============================================================
# ============   H I S T Ó R I C O   S O L A R   =============
# ============================================================
base_rdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{d}{m}A%2FYUPANA_{d}{m}A.zip")

stem_rer   = "Rer y No COES - Despacho (MW)"
barras_solar = [
    "MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
    "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
    "CS MATARANI","CS SAN MARTIN"
]

series_dia = {} 

for n in range((fin - ini).days + 1):
    f = ini + timedelta(days=n)
    y, m, d = f.year, f.strftime("%m"), f.strftime("%d")
    M_TXT   = MES_TXT[f.month-1]

    url_zip = base_rdo.format(y=y, m=m, d=d, M=M_TXT)
    carpeta = dest_dir / f"RDO_A_{y}{m}{d}"
    resultados = carpeta / f"YUPANA_{d}{m}A" / "RESULTADOS"

    # ---- Descarga condicional ----------------------------------------
    if not resultados.exists():
        try:
            r = requests.get(url_zip, timeout=40); r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                zf.extractall(path=carpeta)
        except Exception:
            continue

    # ---- Total solar por fila ---------------------------------------
    df_sol = cargar_dataframe(resultados, stem_rer)
    vals   = rellenar_hasta_48(totales_rer(df_sol, barras_solar))
    if vals and any(v != 0 for v in vals):
        series_dia[f.isoformat()] = vals

# -------------------  G R Á F I C O  -----------------------------
if series_dia:
    plt.figure(figsize=(11, 5))
    y_all = []

    for fecha_lbl, raw_vals in series_dia.items():
        
        # Aplicar regla de ceros
        y_vals = []
        for i, v in enumerate(raw_vals):
            v = 0 if pd.isna(v) else v
            # solo mostrar ceros en los rangos [0-10] y [36-47]
            if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47):
                y_vals.append(None)          # rompe la línea
            else:
                y_vals.append(v)

        if all(v is None for v in y_vals):
            continue

        y_all.extend([v for v in y_vals if v is not None])
        plt.plot(horas, y_vals, marker="o", linewidth=2, label=fecha_lbl)

    # Formato ejes
    min_y = max(0, math.floor(min(y_all)) - 10)
    max_y = math.ceil(max(y_all)) + 10
    ax = plt.gca()
    ax.set_ylim(min_y, max_y)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.xticks(ticks_pos, ticks_lbl, rotation=45, ha="right", fontsize=8)
    plt.title("HISTÓRICO SOLAR (MW))")
    plt.xlabel("Hora")
    plt.ylabel("MW")
    plt.legend(title="Fecha")
    plt.tight_layout()
    plt.show()
else:
    print("No se obtuvieron datos solares.")

# ===========================================================
# ===   H I S T Ó R I C O   S O L A R   P R O M E D I O   ===
# ===========================================================
if series_dia: 
    etiquetas, promedios = [], []

    # 1) Calcular promedios (sin ceros de relleno)
    for fecha_lbl, vals in series_dia.items():
        completos = [
            0 if (v is None or (isinstance(v, float) and math.isnan(v))) else v
            for v in vals[:48]
        ]
        etiquetas.append(fecha_lbl) 
        promedios.append(sum(completos) / len(completos)) 
        
    # 2) Dibujar barras
    plt.figure(figsize=(9, 5))
    ax = plt.gca()
    barras = ax.bar(etiquetas, promedios, color="#ffb347")

    # 3) Número encima de cada barra
    for rect, valor in zip(barras, promedios):
        ax.text(rect.get_x() + rect.get_width() / 2,
                rect.get_height() + 1,
                f"{valor:.0f}",
                ha="center", va="bottom", fontsize=9)

    # Formato de ejes y título
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.set_xlabel("Fecha")
    ax.set_ylabel("MW")
    ax.set_title("SOLAR SEIN (MW)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.show()
else:
    print("Sin datos para promedios diarios del total solar.")
