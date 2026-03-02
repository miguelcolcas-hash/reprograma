import streamlit as st
import requests
import zipfile
import io
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import plotly.express as px
import plotly.graph_objects as go

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Supervisión YUPANA - Osinergmin", layout="wide", initial_sidebar_state="expanded")
st.title("⚡ Dashboard de Supervisión - Programas y Reprogramas (YUPANA)")
st.markdown("Fiscalización Dinámica e Interactiva de Curvas de Carga y Motivos de Reprogramación (COES)")

# --- 2. PARÁMETROS OPERATIVOS ---
MES_TXT = [
    "ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
    "JULIO","AGOSTO","SETIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"
]
barras = ["SANTA ROSA 220 A", "MOQUEGUA 220", "ZORRITOS 220"]
rdo_letras = list("ABCDEFGHIJ")

inicio_hora = datetime(2000, 1, 1, 0, 30)
horas_str = [(inicio_hora + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
horas_str[-1] = "23:59"

archivos_clave = {
    "HIDRO"   : "Hidro - Despacho (MW)",
    "TERMICA" : "Termica - Despacho (MW)",
    "RER"     : "Rer y No COES - Despacho (MW)",
    "CMG"     : "CMg - Barra ($ por MWh)",
    "RES_FRIA": "Termica - Reserva Fria Gas (MW)",
    "RES_GAS" : "Termica - Reserva Gas (MW)"
}

# --- MAPA CROMÁTICO OSINERGMIN ---
COLOR_MAP = {
    "Biogás+Biomasa+Nafta+Flexigas": "#800080",    # Morado
    "Solar": "#FFD700",                            # Amarillo
    "Eólica": "#808080",                           # Plomo
    "Hidráulica": "#1f77b4",                       # Azul
    "Gas de Camisea": "#006400",                   # Verde oscuro
    "Gas del Norte+Gas de la Selva": "#90EE90",    # Verde claro
    "Residual+Diésel D2": "#FF0000"                # Rojo
}

ORDEN_TECNOLOGIAS = [
    "Biogás+Biomasa+Nafta+Flexigas",
    "Solar",
    "Eólica",
    "Hidráulica",
    "Gas de Camisea",
    "Gas del Norte+Gas de la Selva",
    "Residual+Diésel D2"
]

# --- 3. CLASIFICADOR TERMODINÁMICO MAESTRO (ACTUALIZADO CON EXCEPCIONES) ---
def clasificar_tecnologia_yupana(nombre_central, origen_archivo=""):
    """Clasifica cada central en su respectiva tecnología evaluando su nombre."""
    nombre = str(nombre_central).upper()
    
    # 1. Biogás + Biomasa + Nafta + Flexigas (Se evalúa primero para capturar "REFTALARA" y "CANA BRAVA")
    biomasa_kws = [
        "PARAMONGA", "JACINTO", "HUAYCOLORO", "GRINGA", "MAPLE", "FLEXIGAS", "NAFTA", 
        "LUREN", "BIOMASA", "BIOGAS", "AGROAURORA", "CAHUAPANAS", "SUPE", "LAREDO", 
        "PETRAMAS", "DOÑA CATALINA", "DONA CATALINA", "PORTILLO", "CASA GRANDE", 
        "CANA BRAVA", "AGROOLMOS", "CALLAO", "REFTALARA"
    ]
    if any(kw in nombre for kw in biomasa_kws):
        return "Biogás+Biomasa+Nafta+Flexigas"
        
    # 2. Solar (Se evalúa antes que Hidro para atrapar "CSF YARUCAYA")
    solar_kws = [
        "SOL", "PANAMERICANA", "RUBI", "INTIPAMPA", "CLEMESI", "MATARANI", 
        "REPARTICION", "MAJES", "MISTI", "TACNA", "CS CARHUAQUERO", 
        "CSCOENERGY", "CSF", "CSSUNNY"
    ]
    if any(kw in nombre for kw in solar_kws):
        return "Solar"
        
    # 3. Eólica ("PE TALARA" evita colisión con "REFTALARA")
    eolica_kws = [
        "EOL", "WAYRA", "LOMITAS", "CUPISNIQUE", "PE TALARA", "MARCONA", 
        "TRES HERMANAS", "DUNA", "HUAMBOS", "SAN JUAN"
    ]
    if any(kw in nombre for kw in eolica_kws):
        return "Eólica"
        
    # 4. Hidráulica
    hidro_kws = [
        "HIDRO", "CH ", "C.H.", "MANTARO", "RESTITUCION", "CHAGLLA", "CERRO DEL AGUILA", 
        "MACHUPICCHU", "HUINCO", "CHARCANI", "CAÑON DEL PATO", "SAN GABAN", "CHIMAY", 
        "PLATANAL", "YUNCHAN", "QUISHUAR", "AURA", "ZONGO", "CARPAPATA", "LA JOYA", 
        "STACRUZ", "HUASAHUASI", "RONCADOR", "PURMACANA", "NIMPERIAL", "PIZARRAS", 
        "POECHOS", "CANCHAYLLO", "CHANCAY", "RUCUY", "RUNATULLO", "YANAPAMPA", "POTRERO", 
        "YARUCAYA", "CHANGELI", "8AGOSTO", "RENOVANDESH", "EL CARMEN", "TUPURI", "HUALLIN", 
        "GALLITO", "YAUPI", "MATUCANA", "CALLAHUANCA", "MOYOPAMPA", "HUANZA", "CHEO", 
        "CHURO", "CHHER", "CHZANA", "CURUMUY", "PIAS"
    ]
    if origen_archivo == "HIDRO" or any(kw in nombre for kw in hidro_kws):
        return "Hidráulica"
        
    # 5. Residual + Diésel D2
    diesel_kws = [
        "D2", "R6", "RESIDUAL", "DIESEL", "ILO21", "ILO 21", "ILO1", "ILO 1", "MOLLENDO", 
        "RECKA", "INDEPENDENCIA", "SAMANCO", "TARAPOTO", "IQUITOS", "YURIMAGUAS", 
        "PUERTO MALDONADO", "BELLAVISTA", "PEDRO RUIZ", "ETEN", "PIURA D", "CALANA", 
        "ELOR", "SHCUMMINS", "SNTV"
    ]
    if any(kw in nombre for kw in diesel_kws):
        return "Residual+Diésel D2"
        
    # 6. Gas del Norte + Gas de la Selva
    gas_norte_kws = [
        "AGUAYTIA", "TERMOSELVA", "PUCALLPA", "MALACAS", "ZORRITOS", "PARIÑAS", "EEEP", 
        "ENEL PIURA", "PIURA G", "NUEVA ZORRITOS", "AGE", "TALLANCA", "MAL2", "TABLAZO"
    ]
    if any(kw in nombre for kw in gas_norte_kws):
        return "Gas del Norte+Gas de la Selva"
        
    # 7. Default: Gas de Camisea (Térmicas base no capturadas antes: Kallpa, Chilca, Fenix, etc.)
    return "Gas de Camisea"

# --- 4. INGESTA Y ETL ---
def cargar_df_desde_zip(zf, stem):
    for info in zf.infolist():
        nombre_base = info.filename.split('/')[-1]
        if stem in nombre_base and not nombre_base.startswith("~"):
            with zf.open(info) as f:
                if nombre_base.upper().endswith('.CSV'): 
                    try:
                        df = pd.read_csv(f, sep=None, engine='python')
                    except:
                        f.seek(0)
                        df = pd.read_csv(f, sep=',')
                    return df
                elif nombre_base.upper().endswith(('.XLSX', '.XLS')): 
                    return pd.read_excel(f, engine='openpyxl')
    return None

def extraer_todas_centrales(df):
    series = {}
    if df is None or df.empty: return series
    
    invalid_cols = ["HORA", "TIEMPO", "FECHA", "ETAPA", "GENERADOR"]
    if df.shape[1] > 1:
        cols = [c for c in df.columns if not any(inv in str(c).upper() for inv in invalid_cols) and not str(c).startswith("Unnamed")]
        for c in cols:
            series[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).tolist()
    else:
        enc = [h.strip() for h in str(df.columns[0]).split(",")]
        start_idx = 0
        if len(enc) < 2:
            enc = [h.strip() for h in str(df.iloc[0,0]).split(",")]
            start_idx = 1
            
        nombres_validos, idx_validos = [], []
        for i, nombre in enumerate(enc[1:], start=1):
            if not any(inv in nombre.upper() for inv in invalid_cols):
                nombres_validos.append(nombre)
                idx_validos.append(i)
                series[nombre] = []
                
        for fila in df.iloc[start_idx:, 0].astype(str):
            partes = [p.strip() for p in fila.split(",")]
            for nombre, i in zip(nombres_validos, idx_validos):
                if i < len(partes) and partes[i]:
                    series[nombre].append(float(partes[i]))
                else:
                    series[nombre].append(0.0)
    return series

def renombrar_con_sufijos(diccionario_series, tipo_origen):
    renamed = {}
    for c, vals in diccionario_series.items():
        c_clean = str(c).replace("(EOL)", "").replace("(SOL)", "").replace("(HID)", "").replace("(TER)", "").replace("(RER)", "").strip()
        cat_maestra = clasificar_tecnologia_yupana(c_clean, tipo_origen)
        
        if cat_maestra == "Hidráulica": renamed[f"{c_clean} (HID)"] = vals
        elif cat_maestra == "Eólica": renamed[f"{c_clean} (EOL)"] = vals
        elif cat_maestra == "Solar": renamed[f"{c_clean} (SOL)"] = vals
        else: renamed[f"{c_clean} (TER)"] = vals
    return renamed

def rellenar_hasta_48(lst):
    if not lst: return [0.0]*48
    faltan = 48 - len(lst)
    return ([0.0]*faltan + lst) if faltan > 0 else lst[:48]

def suma_elementos(*listas):
    out = [0.0]*48
    for lst in listas:
        if lst:
            for i, v in enumerate(lst[:48]):
                if pd.notna(v): out[i] += v
    return out

def extraer_columna(df, col):
    return pd.to_numeric(df[col], errors='coerce').fillna(0).tolist() if df is not None and col in df.columns else None

def fila_sin_primer_valor(df):
    if df is None or df.empty: return None
    dic = extraer_todas_centrales(df)
    tot = [0.0]*48
    for v in dic.values():
        tot = suma_elementos(tot, rellenar_hasta_48(v))
    return tot

@st.cache_data(show_spinner=False)
def extraer_datos_dia_memoria(f):
    y, m, d = f.strftime("%Y"), f.strftime("%m"), f.strftime("%d")
    M = MES_TXT[f.month-1]
    fecha_str = f"{y}{m}{d}"
    ddmm = f"{d}{m}"
    
    urls = {
        "PDO": f"https://www.coes.org.pe/portal/browser/download?url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FPrograma%20Diario%2F{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FYUPANA_{fecha_str}.zip",
        "PDI_Intervenciones": f"https://www.coes.org.pe/portal/browser/download?url=Operaci%C3%B3n%2FPrograma%20de%20Mantenimiento%2FPrograma%20Diario%2F{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FAnexo1_Intervenciones_{fecha_str}.zip"
    }
    for l in rdo_letras:
        urls[f"RDO_{l}"] = f"https://www.coes.org.pe/portal/browser/download?url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{ddmm}{l}%2FYUPANA_{ddmm}{l}.zip"

    datos_dia = {"Dataframes": {}, "Log": []}
    for nombre, enlace in urls.items():
        try:
            r = requests.get(enlace, timeout=25)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                    if nombre == "PDI_Intervenciones":
                        datos_dia["Dataframes"]["Intervenciones"] = cargar_df_desde_zip(zf, "Anexo")
                    else:
                        datos_dia["Dataframes"][nombre] = {}
                        for key, stem in archivos_clave.items():
                            datos_dia["Dataframes"][nombre][key] = cargar_df_desde_zip(zf, stem)
                datos_dia["Log"].append(f"✅ {nombre}")
            else:
                datos_dia["Log"].append(f"❌ {nombre} (No emitido)")
        except Exception:
            datos_dia["Log"].append(f"❌ {nombre} (Error de red)")
    return datos_dia

# --- 5. MOTOR GRÁFICO (CERO OCULTO Y ORDEN ESTRICTO) ---
def crear_grafica_area_apilada(df_plot, titulo_grafico, aplicar_colores=False, orden_fijo=None):
    df_plot = df_plot.fillna(0)
    num_cols = [c for c in df_plot.columns if c != 'Hora']
    df_plot[num_cols] = df_plot[num_cols].apply(pd.to_numeric, errors='coerce').fillna(0).round(2)
    
    df_plot['TOTAL_SISTEMA'] = df_plot[num_cols].sum(axis=1).round(2)
    idx_pico = df_plot['TOTAL_SISTEMA'].idxmax()
    pico_mw = df_plot.loc[idx_pico, 'TOTAL_SISTEMA']
    pico_hora = df_plot.loc[idx_pico, 'Hora']
    
    if orden_fijo:
        orden_columnas = [col for col in orden_fijo if col in df_plot.columns]
    else:
        totales_por_unidad = df_plot.drop(columns=['Hora', 'TOTAL_SISTEMA']).sum()
        orden_columnas = totales_por_unidad.sort_values(ascending=False).index.tolist()
    
    cols_mantener = ['Hora', 'TOTAL_SISTEMA'] + orden_columnas
    df_melt = df_plot[cols_mantener].melt(id_vars=['Hora', 'TOTAL_SISTEMA'], var_name='Unidad Generadora', value_name='Potencia_MW')
    
    df_melt['Potencia_Plot'] = np.where(df_melt['Potencia_MW'] <= 0.01, np.nan, df_melt['Potencia_MW'])
    
    kw_args = {
        "data_frame": df_melt, "x": "Hora", "y": "Potencia_Plot", "color": "Unidad Generadora",
        "title": titulo_grafico, "labels": {"Potencia_Plot": "Potencia Activa (MW)"}
    }
    if aplicar_colores: kw_args["color_discrete_map"] = COLOR_MAP
    
    fig = px.area(**kw_args)
    fig.update_traces(hovertemplate="%{y:,.2f} MW")
    fig.update_xaxes(categoryorder='array', categoryarray=horas_str)
    
    total_plot = np.where(df_plot['TOTAL_SISTEMA'] <= 0.01, np.nan, df_plot['TOTAL_SISTEMA'])
    fig.add_scatter(
        x=df_plot['Hora'], y=total_plot, mode='lines',
        line=dict(width=0, color='rgba(0,0,0,0)'), name='<b>⚡ TOTAL SISTEMA</b>',
        hovertemplate='<b>%{y:,.2f} MW</b>', showlegend=False
    )
    
    fig.add_annotation(
        x=pico_hora, y=pico_mw, text=f"<b>Pico Máximo: {pico_mw:,.2f} MW</b><br>{pico_hora}",
        showarrow=True, arrowhead=2, arrowsize=1.5, arrowwidth=2, arrowcolor="#e74c3c",
        ax=0, ay=-50, font=dict(size=12, color="#c0392b"),
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#c0392b", borderwidth=1, borderpad=4
    )
    
    fig.update_layout(hovermode="x unified", height=650, xaxis=dict(tickangle=45), margin=dict(t=80, b=50, l=50, r=50))
    return fig

# --- 6. INTERFAZ Y EJECUCIÓN ---
st.sidebar.header("Parámetros de Fiscalización")
rango_fechas = st.sidebar.date_input("Intervalo de Fechas (YUPANA)", value=(date.today(), date.today()))

if st.sidebar.button("Extraer Curvas y Motivos", type="primary"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        ini, fin = rango_fechas
        st.session_state['fecha_ini'], st.session_state['fecha_fin'] = ini, fin
        
        status, prog_bar = st.empty(), st.progress(0)
        log_exp = st.expander("Ver bitácora de extracción del COES", expanded=False)
        
        datos_completos = {}
        dias = (fin - ini).days + 1
        for k in range(dias):
            f_actual = ini + timedelta(days=k)
            status.markdown(f"**⏳ Sincronizando (In-Memory):** {f_actual.strftime('%d/%m/%Y')} *(Día {k+1}/{dias})*")
            datos_dia = extraer_datos_dia_memoria(f_actual)
            datos_completos[f_actual] = datos_dia
            
            with log_exp: st.markdown(f"**{f_actual.strftime('%d/%m/%Y')}** ➔ " + " | ".join(datos_dia["Log"]))
            prog_bar.progress((k + 1) / dias)
            
        st.session_state['datos_yupana'] = datos_completos
        status.success("✅ Extracción y vectorización completadas con éxito.")
        prog_bar.empty()

# --- 7. VISUALIZACIÓN ---
if 'datos_yupana' in st.session_state:
    data = st.session_state['datos_yupana']
    ini, fin = st.session_state['fecha_ini'], st.session_state['fecha_fin']
    
    st.markdown("---")
    fecha_analisis = ini
    if ini != fin:
        col_f, _ = st.columns([1, 2])
        with col_f:
            fechas_op = {f.strftime('%d/%m/%Y'): f for f in data.keys()}
            fecha_analisis = fechas_op[st.selectbox("📅 **Seleccione un día específico:**", options=list(fechas_op.keys()))]
            
    datos_dia_sel = data[fecha_analisis]["Dataframes"]
    programas_validos = [p for p in ["PDO"] + [f"RDO_{l}" for l in rdo_letras] if p in datos_dia_sel]
    
    t_cmg, t_hidro, t_term, t_res_fria, t_res_gas, t_dem, t_eol, t_sol, t_motivos = st.tabs([
        "💸 CMG", "💧 Despacho Hidro", "🔥 Despacho Térmico", "🧊 Reserva Fría Gas", "⛽ Reserva Gas", 
        "📈 Demanda y Generación", "💨 Despacho Eólico", "☀️ Despacho Solar", "📋 Intervenciones"
    ])

    # === CMG ===
    with t_cmg:
        st.markdown(f"### Evolución Intradiaria CMG - {fecha_analisis.strftime('%d/%m/%Y')}")
        for barra in barras:
            df_plot = pd.DataFrame({'Hora': horas_str})
            datos_existen = False
            for prog in programas_validos:
                df_cmg = datos_dia_sel[prog].get("CMG")
                if df_cmg is not None and not df_cmg.empty and barra in df_cmg.columns:
                    vals = rellenar_hasta_48(extraer_columna(df_cmg, barra))
                    df_plot[prog.replace("_", " ")] = vals
                    datos_existen = True
            if datos_existen:
                fig = px.line(df_plot, x='Hora', y=df_plot.columns[1:], markers=True, title=f"CMG - {barra}", labels={"value": "USD/MWh", "variable": "Programa"})
                fig.update_xaxes(categoryorder='array', categoryarray=horas_str)
                fig.update_layout(hovermode="x unified", height=500)
                st.plotly_chart(fig, use_container_width=True)

    # === HIDRO ===
    with t_hidro:
        st.markdown(f"### 💧 Despacho Hidroeléctrico Detallado - {fecha_analisis.strftime('%d/%m/%Y')}")
        todas_hidro = set()
        dict_por_prog = {}
        for prog in programas_validos:
            dic_h = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("HIDRO")), "HIDRO")
            dic_r = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("RER")), "RER")
            dic_h.update({k: v for k, v in dic_r.items() if "(HID)" in k})
            activas_prog = {k: rellenar_hasta_48(v) for k, v in dic_h.items() if sum([x for x in v if pd.notna(x)]) > 0}
            todas_hidro.update(activas_prog.keys())
            dict_por_prog[prog] = activas_prog
            
        hc1, hc2, hc3, hc4 = st.columns(4)
        with hc1: st.selectbox("🏢 Empresa:", ["Todas"], disabled=True, key='h_emp')
        with hc2: st.selectbox("🏭 Tipo Generación:", ["Hidráulica (HID)"], disabled=True, key='h_tip')
        with hc3: st.selectbox("🌍 Zona:", ["Todas"], disabled=True, key='h_zon')
        with hc4: filtro_hidro = st.multiselect("⚡ Centrales:", options=sorted(list(todas_hidro)), default=[], placeholder="Todas (vacío) o buscar...", key='h_cen')
        
        for prog in programas_validos:
            lista_filtro_h = filtro_hidro if filtro_hidro else todas_hidro
            datos_filtrados = {k: v for k, v in dict_por_prog[prog].items() if k in lista_filtro_h}
            if datos_filtrados:
                df_plot = pd.DataFrame(datos_filtrados)
                df_plot.insert(0, 'Hora', horas_str)
                titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                st.markdown("---")

    # === TÉRMICA ===
    with t_term:
        st.markdown(f"### 🔥 Despacho Térmico Detallado - {fecha_analisis.strftime('%d/%m/%Y')}")
        todas_term = set()
        dict_por_prog = {}
        for prog in programas_validos:
            dic_t = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("TERMICA")), "TERMICA")
            dic_r = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("RER")), "RER")
            dic_t.update({k: v for k, v in dic_r.items() if "(TER)" in k})
            activas_prog = {k: rellenar_hasta_48(v) for k, v in dic_t.items() if sum([x for x in v if pd.notna(x)]) > 0}
            todas_term.update(activas_prog.keys())
            dict_por_prog[prog] = activas_prog
            
        tc1, tc2, tc3, tc4 = st.columns(4)
        with tc1: st.selectbox("🏢 Empresa:", ["Todas"], disabled=True, key='t_emp')
        with tc2: st.selectbox("🏭 Tipo Generación:", ["Térmica (TER)"], disabled=True, key='t_tip')
        with tc3: st.selectbox("🌍 Zona:", ["Todas"], disabled=True, key='t_zon')
        with tc4: filtro_term = st.multiselect("⚡ Centrales:", options=sorted(list(todas_term)), default=[], placeholder="Todas (vacío) o buscar...", key='t_cen')
        
        for prog in programas_validos:
            lista_filtro_t = filtro_term if filtro_term else todas_term
            datos_filtrados = {k: v for k, v in dict_por_prog[prog].items() if k in lista_filtro_t}
            if datos_filtrados:
                df_plot = pd.DataFrame(datos_filtrados)
                df_plot.insert(0, 'Hora', horas_str)
                titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                st.markdown("---")

    # === RESERVA FRÍA GAS ===
    with t_res_fria:
        st.markdown(f"### 🧊 Reserva Fría Gas (MW) - {fecha_analisis.strftime('%d/%m/%Y')}")
        todas_fria = set()
        dict_por_prog = {}
        for prog in programas_validos:
            dic_f = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("RES_FRIA")), "RES_FRIA")
            activas_prog = {k: rellenar_hasta_48(v) for k, v in dic_f.items() if sum([x for x in v if pd.notna(x)]) > 0}
            todas_fria.update(activas_prog.keys())
            dict_por_prog[prog] = activas_prog
            
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1: st.selectbox("🏢 Empresa:", ["Todas"], disabled=True, key='rf_emp')
        with fc2: st.selectbox("🏭 Tipo Generación:", ["Reserva Fría (TER)"], disabled=True, key='rf_tip')
        with fc3: st.selectbox("🌍 Zona:", ["Todas"], disabled=True, key='rf_zon')
        with fc4: filtro_fria = st.multiselect("⚡ Centrales:", options=sorted(list(todas_fria)), default=[], placeholder="Todas (vacío) o buscar...", key='rf_cen')
        
        for prog in programas_validos:
            lista_filtro_f = filtro_fria if filtro_fria else todas_fria
            datos_filtrados = {k: v for k, v in dict_por_prog[prog].items() if k in lista_filtro_f}
            if datos_filtrados:
                df_plot = pd.DataFrame(datos_filtrados)
                df_plot.insert(0, 'Hora', horas_str)
                titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                st.markdown("---")

    # === RESERVA GAS ===
    with t_res_gas:
        st.markdown(f"### ⛽ Reserva Gas (MW) - {fecha_analisis.strftime('%d/%m/%Y')}")
        todas_gas = set()
        dict_por_prog = {}
        for prog in programas_validos:
            dic_g = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("RES_GAS")), "RES_GAS")
            activas_prog = {k: rellenar_hasta_48(v) for k, v in dic_g.items() if sum([x for x in v if pd.notna(x)]) > 0}
            todas_gas.update(activas_prog.keys())
            dict_por_prog[prog] = activas_prog
            
        gc1, gc2, gc3, gc4 = st.columns(4)
        with gc1: st.selectbox("🏢 Empresa:", ["Todas"], disabled=True, key='rg_emp')
        with gc2: st.selectbox("🏭 Tipo Generación:", ["Reserva Gas (TER)"], disabled=True, key='rg_tip')
        with gc3: st.selectbox("🌍 Zona:", ["Todas"], disabled=True, key='rg_zon')
        with gc4: filtro_gas = st.multiselect("⚡ Centrales:", options=sorted(list(todas_gas)), default=[], placeholder="Todas (vacío) o buscar...", key='rg_cen')
        
        for prog in programas_validos:
            lista_filtro_g = filtro_gas if filtro_gas else todas_gas
            datos_filtrados = {k: v for k, v in dict_por_prog[prog].items() if k in lista_filtro_g}
            if datos_filtrados:
                df_plot = pd.DataFrame(datos_filtrados)
                df_plot.insert(0, 'Hora', horas_str)
                titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                st.markdown("---")

    # === DEMANDA Y MATRIZ ENERGÉTICA ===
    with t_dem:
        st.markdown(f"### 📈 Demanda Total del Sistema - {fecha_analisis.strftime('%d/%m/%Y')}")
        fig_dem = go.Figure()
        for prog in programas_validos:
            dic_h = extraer_todas_centrales(datos_dia_sel[prog].get("HIDRO"))
            dic_t = extraer_todas_centrales(datos_dia_sel[prog].get("TERMICA"))
            dic_r = extraer_todas_centrales(datos_dia_sel[prog].get("RER"))
            
            tot_sis = [0.0]*48
            for k, v in dic_h.items(): tot_sis = suma_elementos(tot_sis, rellenar_hasta_48(v))
            for k, v in dic_t.items(): tot_sis = suma_elementos(tot_sis, rellenar_hasta_48(v))
            for k, v in dic_r.items(): tot_sis = suma_elementos(tot_sis, rellenar_hasta_48(v))
            
            if sum(tot_sis) > 0:
                fig_dem.add_trace(go.Scatter(x=horas_str, y=tot_sis, mode='lines+markers', name=prog.replace('_', ' ')))
        fig_dem.update_xaxes(categoryorder='array', categoryarray=horas_str)
        fig_dem.update_layout(title="Curvas de Demanda (Comparativa)", hovermode="x unified", yaxis_title="MW", height=450)
        st.plotly_chart(fig_dem, use_container_width=True)

        st.markdown("---")
        st.markdown(f"### 📊 Matriz de Generación Acumulada por Tecnología (Línea de Tiempo Efectiva)")
        st.info("Visualización continua y con **colores normados**. Las franjas marcan el inicio de cada reprograma emitido.")
        
        active_prog_per_period = [programas_validos[0]] * 48
        
        for prog in programas_validos[1:]:
            dic_h = extraer_todas_centrales(datos_dia_sel[prog].get("HIDRO"))
            dic_t = extraer_todas_centrales(datos_dia_sel[prog].get("TERMICA"))
            dic_r = extraer_todas_centrales(datos_dia_sel[prog].get("RER"))
            
            tot_sis = [0.0]*48
            for k, v in dic_h.items(): tot_sis = suma_elementos(tot_sis, rellenar_hasta_48(v))
            for k, v in dic_t.items(): tot_sis = suma_elementos(tot_sis, rellenar_hasta_48(v))
            for k, v in dic_r.items(): tot_sis = suma_elementos(tot_sis, rellenar_hasta_48(v))
            
            start_idx = -1
            for i, val in enumerate(tot_sis):
                if val > 100: 
                    start_idx = i
                    break
                    
            if start_idx != -1:
                for i in range(start_idx, 48):
                    active_prog_per_period[i] = prog
                    
        tech_by_prog = {}
        for prog in programas_validos:
            dic_h = extraer_todas_centrales(datos_dia_sel[prog].get("HIDRO"))
            dic_t = extraer_todas_centrales(datos_dia_sel[prog].get("TERMICA"))
            dic_r = extraer_todas_centrales(datos_dia_sel[prog].get("RER"))
            
            cats = {
                "Gas de Camisea": [0.0]*48,
                "Gas del Norte+Gas de la Selva": [0.0]*48,
                "Hidráulica": [0.0]*48,
                "Eólica": [0.0]*48,
                "Solar": [0.0]*48,
                "Biogás+Biomasa+Nafta+Flexigas": [0.0]*48,
                "Residual+Diésel D2": [0.0]*48
            }
            
            for k, v in dic_h.items():
                cat = clasificar_tecnologia_yupana(k, "HIDRO")
                cats[cat] = suma_elementos(cats[cat], rellenar_hasta_48(v))
            for k, v in dic_t.items():
                cat = clasificar_tecnologia_yupana(k, "TERMICA")
                cats[cat] = suma_elementos(cats[cat], rellenar_hasta_48(v))
            for k, v in dic_r.items():
                cat = clasificar_tecnologia_yupana(k, "RER")
                cats[cat] = suma_elementos(cats[cat], rellenar_hasta_48(v))
                
            tech_by_prog[prog] = cats
            
        stitched_tech = {k: [0.0]*48 for k in tech_by_prog[programas_validos[0]].keys()}
        for i in range(48):
            prog_reinante = active_prog_per_period[i]
            for k in stitched_tech.keys():
                stitched_tech[k][i] = tech_by_prog[prog_reinante][k][i]
                
        stitched_tech = {k: v for k, v in stitched_tech.items() if sum([x for x in v if pd.notna(x)]) > 0}
        
        if stitched_tech:
            df_tech = pd.DataFrame(stitched_tech)
            df_tech.insert(0, 'Hora', horas_str)
            fig_stitched = crear_grafica_area_apilada(
                df_tech, "Distribución Energética Consolidada Continua", 
                aplicar_colores=True, orden_fijo=ORDEN_TECNOLOGIAS
            )
            
            prog_actual = active_prog_per_period[0]
            
            # Etiqueta garantizada a la izquierda para el inicio del día (00:30)
            fig_stitched.add_annotation(
                x=horas_str[0], y=1.05, yref="paper", 
                text=f"<b>Inicia {prog_actual.replace('_', ' ')}</b>",
                showarrow=False, font=dict(size=11, color="white"),
                bgcolor="#e74c3c", bordercolor="white", borderwidth=1, borderpad=3, 
                xanchor="left"
            )
            
            for i in range(1, 48):
                if active_prog_per_period[i] != prog_actual:
                    prog_actual = active_prog_per_period[i]
                    hora_cambio = horas_str[i]
                    
                    fig_stitched.add_vline(x=hora_cambio, line_width=2, line_dash="dash", line_color="rgba(255,255,255,0.7)")
                    fig_stitched.add_annotation(
                        x=hora_cambio, y=1.05, yref="paper", text=f"<b>Inicia {prog_actual.replace('_', ' ')}</b>",
                        showarrow=False, font=dict(size=11, color="white"),
                        bgcolor="#e74c3c", bordercolor="white", borderwidth=1, borderpad=3, xanchor="center"
                    )
                    
            st.plotly_chart(fig_stitched, use_container_width=True)

        st.markdown("---")
        st.markdown("### 🗄️ Datos Fuente: Despacho Continuo por Central (Empalmado)")
        
        todas_las_centrales_yupana = set()
        centrales_tipos = {}
        for prog in programas_validos:
            for tipo in ["HIDRO", "TERMICA", "RER"]:
                dic_raw = extraer_todas_centrales(datos_dia_sel[prog].get(tipo))
                for c in dic_raw.keys():
                    todas_las_centrales_yupana.add(c)
                    if c not in centrales_tipos:
                        centrales_tipos[c] = clasificar_tecnologia_yupana(c, tipo)
                
        lista_todas_centrales = sorted(list(todas_las_centrales_yupana))
        
        stitched_raw = {}
        for c in lista_todas_centrales:
            nombre_columna_exportacion = f"{c} ({centrales_tipos[c]})"
            stitched_raw[nombre_columna_exportacion] = [0.0]*48
            
        stitched_raw["Programa_Vigente"] = active_prog_per_period
        
        for i in range(48):
            prog_reinante = active_prog_per_period[i]
            dic_h = extraer_todas_centrales(datos_dia_sel[prog_reinante].get("HIDRO"))
            dic_t = extraer_todas_centrales(datos_dia_sel[prog_reinante].get("TERMICA"))
            dic_r = extraer_todas_centrales(datos_dia_sel[prog_reinante].get("RER"))
            
            for c in lista_todas_centrales:
                val = 0.0
                if c in dic_h: val += rellenar_hasta_48(dic_h[c])[i]
                if c in dic_t: val += rellenar_hasta_48(dic_t[c])[i]
                if c in dic_r: val += rellenar_hasta_48(dic_r[c])[i]
                
                nombre_columna_exportacion = f"{c} ({centrales_tipos[c]})"
                stitched_raw[nombre_columna_exportacion][i] = round(val, 2)
                
        df_stitched_raw = pd.DataFrame(stitched_raw)
        df_stitched_raw.insert(0, 'Hora', horas_str)
        prog_col = df_stitched_raw.pop("Programa_Vigente")
        df_stitched_raw.insert(1, "Programa_Vigente", prog_col)
        
        st.dataframe(df_stitched_raw, use_container_width=True)
        
        buffer_raw = io.BytesIO()
        with pd.ExcelWriter(buffer_raw, engine='openpyxl') as writer:
            df_stitched_raw.to_excel(writer, index=False, sheet_name='Despacho_Continuo')
        st.download_button(
            label="📥 Descargar Matriz Empalmada (Excel)", data=buffer_raw.getvalue(),
            file_name=f"Despacho_Continuo_Detalle_{fecha_analisis.strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary"
        )

    # === EÓLICO ===
    with t_eol:
        st.markdown(f"### 💨 Despacho Eólico Desglosado - {fecha_analisis.strftime('%d/%m/%Y')}")
        for prog in programas_validos:
            dic_r = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("RER")), "RER")
            activas_prog = {k: rellenar_hasta_48(v) for k, v in dic_r.items() if "(EOL)" in k and sum([x for x in v if pd.notna(x)]) > 0}
            if activas_prog:
                df_plot = pd.DataFrame(activas_prog)
                df_plot.insert(0, 'Hora', horas_str)
                titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                st.markdown("---")

    # === SOLAR ===
    with t_sol:
        st.markdown(f"### ☀️ Despacho Solar Desglosado - {fecha_analisis.strftime('%d/%m/%Y')}")
        for prog in programas_validos:
            dic_r = renombrar_con_sufijos(extraer_todas_centrales(datos_dia_sel[prog].get("RER")), "RER")
            activas_prog = {k: rellenar_hasta_48(v) for k, v in dic_r.items() if "(SOL)" in k and sum([x for x in v if pd.notna(x)]) > 0}
            if activas_prog:
                df_plot = pd.DataFrame(activas_prog)
                df_plot.insert(0, 'Hora', horas_str)
                titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                st.markdown("---")

    # === MOTIVOS ===
    with t_motivos:
        if "Intervenciones" in datos_dia_sel and datos_dia_sel["Intervenciones"] is not None and not datos_dia_sel["Intervenciones"].empty:
            df_mot = datos_dia_sel["Intervenciones"].dropna(how='all').dropna(axis=1, how='all')
            st.dataframe(df_mot, use_container_width=True)
        else:
            st.warning("No hay intervenciones reportadas (Anexo 1) para este día en el portal del COES.")