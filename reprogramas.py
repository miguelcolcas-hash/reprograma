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
rdo_letras = list("ABCDE")

inicio_hora = datetime(2000, 1, 1, 0, 30)
horas_str = [(inicio_hora + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
horas_str[-1] = "23:59"

barras_rer = [
    "CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA",
    "NIMPERIAL","PIZARRAS","POECHOS2","CANCHAYLLO","CHANCAY","RUCUY",
    "RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO","CH MARANON",
    "YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO",
    "RENOVANDESH1","EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2",
    "TUPURI","CH HUALLIN"
]
barras_eol = [
    "PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS",
    "WAYRAI","HUAMBOS","DUNA","CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
    "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"
]
barras_solar = [
    "MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
    "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
    "CS MATARANI","CS SAN MARTIN"
]

archivos_clave = {
    "HIDRO"   : "Hidro - Despacho (MW)",
    "TERMICA" : "Termica - Despacho (MW)",
    "RER"     : "Rer y No COES - Despacho (MW)",
    "CMG"     : "CMg - Barra ($ por MWh)"
}

# --- 3. ETL OPTIMIZADO (VECTORIZACIÓN PANDAS) ---
def cargar_df_desde_zip(zf, stem):
    for info in zf.infolist():
        nombre_base = info.filename.split('/')[-1]
        if stem in nombre_base and not nombre_base.startswith("~"):
            with zf.open(info) as f:
                if nombre_base.upper().endswith('.CSV'): return pd.read_csv(f)
                elif nombre_base.upper().endswith(('.XLSX', '.XLS')): return pd.read_excel(f, engine='openpyxl')
    return None

def parse_yupana(df_raw, tipo=""):
    """Limpia y vectoriza DataFrames garantizando siempre 48 periodos."""
    if df_raw is None or df_raw.empty: return pd.DataFrame()
    
    # Manejo de CSVs agrupados en una columna
    if df_raw.shape[1] == 1:
        df = df_raw.iloc[:, 0].astype(str).str.split(',', expand=True)
        df.columns = df.iloc[0].str.strip()
        df = df.iloc[1:]
    else:
        df = df_raw.copy()
        df.columns = df.columns.astype(str).str.strip()
        
    # Limpieza de columnas y conversión a numérico
    cols_keep = [c for c in df.columns if c.upper() not in ['HORA', 'TIEMPO', 'FECHA'] and not c.startswith('Unnamed') and c]
    df = df[cols_keep].apply(pd.to_numeric, errors='coerce').fillna(0)
    
    # PARCHE DE REINDEXADO: Fuerza exactamente 48 filas, rellenando con 0 MW si faltan datos
    df = df.reset_index(drop=True).reindex(range(48), fill_value=0)
    
    # Asignación de sufijos
    if tipo == "HIDRO":
        df.columns = [f"{c} (HID)" for c in df.columns]
    elif tipo == "TERMICA":
        df.columns = [f"{c} (TER)" for c in df.columns]
    elif tipo == "RER":
        nuevas_cols = []
        for c in df.columns:
            c_clean = c.replace("(EOL)", "").replace("(SOL)", "").replace("(HID)", "").strip()
            if c_clean in barras_eol: nuevas_cols.append(f"{c_clean} (EOL)")
            elif c_clean in barras_solar: nuevas_cols.append(f"{c_clean} (SOL)")
            elif c_clean in barras_rer: nuevas_cols.append(f"{c_clean} (HID)")
            else: nuevas_cols.append(f"{c_clean} (RER)")
        df.columns = nuevas_cols
        
    return df

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
                        datos_dia["Dataframes"][nombre] = {
                            "HIDRO": parse_yupana(cargar_df_desde_zip(zf, archivos_clave["HIDRO"]), "HIDRO"),
                            "TERMICA": parse_yupana(cargar_df_desde_zip(zf, archivos_clave["TERMICA"]), "TERMICA"),
                            "RER": parse_yupana(cargar_df_desde_zip(zf, archivos_clave["RER"]), "RER"),
                            "CMG": parse_yupana(cargar_df_desde_zip(zf, archivos_clave["CMG"]), "")
                        }
                datos_dia["Log"].append(f"✅ {nombre}")
            else:
                datos_dia["Log"].append(f"❌ {nombre} (No publicado)")
        except Exception:
            datos_dia["Log"].append(f"❌ {nombre} (Error de red)")
    return datos_dia

# --- 4. MOTOR GRÁFICO ---
def crear_grafica_area_apilada(df_plot, titulo_grafico):
    """Genera área apilada con Total Sistema en Hover y omisión de valores en 0."""
    df_plot = df_plot.fillna(0)
    
    # Suma Total para Pico Máximo y Hover
    df_plot['TOTAL_SISTEMA'] = df_plot.drop(columns=['Hora']).sum(axis=1)
    idx_pico = df_plot['TOTAL_SISTEMA'].idxmax()
    pico_mw = df_plot.loc[idx_pico, 'TOTAL_SISTEMA']
    pico_hora = df_plot.loc[idx_pico, 'Hora']
    
    # Orden Jerárquico de base a punta
    totales_por_unidad = df_plot.drop(columns=['Hora', 'TOTAL_SISTEMA']).sum()
    orden_columnas = totales_por_unidad.sort_values(ascending=False).index.tolist()
    
    # Preparar Melt
    cols_mantener = ['Hora', 'TOTAL_SISTEMA'] + orden_columnas
    df_melt = df_plot[cols_mantener].melt(id_vars=['Hora', 'TOTAL_SISTEMA'], var_name='Unidad Generadora', value_name='Potencia_MW')
    
    # Reemplazar 0 por NaN para invisibilidad en tooltip interactivo
    df_melt['Potencia_Plot'] = df_melt['Potencia_MW'].replace(0, np.nan)
    
    fig = px.area(
        df_melt, x="Hora", y="Potencia_Plot", color="Unidad Generadora", 
        title=titulo_grafico, labels={"Potencia_Plot": "Potencia Activa (MW)"}
    )
    fig.update_traces(hovertemplate="%{y:,.2f} MW")
    
    # Inyectar el Total del Sistema en el Hover
    fig.add_scatter(
        x=df_plot['Hora'], y=df_plot['TOTAL_SISTEMA'], mode='lines',
        line=dict(width=0, color='rgba(0,0,0,0)'), name='<b>⚡ TOTAL SISTEMA</b>',
        hovertemplate='<b>%{y:,.2f} MW</b>', showlegend=False
    )
    
    # Anotación Pico Máximo
    fig.add_annotation(
        x=pico_hora, y=pico_mw, text=f"<b>Pico Máximo: {pico_mw:,.2f} MW</b><br>{pico_hora}",
        showarrow=True, arrowhead=2, arrowsize=1.5, arrowwidth=2, arrowcolor="#e74c3c",
        ax=0, ay=-50, font=dict(size=12, color="#c0392b"),
        bgcolor="rgba(255,255,255,0.8)", bordercolor="#c0392b", borderwidth=1, borderpad=4
    )
    
    fig.update_layout(hovermode="x unified", height=650, xaxis=dict(tickangle=45))
    return fig

# --- 5. INTERFAZ Y EJECUCIÓN ---
st.sidebar.header("Parámetros de Fiscalización")
rango_fechas = st.sidebar.date_input("Intervalo de Fechas (YUPANA)", value=(datetime(2025, 7, 6), datetime(2025, 7, 8)))

if st.sidebar.button("Extraer Curvas y Motivos", type="primary"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        ini, fin = rango_fechas
        st.session_state['fecha_ini'], st.session_state['fecha_fin'] = ini, fin
        
        status, prog_bar = st.empty(), st.progress(0)
        # Bitácora retraída por defecto (expanded=False)
        log_exp = st.expander("Bitácora de Extracción COES", expanded=False)
        
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
        status.success("✅ Extracción y vectorización completadas.")
        prog_bar.empty()

# --- 6. VISUALIZACIÓN ---
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
    
    t_cmg, t_hidro, t_term, t_dem, t_eol, t_sol, t_motivos = st.tabs([
        "💸 CMG", "💧 Despacho Hidro", "🔥 Despacho Térmico", "📈 Demanda Sistema", 
        "💨 Despacho Eólico", "☀️ Despacho Solar", "📋 Intervenciones"
    ])

    # === CMG ===
    with t_cmg:
        st.markdown(f"### Evolución Intradiaria CMG - {fecha_analisis.strftime('%d/%m/%Y')}")
        for barra in barras:
            df_plot = pd.DataFrame({'Hora': horas_str})
            datos_existen = False
            for prog in programas_validos:
                if not datos_dia_sel[prog]["CMG"].empty and barra in datos_dia_sel[prog]["CMG"].columns:
                    df_plot[prog.replace("_", " ")] = datos_dia_sel[prog]["CMG"][barra]
                    datos_existen = True
                    
            if datos_existen:
                fig = px.line(df_plot, x='Hora', y=df_plot.columns[1:], markers=True, title=f"CMG - {barra}", labels={"value": "USD/MWh", "variable": "Programa"})
                fig.update_layout(hovermode="x unified", height=500)
                st.plotly_chart(fig, use_container_width=True)

    # === HIDRO ===
    with t_hidro:
        st.markdown(f"### 💧 Despacho Hidroeléctrico Detallado - {fecha_analisis.strftime('%d/%m/%Y')}")
        todas_hidro = set()
        for prog in programas_validos:
            df_h, df_r = datos_dia_sel[prog]["HIDRO"], datos_dia_sel[prog]["RER"]
            if not df_h.empty:
                df_h_completo = pd.concat([df_h, df_r[[c for c in df_r.columns if "(HID)" in c]]], axis=1)
                todas_hidro.update(df_h_completo.columns[df_h_completo.sum() > 0].tolist())
            
        st.markdown("#### 🔍 Filtros Operativos")
        hc1, hc2, hc3, hc4 = st.columns(4)
        with hc1: st.selectbox("🏢 Empresa:", ["Todas"], disabled=True, key='h_emp')
        with hc2: st.selectbox("🏭 Tipo Generación:", ["Hidráulica (HID)"], disabled=True, key='h_tip')
        with hc3: st.selectbox("🌍 Zona:", ["Todas"], disabled=True, key='h_zon')
        with hc4: filtro_hidro = st.multiselect("⚡ Centrales:", options=sorted(list(todas_hidro)), default=sorted(list(todas_hidro)), placeholder="Buscar o seleccionar...", key='h_cen')
        
        for prog in programas_validos:
            df_h, df_r = datos_dia_sel[prog]["HIDRO"], datos_dia_sel[prog]["RER"]
            if not df_h.empty:
                df_h_completo = pd.concat([df_h, df_r[[c for c in df_r.columns if "(HID)" in c]]], axis=1)
                cols_plot = [c for c in df_h_completo.columns if c in filtro_hidro and df_h_completo[c].sum() > 0]
                
                if cols_plot:
                    df_plot = df_h_completo[cols_plot].copy()
                    df_plot.insert(0, 'Hora', horas_str)
                    titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                    st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                    st.markdown("---")

    # === TÉRMICA ===
    with t_term:
        st.markdown(f"### 🔥 Despacho Térmico Detallado - {fecha_analisis.strftime('%d/%m/%Y')}")
        todas_term = set()
        for prog in programas_validos:
            df_t = datos_dia_sel[prog]["TERMICA"]
            if not df_t.empty: todas_term.update(df_t.columns[df_t.sum() > 0].tolist())
            
        st.markdown("#### 🔍 Filtros Operativos")
        tc1, tc2, tc3, tc4 = st.columns(4)
        with tc1: st.selectbox("🏢 Empresa:", ["Todas"], disabled=True, key='t_emp')
        with tc2: st.selectbox("🏭 Tipo Generación:", ["Térmica (TER)"], disabled=True, key='t_tip')
        with tc3: st.selectbox("🌍 Zona:", ["Todas"], disabled=True, key='t_zon')
        with tc4: filtro_term = st.multiselect("⚡ Centrales:", options=sorted(list(todas_term)), default=sorted(list(todas_term)), placeholder="Buscar o seleccionar...", key='t_cen')
        
        for prog in programas_validos:
            df_t = datos_dia_sel[prog]["TERMICA"]
            if not df_t.empty:
                cols_plot = [c for c in df_t.columns if c in filtro_term and df_t[c].sum() > 0]
                if cols_plot:
                    df_plot = df_t[cols_plot].copy()
                    df_plot.insert(0, 'Hora', horas_str)
                    titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                    st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)
                    st.markdown("---")

    # === DEMANDA ===
    with t_dem:
        st.markdown(f"### 📈 Demanda Sistema - {fecha_analisis.strftime('%d/%m/%Y')}")
        fig_dem = go.Figure()
        for prog in programas_validos:
            if not datos_dia_sel[prog]["HIDRO"].empty:
                demanda_total = datos_dia_sel[prog]["HIDRO"].sum(axis=1) + datos_dia_sel[prog]["TERMICA"].sum(axis=1) + datos_dia_sel[prog]["RER"].sum(axis=1)
                if demanda_total.sum() > 0:
                    fig_dem.add_trace(go.Scatter(x=horas_str, y=demanda_total, mode='lines+markers', name=prog.replace('_', ' ')))
        fig_dem.update_layout(title="Curva de Carga Total", hovermode="x unified", yaxis_title="MW")
        st.plotly_chart(fig_dem, use_container_width=True)

    # === EÓLICO ===
    with t_eol:
        st.markdown(f"### 💨 Despacho Eólico Desglosado - {fecha_analisis.strftime('%d/%m/%Y')}")
        for prog in programas_validos:
            df_r = datos_dia_sel[prog]["RER"]
            if not df_r.empty:
                cols_plot = [c for c in df_r.columns if "(EOL)" in c and df_r[c].sum() > 0]
                if cols_plot:
                    df_plot = df_r[cols_plot].copy()
                    df_plot.insert(0, 'Hora', horas_str)
                    titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                    st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)

    # === SOLAR ===
    with t_sol:
        st.markdown(f"### ☀️ Despacho Solar Desglosado - {fecha_analisis.strftime('%d/%m/%Y')}")
        for prog in programas_validos:
            df_r = datos_dia_sel[prog]["RER"]
            if not df_r.empty:
                cols_plot = [c for c in df_r.columns if "(SOL)" in c and df_r[c].sum() > 0]
                if cols_plot:
                    df_plot = df_r[cols_plot].copy()
                    df_plot.insert(0, 'Hora', horas_str)
                    titulo = "Programa Diario de Operación (PDO)" if prog == "PDO" else f"Reprograma ({prog.replace('_', ' ')})"
                    st.plotly_chart(crear_grafica_area_apilada(df_plot, titulo), use_container_width=True)

    # === MOTIVOS ===
    with t_motivos:
        if "Intervenciones" in datos_dia_sel and not datos_dia_sel["Intervenciones"].empty:
            df_mot = datos_dia_sel["Intervenciones"].dropna(how='all').dropna(axis=1, how='all')
            st.dataframe(df_mot, use_container_width=True)
        else:
            st.warning("No hay intervenciones reportadas (Anexo 1) para este día en el portal del COES.")