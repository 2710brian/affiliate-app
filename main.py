import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. SESSION INITIALISERING (SKAL KØRE FØR ALT ANDET) ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# --- 2. KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide", page_icon="💼")

# Hjælpefunktion til billeder
def get_base64(bin_file):
    if os.path.exists(bin_file):
        with open(bin_file, 'rb') as f:
            data = f.read()
        return base64.b64encode(data).decode()
    return ""

# --- 3. DATABASE MOTOR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            return create_engine(db_url, pool_pre_ping=True)
        except: return None
    return None

db_engine = get_engine()

# --- 4. LOGIN SKÆRM (HVIS IKKE LOGGET IND) ---
if not st.session_state.authenticated:
    bg_b64 = get_base64("background.png")
    logo_b64 = get_base64("applogo.png")
    
    st.markdown(f"""
        <style>
        .stApp {{
            background: url("data:image/png;base64,{bg_b64}") no-repeat center center fixed;
            background-size: cover;
        }}
        .login-card {{
            background-color: rgba(255, 255, 255, 0.9);
            padding: 30px;
            border-radius: 15px;
            text-align: center;
            border: 1px solid #ddd;
        }}
        .corner-logo {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 150px;
        }}
        </style>
        {"<img src='data:image/png;base64," + logo_b64 + "' class='corner-logo'>" if logo_b64 else ""}
    """, unsafe_allow_html=True)

    # Centrer login ved hjælp af Streamlit kolonner (SIKKER METODE)
    st.markdown("<br><br><br><br>", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 1, 1])
    
    with col:
        st.markdown("<div class='login-card'>", unsafe_allow_html=True)
        st.subheader("💼 CRM MASTER LOGIN")
        u_name = st.text_input("Brugernavn", key="user_box")
        p_name = st.text_input("Adgangskode", type="password", key="pass_box")
        
        # Knappen er nu uden for forms eller mærkelige lag
        if st.button("START ARBEJDSDAG", use_container_width=True, type="primary"):
            # Hent koder fra Railway
            r_u = os.getenv("APP_USER", "admin")
            r_p = os.getenv("APP_PASSWORD", "admin123")
            
            if u_name == r_u and p_name == r_p:
                st.session_state.authenticated = True
                st.rerun()
            else:
                # Tjek databasen hvis ikke admin
                if db_engine:
                    with db_engine.connect() as conn:
                        res = conn.execute(text("SELECT password FROM users WHERE username = :u"), {"u": u_name}).fetchone()
                        if res and res[0] == p_name:
                            st.session_state.authenticated = True
                            st.rerun()
                        else:
                            st.error("Ugyldig adgangskode")
        st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

# --- 5. CRM SYSTEM (KUN SYNLIGT VED LOGIN) ---

MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    ren = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=ren).loc[:, ~df.columns.duplicated()]
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    if 'Virksomhed' in df.columns:
        df['Website'] = df.apply(lambda r: f"https://www.{re.sub(r'[^a-z0-9]', '', str(r['Virksomhed']).lower())}.dk" if str(r.get('Website','')) == '' else r['Website'], axis=1)
    return df.reindex(columns=MASTER_COLS, fill_value="")

# Indlæs data
if 'df' not in st.session_state:
    if db_engine:
        try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
        except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)
    else: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)

st.session_state.df = force_clean(st.session_state.df)

# --- KLIENT KORT POP-UP ---
@st.dialog("📝 Klient-kort", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    t1, t2 = st.tabs(["📊 Stamdata & Pipeline", "📓 Noter & Filer"])
    upd = {}
    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            upd['Dialog'] = st.text_input("Dialog Status", value=row.get('Dialog',''))
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr',''))
            upd['Opflg. dato'] = st.text_input("Opfølgning", value=row.get('Opflg. dato',''))
            upd['Kontakt dato'] = st.text_input("Kontakt dato", value=row.get('Kontakt dato',''))
        with c3:
            upd['Aff. status'] = st.text_input("Aff. status", value=row.get('Aff. status',''))
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori',''))
            upd['MID'] = st.text_input("MID", value=row.get('MID',''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter',''))
    with t2:
        upd['Noter'] = st.text_area("Noter", value=row.get('Noter',''), height=300)
    
    if st.button("💾 GEM"):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if db_engine: st.session_state.df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        st.rerun()

# --- SIDEBAR ---
with st.sidebar:
    st.title("⚙️ Kontrol")
    if st.button("🚪 Log ud"):
        st.session_state.authenticated = False
        st.rerun()
    st.divider()
    st.download_button("📥 Eksport Master", st.session_state.df.to_csv(index=False), "master.csv")
    st.divider()
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = force_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        if db_engine: st.session_state.df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        st.rerun()

# --- MAIN ---
st.title("💼 CRM Master Workspace")
search = st.text_input("🔍 Søg...")
df_v = st.session_state.df.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[[c for c in df_v.columns if c != 'Fil_Data']], column_config={"Website": st.column_config.LinkColumn("Website")}, use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    client_popup(real_idx)
