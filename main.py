import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION (ABSOLUT TOP) ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide", page_icon="💼")

# Funktion til at læse billeder
def get_base64(bin_file):
    if os.path.exists(bin_file):
        with open(bin_file, 'rb') as f:
            data = f.read()
        return base64.b64encode(data).decode()
    return None

# --- 2. DATABASE MOTOR ---
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS merchants (id SERIAL PRIMARY KEY, data JSONB)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS settings (type TEXT, value TEXT)"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. LOGIN LOGIK ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    bg_b64 = get_base64("background.png")
    logo_b64 = get_base64("applogo.png")
    
    st.markdown(f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{bg_b64 if bg_b64 else ''}");
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
        }}
        .login-box-clean {{
            background-color: rgba(255, 255, 255, 0.7);
            padding: 30px;
            border-radius: 15px;
            max-width: 400px;
            margin: 100px auto;
            text-align: center;
        }}
        .corner-logo {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 150px;
            z-index: 1000;
        }}
        </style>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 1, 1])
    with col:
        st.markdown("<div class='login-box-clean'>", unsafe_allow_html=True)
        st.subheader("CRM Master Login")
        u_input = st.text_input("Brugernavn")
        p_input = st.text_input("Adgangskode", type="password")
        
        if st.button("Log ind", use_container_width=True, type="primary"):
            # TJEK FØRST RAILWAY VARIABLER (Højeste prioritet)
            rail_user = os.getenv("APP_USER", "admin")
            rail_pass = os.getenv("APP_PASSWORD", "admin123")
            
            if u_input == rail_user and p_input == rail_pass:
                st.session_state.authenticated = True
                st.rerun()
            else:
                # Tjek derefter databasen for andre brugere
                if db_engine:
                    with db_engine.connect() as conn:
                        res = conn.execute(text("SELECT password FROM users WHERE username = :u"), {"u": u_input}).fetchone()
                        if res and res[0] == p_input:
                            st.session_state.authenticated = True
                            st.rerun()
                        else:
                            st.error("Ugyldig adgangskode")
        st.markdown("</div>", unsafe_allow_html=True)
    
    if logo_b64:
        st.markdown(f'<img src="data:image/png;base64,{logo_b64}" class="corner-logo">', unsafe_allow_html=True)
    st.stop()

# --- 4. CRM FUNKTIONALITET ---

MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    
    # 1. FJERN DUBLETTER I KOLONNER INDEN OMDØBNING (Løser ValueError)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 2. OMDØB
    rename_map = {'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=rename_map)
    
    # 3. FJERN DUBLETTER IGEN (Hvis omdøbning skabte to kolonner med samme navn)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 4. RENS
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

# Indlæs data
if 'df' not in st.session_state:
    if db_engine:
        try:
            st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
        except:
            st.session_state.df = pd.DataFrame(columns=MASTER_COLS)
    else:
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS)

st.session_state.df = force_clean(st.session_state.df)

# --- (Resten af dine CRM funktioner som f.eks. client_popup, sidebar osv. skal fortsætte her) ---
# For at holde koden overskuelig genbruger vi din eksisterende logik for pop-up og sidebar herunder:

def load_options():
    opts = {"networks": ["Partner-ads", "Awin", "Adtraction"], "lands": ["DK", "SE", "NO"], "aff_status": ["Godkendt", "Ikke ansøgt"], "dialogs": ["Ikke kontakte", "Kontaktet", "Vundet"]}
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in opts.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: opts[key] = sorted(list(set(opts[key] + stored)))
        except: pass
    return opts

opts = load_options()

@st.dialog("📝 Klient-kort", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    t1, t2 = st.tabs(["📊 Data", "📓 Noter"])
    upd = {}
    with t1:
        c1, c2 = st.columns(2)
        with c1:
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            upd['Dialog'] = st.selectbox("Dialog", opts['dialogs'], index=opts['dialogs'].index(row.get('Dialog')) if row.get('Dialog') in opts['dialogs'] else 0)
            upd['Opflg. dato'] = st.text_input("Opfølgning", value=row.get('Opflg. dato',''))
            upd['Aff. status'] = st.text_input("Aff. status", value=row.get('Aff. status',''))
    with t2:
        upd['Noter'] = st.text_area("Noter", value=row.get('Noter',''), height=300)
    
    if st.button("💾 GEM"):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        # Gem til DB
        st.session_state.df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        st.rerun()

# --- SIDEBAR & TABEL ---
with st.sidebar:
    st.header("⚙️ CRM Menu")
    if st.button("🚪 Log ud"):
        st.session_state.authenticated = False
        st.rerun()
    st.divider()
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = force_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        st.session_state.df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        st.rerun()

st.title("💼 CRM Workspace")
search = st.text_input("🔍 Søg...")
df_v = st.session_state.df.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v, use_container_width=True, selection_mode="single-row", on_select="rerun")
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    client_popup(real_idx)
