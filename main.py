import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date
import base64

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="CRM Master Stabel", layout="wide")

# --- 2. DATABASE MOTOR ---
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

# --- 3. ADGANGSKONTROL ---
correct_code = os.getenv("APP_PASSWORD", "mgm2024")
access_code = st.text_input("Indtast Adgangskode", type="password")

if access_code != correct_code:
    st.info("Indtast koden for at åbne CRM.")
    st.stop()

# --- 4. MASTER DEFINITIONER ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

# --- 5. RENSE-LOGIK (LØSER DUBLET-FEJLEN) ---
def ultra_clean(df):
    if df.empty:
        return pd.DataFrame(columns=MASTER_COLS)
    
    # TRIN 1: Fjern fysisk dublerede kolonnenavne (Løser ValueError)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # TRIN 2: Omdøbning
    rename_map = {
        'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 
        'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 
        'Status': 'Aff. status', 'Aff. Status': 'Aff. status',
        'Dato': 'Kontakt dato'
    }
    df = df.rename(columns=rename_map)
    
    # TRIN 3: Fjern dubletter igen (hvis omdøbning skabte nye)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # TRIN 4: Rens tekst
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00', 'nan.0'], '')
    
    # TRIN 5: Tving master rækkefølge
    df = df.reindex(columns=MASTER_COLS, fill_value="")
    
    return df

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return ultra_clean(df)
        except: return pd.DataFrame(columns=MASTER_COLS)
    return pd.DataFrame(columns=MASTER_COLS)

def save_data(df):
    if db_engine:
        try:
            df = ultra_clean(df)
            # Lav unikt match-id for at undgå dublet-rækker
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
            return False
    return False

# Dropdown options
def get_opts():
    opts = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Awin", "Tradetracker"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Kontaktet", "Vundet", "Tabt", "Call"],
        "aff_status": ["Godkendt", "Ikke ansøgt", "Afvist", "Afventer"]
    }
    if db_engine:
        try:
            df_s = pd.read_sql("SELECT * FROM settings", db_engine)
            for k in opts.keys():
                stored = df_s[df_s['type'] == k]['value'].tolist()
                opts[k] = sorted(list(set(opts[k] + stored)))
        except: pass
    return opts

# --- INITIALISERING ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()
options = get_opts()

# --- 6. POP-UP ---
@st.dialog("📝 Klient-kort", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    
    t1, t2 = st.tabs(["📊 Data & Pipeline", "📓 Noter & Filer"])
    upd = {}
    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            upd['Dialog'] = st.selectbox("Dialog", options['dialogs'], index=options['dialogs'].index(row.get('Dialog')) if row.get('Dialog') in options['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.ge
