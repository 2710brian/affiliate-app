import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="MGM CRM Pro", layout="wide")

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

# --- 3. ADGANGS-LÅS (SIMPEL & SKUDSIKKER) ---
# Din adgangskode er mgm2024 (Du kan rette den herunder)
correct_pw = "mgm2024"

if "auth" not in st.session_state:
    st.session_state.auth = False

if not st.session_state.auth:
    st.title("🔒 CRM Master - Adgangskontrol")
    pwd_input = st.text_input("Indtast adgangskode for at se databasen", type="password")
    if st.button("Åbn Database", type="primary"):
        if pwd_input == correct_pw:
            st.session_state.auth = True
            st.rerun()
        else:
            st.error("❌ Forkert adgangskode")
    st.stop() # Alt herunder er usynligt indtil koden er rigtig

# --- HERUNDER STARTER CRM (KUN SYNLIGT VED LOG IND) ---

# --- 4. MASTER KOLONNER (DINE 26 FELTER) ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

DIALOG_OPTIONS = ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Kontaktet", "Vundet", "Tabt", "Call"]

# --- 5. RENSE- OG REPARATIONS LOGIK ---
def repair_data(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    
    # TRIN 1: Fjern alle fysisk dublerede kolonner i databasen (Løser ValueError)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # TRIN 2: Omdøbning
    rename_map = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # TRIN 3: Rens indhold
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00', 'nan.0'], '')
    
    # TRIN 4: Tving rækkefølge
    return df.reindex(columns=MASTER_COLS, fill_value="")

def load_from_db():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return repair_data(df)
        except: return pd.DataFrame(columns=MASTER_COLS)
    return pd.DataFrame(columns=MASTER_COLS)

def save_to_db(df):
    if db_engine:
        df = repair_data(df)
        # Sørg for unikt match på navn
        df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# Indlæs data til CRM
if 'df' not in st.session_state:
    st.session_state.df = load_from_db()
st.session_state.df = repair_data(st.session_state.df)

# --- 6. KLIENT KORT POP-UP ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    
    t1, t2 = st.tabs(["📊 Data & Pipeline", "📓 Noter & Dokumenter"])
    upd = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            d_val = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", DIALOG_OPTIONS, index=DIALOG_OPTIONS.index(d_val) if d_val in DIALOG_OPTIONS else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr',''))
            upd['Opflg. dato'] = st.text_input("Opfølgningsdato", value=row.get('Opflg. dato',''))
            upd['Kontakt dato'] = st.text_input("Kontakt dato", value=row.get('Kontakt dato',''))
        with c3:
            st.markdown("##### 📈 Aff. Info")
            upd['Aff. status'] = st.text_input("Aff. status", value=row.get('Aff. status',''))
            for f in ['Kategori', 'MID', 'Produkter', 'EPC']: upd[f] = st.text_input(f, value=row.get(f,''))

    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter", value=row.get('Noter',''), height=300)

    if st.button("💾 GEM ÆNDRINGER", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_to_db(st.session_state.df): st.rerun()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.title("⚙️ Kontrol")
    if st.button("🚪 Lås CRM"):
        st.session_state.auth = False
        st.rerun()
    st.divider()
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    st.divider()
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = repair_data(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_to_db(st.session_state.df)
        st.rerun()

# --- 8. HOVEDVISNING ---
st.title("💼 Affiliate CRM Master")
search = st.text_input("🔍 Søg i CRM...", "")
df_v = st.session_state.df.copy()
if search:
    df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

# TABELVISNING
sel = st.dataframe(
    df_v, 
    use_container_width=True, 
    selection_mode="single-row", 
    on_select="rerun", 
    height=600,
    column_config={"Website": st.column_config.LinkColumn("Website")}
)

if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    client_popup(real_idx)
