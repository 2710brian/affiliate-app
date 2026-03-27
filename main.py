import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide")

# --- 2. DATABASE MOTOR ---
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS merchants (id SERIAL PRIMARY KEY, data JSONB)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS settings (type TEXT, value TEXT)"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. MASTER DEFINITIONER ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

# --- 4. DROPDOWN LOGIK (FIXET: MISTER ALDRIG VALG) ---
def load_options():
    # Dine faste standardvalg
    defaults = {
        "aff_status": ["Godkendt", "Afvist", "Ikke ansøgt", "Lukket ned", "Afventer", "Pause"],
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Tradetracker", "Awin", "GJ", "Daisycon", "Shopnello", "TradeDoubler", "Webigans"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                # Vi lægger gemte ting sammen med defaults og fjerner dubletter
                defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

def add_dropdown_option(t, v):
    if db_engine and v:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t":t, "v":v})
            conn.commit()

# --- 5. RENSE- OG REPARATIONS-MOTOR ---
def robust_repair(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    # Fjern dublerede kolonner (Vigtigst for stabilitet)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # Map navne
    ren = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=ren)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # Rens indhold
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    # Tving master struktur
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_to_db(df):
    if db_engine:
        df = robust_repair(df)
        df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# --- 6. INITIALISERING ---
if 'df' not in st.session_state:
    try:
        st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
    except:
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS)

st.session_state.df = robust_repair(st.session_state.df)
opts = load_options()

# --- 7. POP-UP KLIENT KORT ---
@st.dialog("📝 Klient-kort / Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    
    t1, t2 = st.tabs(["📊 Data & Pipeline", "📓 Noter & Filer"])
    upd = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            # DIALOG STATUS
            d_val = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(d_val) if d_val in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr',''))
            
            def sd(v):
                try: return pd.to_datetime(v, dayfirst=True).date()
                except: return date.today()
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=sd(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=sd(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Aff. Info")
            # AFF STATUS (FIXET)
            a_val = row.get('Aff. status', 'Ikke ansøgt')
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(a_val) if a_val in opts['aff_status'] else 0)
            
            upd['Kategori'] = st.text_input("Hovedkategori", value=row.get('Kategori',''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter',''))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC',''))

        st.divider()
        st.markdown("##### 📊 Systemdata")
        ca, cb, cc = st.columns(3)
        with ca: upd['Date Added'] = st.text_input("Date Added", value=row.get('Date Added',''))
        with cb: upd['Network'] = st.selectbox("Network", opts['networks'], index=opts['networks'].index(row.get('Network')) if row.get('Network') in opts['networks'] else 0)
        with cc: upd['Land'] = st.selectbox("Land", opts['lands'], index=opts['lands'].index(row.get('Land')) if row.get('Land') in opts['lands'] else 0)

    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter", value=row.get('Noter',''), height=300)

    if st.button("💾 GEM KLIENT DATA", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_to_db(st.session_state.df): st.rerun()

# --- 8. UI SIDEBAR ---
with st.sidebar:
    st.title("⚙️ CRM Kontrol")
    
    with st.expander("🛠️ Administrer Dropdowns"):
        t_sel = st.selectbox("Vælg type:", ["aff_status", "networks", "lands", "dialogs"])
        v_new = st.text_input("Tilføj nyt valg:")
        if st.button("Tilføj nu") and v_new:
            add_dropdown_option(t_sel, v_new)
            st.rerun()

    st.divider()
    st.subheader("Eksport")
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'sel_rows' in st.session_state and len(st.session_state.sel_rows) > 0:
        st.download_button("📥 Download UDVALGTE", st.session_state.df.iloc[st.ses
