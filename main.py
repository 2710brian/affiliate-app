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

# --- 3. ADGANGS-LÅS (Sikret mod Railway-fejl) ---
# Tjekker først Railway Variables, ellers standard
correct_pw = os.getenv("APP_PASSWORD", "mgm2024")

if "auth" not in st.session_state:
    st.session_state.auth = False

if not st.session_state.auth:
    st.title("🔒 CRM Master - Adgangskontrol")
    pwd_input = st.text_input("Indtast adgangskode", type="password")
    if st.button("Åbn Database", type="primary"):
        if pwd_input == correct_pw:
            st.session_state.auth = True
            st.rerun()
        else:
            st.error("❌ Forkert adgangskode")
    st.stop()

# --- 4. MASTER DEFINITIONER ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

# --- 5. DROPDOWN & INDSTILLINGER ---
def load_options():
    defaults = {
        "aff_status": ["Godkendt", "Afvist", "Ikke ansøgt", "Lukket ned", "Afventer", "Pause"],
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Tradetracker", "Awin", "TradeDoubler", "Webigans"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Infomail sendt", "Cold Mail", "Kontaktet", "Vundet", "Tabt", "Call"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

def add_dropdown_option(t, v):
    if db_engine and v:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t":t, "v":v})
            conn.commit()

# --- 6. RENSE- OG REPARATIONS LOGIK ---
def robust_repair(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    # Fjern dublerede kolonner
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # Map navne
    ren = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=ren)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # Rens
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00', 'nan.0'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_to_db(df):
    if db_engine:
        df = robust_repair(df)
        df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# Initialiser
if 'df' not in st.session_state:
    try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
    except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)

st.session_state.df = robust_repair(st.session_state.df)
opts = load_options()

# --- 7. POP-UP KLIENT KORT ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
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
            d_val = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(d_val) if d_val in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr',''))
            def parse_d(v):
                try: return pd.to_datetime(v, dayfirst=True).date()
                except: return date.today()
            upd['Opflg. dato'] = st.date_input("Opfølgning", value=parse_d(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=parse_d(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Info")
            a_val = row.get('Aff. status', 'Ikke ansøgt')
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(a_val) if a_val in opts['aff_status'] else 0)
            for f in ['Kategori', 'MID', 'Produkter', 'EPC']: upd[f] = st.text_input(f, value=row.get(f,''))

    with t2:
        upd['Noter'] = st.text_area("📓 Klient Noter", value=row.get('Noter',''), height=300)

    if st.button("💾 GEM ÆNDRINGER", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_to_db(st.session_state.df): st.rerun()

# --- 8. UI SIDEBAR ---
with st.sidebar:
    st.header("⚙️ CRM Kontrol")
    
    # SORTERING
    with st.expander("📊 Sortering"):
        s_col = st.selectbox("Sortér efter:", ["Date Added", "Produkter", "EPC", "Virksomhed", "Dialog"])
        s_asc = st.radio("Orden:", ["Nyeste/Højeste", "Ældste/Laveste"])
        if st.button("Udfør Sortering"):
            st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_asc=="Ældste/Laveste"))
            save_to_db(st.session_state.df); st.rerun()

    # DROPDOWN ADMINISTRATION
    with st.expander("🛠️ Administrer Dropdowns"):
        t_sel = st.selectbox("Type:", ["aff_status", "networks", "lands", "dialogs"])
        v_new = st.text_input("Tilføj nyt valg:")
        if st.button("Tilføj nu") and v_new: add_dropdown_option(t_sel, v_new); st.rerun()

    st.divider()
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    
    st.divider()
    st.subheader("Import")
    kat_up = st.text_input("Kategori:", "Bolig")
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = robust_repair(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_to_db(st.session_state.df); st.rerun()
    
    if st.button("🚪 Lås CRM"):
        st.session_state.auth = False; st.rerun()

# --- 9. HOVEDVISNING ---
st.title("💼 Affiliate CRM Master")
search = st.text_input("🔍 Søg i CRM...", "")
df_v = st.session_state.df.copy()
if search:
    df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v, use_container_width=True, selection_mode="single-row", on_select="rerun", height=600, column_config={"Website": st.column_config.LinkColumn("Website")})

if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    client_popup(real_idx)
