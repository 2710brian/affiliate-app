import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Pro", layout="wide")

# --- 2. DATABASE FORBINDELSE ---
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            # Opret settings tabel hvis den mangler
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS settings (type TEXT, value TEXT)"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. MASTER STRUKTUR ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

# --- 4. DROPDOWN ADMINISTRATION ---
def load_options():
    defaults = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Tradetracker", "Awin", "GJ", "Daisycon", "Shopnello", "TradeDoubler", "Webigans"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"],
        "aff_status": ["Godkendt", "Ikke ansøgt", "Afvist", "Afventer", "Pause"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(stored)))
        except: pass
    return defaults

def add_option(opt_type, value):
    if db_engine and value:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t": opt_type, "v": value})
            conn.commit()

# --- 5. RENSE- OG MAPPING-MOTOR ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean_data(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    
    # Map navne fra alle fil-typer
    rename_map = {
        'Programnavn': 'Virksomhed', 'Merchant': 'Virksomhed', 
        'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 
        'DateAdded': 'Date Added', 'Status': 'Aff. status', 
        'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'
    }
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()] # FJERN DUBLETTER
    
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')

    # WEBSITE GENERATOR
    def set_site(r):
        s, v = str(r.get('Website', '')), str(r.get('Virksomhed', ''))
        if (s == "" or s == "None") and v != "":
            c = re.sub(r'[^a-z0-9]', '', v.lower())
            return f"https://www.{c}.dk" if c else ""
        return s
    df['Website'] = df.apply(set_site, axis=1)

    return df.reindex(columns=MASTER_COLS, fill_value="")

def load_db():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return force_clean_data(df)
        except: return pd.DataFrame(columns=MASTER_COLS)
    return pd.DataFrame(columns=MASTER_COLS)

def save_db(df):
    if db_engine:
        try:
            df = force_clean_data(df)
            df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
            df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Save Error: {e}")
            return False
    return False

# --- 6. APP START ---
if 'df' not in st.session_state:
    st.session_state.df = load_db()
opts = load_options()

# --- 7. KLIENT KORT POP-UP ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["Stamdata & Pipeline", "Noter & Systemdata"])
    upd = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']:
                upd[f] = st.text_input(f, value=row.get(f, ''))
        with c2:
            st.markdown("##### ⚙️ Pipeline (Status 1)")
            d_v = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(d_v) if d_v in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr', ''))
            
            # KALENDER VÆLGERE
            upd['Opflg. dato'] = st.date_input("Opfølgning", value=get_safe_date(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=get_safe_date(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Info (Status 2)")
            # AFF STATUS DROPDOWN
            a_v = row.get('Aff. status', 'Ikke ansøgt')
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(a_v) if a_v in opts['aff_status'] else 0)
            
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori', ''))
            upd['MID'] = st.text_input("MID", value=row.get('MID', ''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter', ''))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC', ''))

    with t2:
        c_a, c_b = st.columns([2, 1])
        with c_a:
            upd['Noter'] = st.text_area("📓 Interne Noter", value=row.get('Noter', ''), height=250)
        with c_b:
            st.markdown("##### 📊 Systemdata")
            # DATE ADDED KALENDER
            upd['Date Added'] = st.date_input("Dato tilføjet", value=get_safe_date(row.get('Date Added'))).strftime('%d/%m/%Y')
            
            n_v = row.get('Network', 'Partner-ads')
            upd['Network'] = st.selectbox("Netværk", opts['networks'], index=opts['networks'].index(n_v) if n_v in opts['networks'] else 0)
            
            l_v = row.get('Land', 'DK')
            upd['Land'] = st.selectbox("Land", opts['lands'], index=opts['lands'].index(l_v) if l_v in opts['lands'] else 0)
            
            for f in ['Segment', 'Salgs % (sats)', 'Lead/Fast (sats)', 'Trafik']:
                upd[f] = st.text_input(f, value=row.get(f, ''))

    if st.button("💾 GEM KLIENT KORT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 8. SIDEBAR ---
with st.sidebar:
    st.title("💼 CRM Kontrol")
    
    with st.expander("🛠️ Administrer Dropdowns"):
        t_sel = st.selectbox("Type:", ["networks", "lands", "dialogs", "aff_status"])
        v_new = st.text_input("Nyt navn:")
        if st.button("Tilføj") and v_new: add_option(t_sel, v_new); st.rerun()

    st.divider()
    st.header("📤 Eksport")
    st.download_button("📥 Master CSV", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'filtered' in st.session_state:
        st.download_button("📥 Udvalgte CSV", st.session_state.filtered.to_csv(index=False), "udvalgte.csv", use_container_width=True)

    st.divider()
    st.header("📥 Import")
    kat_up = st.text_input("Kategori:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Upload fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd = force_clean_data(nd)
        nd['Kategori'] = kat_up
        st.session_state.df = force_clean_data(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_db(st.session_state.df); st.rerun()

    if st.button("🚨 NULSTIL DATABASE"):
        if db_engine:
            with db_engine.connect() as conn:
                for t in ['merchants', 'settings']: conn.execute(text(f"DROP TABLE IF EXISTS {t}"))
                conn.commit()
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 9. HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    df_show = st.session_state.df.copy()
    if search:
        mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_show = df_show[mask]
    st.session_state.filtered = df_show

    st.write(f"Antal: **{len(df_show)}**")
    sel = st.dataframe(
        df_show,
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_show.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("👋 Upload din fil for at starte.")
