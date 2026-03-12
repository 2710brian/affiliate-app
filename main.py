import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide")

# --- 2. DATABASE FORBINDELSE ---
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
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"]
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

# --- 5. RENSE- OG MERGE-MOTOR ---
def clean_name_key(name):
    if not name or pd.isna(name): return "unknown_" + os.urandom(4).hex()
    s = str(name).lower().strip()
    s = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', s)
    return re.sub(r'[^a-z0-9]', '', s)

def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try:
        return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean_data(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    
    rename_map = {
        'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 
        'EPC (nøgletal)': 'EPC', 'DateAdded': 'Date Added',
        'Status': 'Aff. status', 'Dato': 'Kontakt dato',
        'Info (Status 2)': 'Aff. status'
    }
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()] 
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0'], '')

    # Website fix
    if 'Virksomhed' in df.columns:
        def site_gen(r):
            s, v = str(r.get('Website', '')), str(r.get('Virksomhed', ''))
            if (s == "" or s == "None") and v != "":
                c = re.sub(r'[^a-z0-9]', '', v.lower())
                return f"https://www.{c}.dk" if c else ""
            return s
        df['Website'] = df.apply(site_gen, axis=1)

    return df.reindex(columns=MASTER_COLS, fill_value="")

# --- 6. DATABASE FUNKTIONER ---
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
            df['MATCH_KEY'] = df['Virksomhed'].apply(clean_name_key)
            df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Save Error: {e}")
            return False
    return False

# --- 7. SESSION START ---
if 'df' not in st.session_state:
    st.session_state.df = load_db()
opts = load_options()

# --- 8. KLIENT KORT (POP-UP) ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["Stamdata & Pipeline", "Noter & Dokumenter"])
    upd = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt Info")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']:
                upd[f] = st.text_input(f, value=row.get(f, ''))
        
        with c2:
            st.markdown("##### ⚙️ Pipeline (Status 1: Dialog)")
            d_val = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(d_val) if d_val in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr', ''))
            
            # KALENDER VÆLGERE
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=get_safe_date(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=get_safe_date(row.get('Kontakt dato'))).strftime('%d/%m/%Y')

        with c3:
            st.markdown("##### 📈 Info (Status 2: Aff. status)")
            upd['Aff. status'] = st.text_input("Aff. status", value=row.get('Aff. status', ''))
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori', ''))
            
            n_val = row.get('Network', 'Partner-ads')
            upd['Network'] = st.selectbox("Network", opts['networks'], index=opts['networks'].index(n_val) if n_val in opts['networks'] else 0)
            
            l_val = row.get('Land', 'DK')
            upd['Land'] = st.selectbox("Land Ikon", opts['lands'], index=opts['lands'].index(l_val) if l_val in opts['lands'] else 0)

        st.divider()
        st.markdown("##### 📊 Øvrige Systemdata")
        ca, cb, cc = st.columns(3)
        for i, f in enumerate(['Date Added', 'Programnavn', 'MID', 'Produkter', 'EPC', 'Segment', 'Salgs % (sats)', 'Lead/Fast (sats)', 'Trafik', 'Feed?']):
            target = [ca, cb, cc][i % 3]
            upd[f] = target.text_input(f, value=row.get(f, ''))

    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter", value=row.get('Noter', ''), height=300)

    if st.button("💾 GEM KLIENT KORT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 9. SIDEBAR ---
with st.sidebar:
    st.title("💼 CRM Kontrol")
    
    with st.expander("🛠️ Administrer Dropdowns"):
        n_new = st.text_input("Nyt Netværk:")
        if st.button("Tilføj Netværk") and n_new: add_option("networks", n_new); st.rerun()
        l_new = st.text_input("Nyt Land (Kode):")
        if st.button("Tilføj Land") and l_new: add_option("lands", l_new); st.rerun()

    st.divider()
    st.header("📤 Eksport")
    st.download_button("📥 Master CSV", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)

    st.divider()
    st.header("📥 Import")
    kat_name = st.text_input("Kategori:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Upload fil")
    if f_up and st.button("Flet & Gem Data", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd = force_clean_data(nd)
        nd['Kategori'] = kat_name
        
        # Samkøring baseret på MATCH_KEY
        st.session_state.df['MATCH_KEY'] = st.session_state.df['Virksomhed'].apply(clean_name_key)
        nd['MATCH_KEY'] = nd['Virksomhed'].apply(clean_name_key)
        
        # Flet rækkerne: update eksisterende, concat nye
        existing = st.session_state.df.set_index('MATCH_KEY')
        incoming = nd.set_index('MATCH_KEY')
        
        # Vi overskriver kun tal-data, bevarer manuelle leads
        cols_to_sync = [c for c in incoming.columns if c in ['MID', 'Programnavn', 'Produkter', 'EPC', 'Status', 'Salgs % (sats)', 'Segment']]
        existing.update(incoming[cols_to_sync])
        
        st.session_state.df = existing.combine_first(incoming).reset_index()
        save_db(st.session_state.df); st.rerun()

    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn:
                for t in ['merchants', 'settings']: conn.execute(text(f"DROP TABLE IF EXISTS {t}"))
                conn.execute(text("CREATE TABLE settings (type TEXT, value TEXT)"))
                conn.commit()
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 10. HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    df_show = st.session_state.df.copy()
    if search:
        mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_show = df_show[mask]

    st.write(f"Antal: **{len(df_show)}**")
    st.info("💡 Klik på boksen til venstre for en række for at åbne kortet.")

    sel = st.dataframe(
        df_show,
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_show.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("👋 Upload din fil for at starte CRM.")
