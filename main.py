import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date
import base64

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="CRM Master", layout="wide")

# --- 2. DATABASE FORBINDELSE ---
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
                conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS settings (type TEXT, value TEXT)"))
                res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
                if res[0] == 0:
                    conn.execute(text("INSERT INTO users (username, password) VALUES ('admin', 'admin123')"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. LOGIN SYSTEM ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("💼 Affiliate CRM Master")
    st.write("Log ind for at starte arbejdsdagen")
    
    u_in = st.text_input("Brugernavn")
    p_in = st.text_input("Adgangskode", type="password")
    
    if st.button("Log ind", type="primary"):
        rail_u = os.getenv("APP_USER", "admin")
        rail_p = os.getenv("APP_PASSWORD", "admin123")
        
        if u_in == rail_u and p_in == rail_p:
            st.session_state.authenticated = True
            st.rerun()
        elif db_engine:
            with db_engine.connect() as conn:
                res = conn.execute(text("SELECT password FROM users WHERE username = :u"), {"u": u_in}).fetchone()
                if res and res[0] == p_in:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Ugyldigt login")
    st.stop()

# --- 4. CRM MASTER DEFINITIONER ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

def load_options():
    opts = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Awin", "Tradetracker", "GJ", "Daisycon", "Shopnello", "TradeDoubler", "Webigans"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "aff_status": ["Godkendt", "Ikke ansøgt", "Afvist", "Afventer", "Pause"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"]
    }
    if db_engine:
        try:
            df_s = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in opts.keys():
                stored = df_s[df_s['type'] == key]['value'].tolist()
                opts[key] = sorted(list(set(opts[key] + stored)))
        except: pass
    return opts

def add_option(t, v):
    if db_engine and v:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t":t, "v":v})
            conn.commit()

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    rename_map = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=rename_map).loc[:, ~df.columns.duplicated()]
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    if 'Virksomhed' in df.columns:
        def site_gen(r):
            s, v = str(r.get('Website','')).strip(), str(r.get('Virksomhed','')).strip()
            if (s == "" or s == "nan") and v != "":
                c = re.sub(r'[^a-z0-9]', '', v.lower())
                return f"https://www.{c}.dk" if c else ""
            return s
        df['Website'] = df.apply(site_gen, axis=1)
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# --- DATA LOAD ---
if 'df' not in st.session_state:
    if db_engine:
        try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
        except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)
st.session_state.df = force_clean(st.session_state.df)
opts = load_options()

# --- 5. POP-UP KLIENT KORT ---
@st.dialog("📝 Klient Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    t1, t2 = st.tabs(["📊 Stamdata & Pipeline", "📓 Noter & Filer"])
    upd = {}
    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            upd['Dialog'] = st.selectbox("Dialog", opts['dialogs'], index=opts['dialogs'].index(row.get('Dialog')) if row.get('Dialog') in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr',''))
            def sd(v):
                try: return pd.to_datetime(v, dayfirst=True).date()
                except: return date.today()
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=sd(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=sd(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Info")
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(row.get('Aff. status')) if row.get('Aff. status') in opts['aff_status'] else 0)
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori',''))
            upd['MID'] = st.text_input("MID", value=row.get('MID',''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter',''))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC',''))
        st.divider()
        ca, cb, cc = st.columns(3)
        with ca: upd['Date Added'] = st.text_input("Dato tilføjet", value=row.get('Date Added',''))
        with cb: upd['Segment'] = st.text_input("Segment", value=row.get('Segment',''))
        with cc:
            upd['Network'] = st.selectbox("Netværk", opts['networks'], index=opts['networks'].index(row.get('Network')) if row.get('Network') in opts['networks'] else 0)
            upd['Land'] = st.selectbox("Land", opts['lands'], index=opts['lands'].index(row.get('Land')) if row.get('Land') in opts['lands'] else 0)
    
    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter", value=row.get('Noter',''), height=300)
        if row.get('Fil_Navn'):
            st.info(f"Fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Download fil</a>', unsafe_allow_html=True)
        up = st.file_uploader("Vedhæft fil", key=f"up_{idx}")
        if up:
            upd['Fil_Navn'], upd['Fil_Data'] = up.name, base64.b64encode(up.read()).decode()

    if st.button("💾 GEM ÆNDRINGER", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 6. SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Kontrol Center")
    with st.expander("👤 Admin Panel"):
        t_sel = st.selectbox("Type:", ["networks", "lands", "aff_status", "dialogs"])
        v_new = st.text_input("Tilføj valg:")
        if st.button("Tilføj") and v_new: add_option(t_sel, v_new); st.rerun()
        st.divider()
        nu, np = st.text_input("Ny Bruger:"), st.text_input("Ny Kode:", type="password")
        if st.button("Opret Bruger") and nu and np:
            with db_engine.connect() as conn: conn.execute(text("INSERT INTO users VALUES (:u,:p)"), {"u":nu,"p":np}); conn.commit()
            st.success("Bruger oprettet")
    
    st.divider()
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'sel_rows' in st.session_state and len(st.session_state.sel_rows) > 0:
        st.download_button("📥 Download VALGTE", st.session_state.df.iloc[st.session_state.sel_rows].to_csv(index=False), "udvalgte.csv", use_container_width=True, type="primary")

    st.divider()
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = force_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_db(st.session_state.df); st.rerun()
    
    if st.button("🚪 Log ud"):
        st.session_state.authenticated = False
        st.rerun()

# --- 7. MAIN CRM ---
st.title("💼 CRM Master Workspace")
search = st.text_input("🔍 Søg...")
df_v = st.session_state.df.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[[c for c in df_v.columns if c != 'Fil_Data']], column_config={"Website": st.column_config.LinkColumn("Website")}, use_container_width=True, selection_mode="multi-row", on_select="rerun", height=600)
st.session_state.sel_rows = sel.selection.rows

if len(st.session_state.sel_rows) == 1:
    real_idx = df_v.index[st.session_state.sel_rows[0]]
    if st.button(f"✏️ Åbn kort for {df_v.loc[real_idx, 'Virksomhed']}", type="primary"): client_popup(real_idx)
