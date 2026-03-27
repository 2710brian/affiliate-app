import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION (SKAL VÆRE ØVERST) ---
st.set_page_config(page_title="Marketing Group CRM", layout="wide", page_icon="💼")

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
                conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS settings (type TEXT, value TEXT)"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. LOGIN & BRANDING LOGIK ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def get_b64(file):
    if os.path.exists(file):
        with open(file, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return ""

if not st.session_state.authenticated:
    bg = get_b64("background.png")
    logo = get_b64("applogo.png")
    
    st.markdown(f"""
        <style>
        .stApp {{
            background: url("data:image/png;base64,{bg}") no-repeat center center fixed;
            background-size: cover;
        }}
        .login-card {{
            background-color: rgba(255, 255, 255, 0.9);
            padding: 40px;
            border-radius: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            text-align: center;
            width: 400px;
            margin: 15vh auto;
        }}
        .logo-corner {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 180px;
        }}
        </style>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        st.markdown("<div class='login-card'>", unsafe_allow_html=True)
        st.subheader("💼 CRM MASTER LOGIN")
        u = st.text_input("Brugernavn", key="u_login")
        p = st.text_input("Adgangskode", type="password", key="p_login")
        
        if st.button("START ARBEJDSDAG", use_container_width=True, type="primary"):
            r_u = os.getenv("APP_USER", "admin")
            r_p = os.getenv("APP_PASSWORD", "admin123")
            
            if u == r_u and p == r_p:
                st.session_state.authenticated = True
                st.rerun()
            elif db_engine:
                with db_engine.connect() as conn:
                    res = conn.execute(text("SELECT password FROM users WHERE username = :u"), {"u": u}).fetchone()
                    if res and res[0] == p:
                        st.session_state.authenticated = True
                        st.rerun()
                    else: st.error("Ugyldigt login")
        st.markdown("</div>", unsafe_allow_html=True)

    if logo:
        st.markdown(f'<img src="data:image/png;base64,{logo}" class="logo-corner">', unsafe_allow_html=True)
    st.stop()

# --- 4. CRM FUNKTIONALITET (KØRER KUN EFTER LOGIN) ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

def load_options():
    opts = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Awin", "Tradetracker"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK"],
        "aff_status": ["Godkendt", "Ikke ansøgt", "Afvist", "Afventer"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Kontaktet", "Vundet", "Tabt", "Call"]
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
    # Fjern dubletter i kolonner med det samme (Løser ValueError)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    rename = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=rename)
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

# Data
if 'df' not in st.session_state:
    try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
    except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)
st.session_state.df = force_clean(st.session_state.df)
opts = load_options()

# --- 5. POP-UP (KUN TO FANER SOM ØNSKET) ---
@st.dialog("📝 Klient Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    
    t1, t2 = st.tabs(["📊 Stamdata & Pipeline", "📓 Noter & Vedhæftninger"])
    upd = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']: upd[f] = st.text_input(f, value=row.get(f,''))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(row.get('Dialog')) if row.get('Dialog') in opts['dialogs'] else 0)
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
        with ca: upd['Date Added'] = st.date_input("Dato tilføjet", value=sd(row.get('Date Added'))).strftime('%d/%m/%Y')
        with cb: upd['Segment'] = st.text_input("Segment", value=row.get('Segment',''))
        with cc:
            upd['Network'] = st.selectbox("Netværk", opts['networks'], index=opts['networks'].index(row.get('Network')) if row.get('Network') in opts['networks'] else 0)
            upd['Land'] = st.selectbox("Land", opts['lands'], index=opts['lands'].index(row.get('Land')) if row.get('Land') in opts['lands'] else 0)
            upd['Trafik'] = st.text_input("Trafik", value=row.get('Trafik', ''))

    with t2:
        upd['Noter'] = st.text_area("📓 Klient Logbog", value=row.get('Noter',''), height=300)
        up = st.file_uploader("Vedhæft fil (PDF, Lyd, Billeder)", key=f"f_{idx}")
        if row.get('Fil_Navn'):
            st.info(f"📂 Fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Download fil</a>', unsafe_allow_html=True)
        if up:
            upd['Fil_Navn'], upd['Fil_Data'] = up.name, base64.b64encode(up.read()).decode()

    if st.button("💾 GEM ALT PÅ KLIENT", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 6. SIDEBAR ---
with st.sidebar:
    st.header("⚙️ CRM Kontrol")
    with st.expander("👤 Admin & Dropdowns"):
        t_sel = st.selectbox("Type:", ["networks", "lands", "aff_status"])
        v_new = st.text_input("Nyt valg:")
        if st.button("Tilføj") and v_new: add_option(t_sel, v_new); st.rerun()
        st.divider()
        nu, np = st.text_input("Ny Bruger:"), st.text_input("Ny Kode:", type="password")
        if st.button("Opret Bruger") and nu and np:
            with db_engine.connect() as conn: conn.execute(text("INSERT INTO users VALUES (:u,:p)"), {"u":nu,"p":np}); conn.commit()
            st.success("Oprettet")
    
    st.divider()
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'sel_idx' in st.session_state and len(st.session_state.sel_idx) > 0:
        st.download_button("📥 Download VALGTE", st.session_state.df.iloc[st.session_state.sel_idx].to_csv(index=False), "udvalgte.csv", use_container_width=True, type="primary")
    st.divider()
    kat_up = st.text_input("Kategori:", "Bolig")
    f_up = st.file_uploader("Flet fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = force_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_db(st.session_state.df); st.rerun()
    if st.button("🚪 Log ud"):
        st.session_state.authenticated = False; st.rerun()

# --- 7. WORKSPACE ---
st.title("💼 CRM Master Workspace")
search = st.text_input("🔍 Søg...")
df_v = st.session_state.df.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[[c for c in df_v.columns if c != 'Fil_Data']], column_config={"Website": st.column_config.LinkColumn("Website")}, use_container_width=True, selection_mode="multi-row", on_select="rerun", height=600)
st.session_state.sel_idx = sel.selection.rows

if len(st.session_state.sel_idx) == 1:
    real_idx = df_v.index[st.session_state.sel_idx[0]]
    if st.button(f"✏️ Åbn kort for {df_v.loc[real_idx, 'Virksomhed']}", type="primary"): client_popup(real_idx)
