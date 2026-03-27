import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION & SESSION (SKAL KØRE FØRST) ---
st.set_page_config(page_title="Marketing Group Malaga CRM", layout="wide", page_icon="💼")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# Funktion til lynhurtig billedhåndtering
@st.cache_data
def get_base64_image(file_path):
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return None

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
                res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
                if res[0] == 0:
                    u, p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "admin123")
                    conn.execute(text("INSERT INTO users (username, password) VALUES (:u, :p)"), {"u": u, "p": p})
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. BRANDING & LOGIN INTERFACE ---
if not st.session_state.authenticated:
    bg_img = get_base64_image("background.png")
    logo_img = get_base64_image("applogo.png")

    st.markdown(f"""
        <style>
        /* Fjerner Streamlit standard hvide elementer */
        header {{visibility: hidden;}}
        .main .block-container {{padding: 0;}}
        
        .stApp {{
            background: url("data:image/png;base64,{bg_img}") no-repeat center center fixed;
            background-size: cover;
        }}
        
        .login-card {{
            background-color: rgba(255, 255, 255, 0.85);
            padding: 40px;
            border-radius: 20px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
            width: 400px;
            margin: 15vh auto;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.4);
        }}
        
        .fixed-logo {{
            position: fixed;
            bottom: 30px;
            right: 30px;
            width: 180px;
            z-index: 9999;
        }}
        </style>
        
        <div class="login-card">
            <h2 style="color: #1e3a8a; margin-bottom: 5px;">CRM MASTER</h2>
            <p style="color: #64748b; font-size: 14px;">Marketing Group Malaga - Official Workspace</p>
        </div>
        
        {"<img src='data:image/png;base64," + logo_img + "' class='fixed-logo'>" if logo_img else ""}
    """, unsafe_allow_html=True)

    # Login input (Placeret præcis i midten)
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        st.markdown("<br>", unsafe_allow_html=True)
        u_in = st.text_input("Brugernavn", placeholder="Brugernavn", label_visibility="collapsed")
        p_in = st.text_input("Adgangskode", type="password", placeholder="Adgangskode", label_visibility="collapsed")
        
        if st.button("START ARBEJDSDAG", use_container_width=True, type="primary"):
            r_u, r_p = os.getenv("APP_USER", "admin"), os.getenv("APP_PASSWORD", "admin123")
            if u_in == r_u and p_in == r_p:
                st.session_state.authenticated = True
                st.rerun()
            elif db_engine:
                with db_engine.connect() as conn:
                    res = conn.execute(text("SELECT password FROM users WHERE username = :u"), {"u": u_in}).fetchone()
                    if res and res[0] == p_in:
                        st.session_state.authenticated = True
                        st.rerun()
                    else: st.error("Ugyldig adgangskode")
    st.stop()

# --- 4. CRM LOGIK (KØRER KUN EFTER LOGIN) ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

def load_options():
    defaults = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Tradetracker", "Awin", "GJ", "Daisycon", "Shopnello", "TradeDoubler", "Webigans"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "aff_status": ["Godkendt", "Ikke ansøgt", "Afvist", "Pause"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"]
    }
    if db_engine:
        try:
            df_s = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in defaults.keys():
                stored = df_s[df_s['type'] == key]['value'].tolist()
                defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    rename_map = {'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=rename_map).loc[:, ~df.columns.duplicated()]
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    if 'Virksomhed' in df.columns:
        df['Website'] = df.apply(lambda r: f"https://www.{re.sub(r'[^a-z0-9]', '', str(r['Virksomhed']).lower())}.dk" if str(r.get('Website','')) == '' else r['Website'], axis=1)
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# Data initialisering
if 'df' not in st.session_state:
    try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
    except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)
st.session_state.df = force_clean(st.session_state.df)
opts = load_options()

# --- 5. POP-UP (CRM BOARD) ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
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
            d_v = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog", opts['dialogs'], index=opts['dialogs'].index(d_v) if d_v in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr',''))
            def sd(v):
                try: return pd.to_datetime(v, dayfirst=True).date()
                except: return date.today()
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=sd(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=sd(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Aff. Info")
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(row.get('Aff. status')) if row.get('Aff. status') in opts['aff_status'] else 0)
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori',''))
            upd['MID'] = st.text_input("MID", value=row.get('MID',''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter',''))
        st.divider()
        st.markdown("##### 📊 Tekniske data")
        ca, cb, cc = st.columns(3)
        for i, f in enumerate(['Date Added', 'EPC', 'Segment', 'Salgs % (sats)', 'Trafik', 'Network', 'Land', 'Feed?']):
            target = [ca, cb, cc][i % 3]
            upd[f] = target.text_input(f, value=row.get(f,''))
    with t2:
        upd['Noter'] = st.text_area("📓 Klient Noter", value=row.get('Noter',''), height=300)
        up = st.file_uploader("Upload fil til klient", key=f"file_{idx}")
        if up: upd['Fil_Navn'], upd['Fil_Data'] = up.name, base64.b64encode(up.read()).decode()

    if st.button("💾 GEM ALT PÅ KLIENT", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 6. WORKSPACE UI ---
with st.sidebar:
    st.header("⚙️ CRM Kontrol")
    with st.expander("👤 Admin Panel"):
        t_sel = st.selectbox("Dropdown type:", ["networks", "lands", "aff_status"])
        v_new = st.text_input("Nyt valg:")
        if st.button("Tilføj") and v_new:
            with db_engine.connect() as conn:
                conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t":t_sel, "v":v_new})
                conn.commit()
            st.rerun()
        st.divider()
        nu, np = st.text_input("Ny Bruger:"), st.text_input("Ny Kode:", type="password")
        if st.button("Opret Bruger") and nu and np:
            with db_engine.connect() as conn: conn.execute(text("INSERT INTO users VALUES (:u,:p)"), {"u":nu,"p":np}); conn.commit()
            st.success("Oprettet")
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
    if st.button("🚪 Log ud"): st.session_state.authenticated = False; st.rerun()

st.title("💼 CRM Master Workspace")
search = st.text_input("🔍 Søg i alt data...")
df_v = st.session_state.df.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[[c for c in df_v.columns if c != 'Fil_Data']], column_config={"Website": st.column_config.LinkColumn("Website")}, use_container_width=True, selection_mode="multi-row", on_select="rerun", height=600)
st.session_state.sel_rows = sel.selection.rows

if len(st.session_state.sel_rows) == 1:
    real_idx = df_v.index[st.session_state.sel_rows[0]]
    if st.button(f"✏️ Åbn kort for {df_v.loc[real_idx, 'Virksomhed']}", type="primary", use_container_width=True):
        client_popup(real_idx)
    if st.button(f"✏️ Åbn kort for {df_v.loc[real_idx, 'Virksomhed']}", type="primary"): client_popup(real_idx)
