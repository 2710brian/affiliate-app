import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION (SKAL VÆRE ØVERST) ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide", page_icon="💼")

# Hjælpefunktion til at læse billeder
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
            return create_engine(db_url, pool_pre_ping=True)
        except: return None
    return None

db_engine = get_engine()

# --- 3. LOGIN & FORSIDE DESIGN (Uden hvide bokse) ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    bg_b64 = get_base64("background.png")
    logo_b64 = get_base64("applogo.png")
    
    # CSS der fjerner ALT standard hvidt og placerer logo
    st.markdown(f"""
        <style>
        /* Skjul Streamlit header og ryd op */
        header {{visibility: hidden;}}
        .main .block-container {{padding: 0;}}
        
        .stApp {{
            background: url("data:image/png;base64,{bg_b64 if bg_b64 else ''}") no-repeat center center fixed;
            background-size: cover;
        }}
        
        /* Selve Login-kortet */
        .login-card {{
            background-color: rgba(255, 255, 255, 0.8);
            padding: 40px;
            border-radius: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            text-align: center;
            width: 400px;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
        }}
        
        /* Logo i hjørnet */
        .corner-logo {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 180px;
            z-index: 1000;
        }}
        
        /* Fjern baggrund på input felter for at matche kortet */
        .stTextInput input {{
            background-color: white !important;
        }}
        </style>
        
        <div class="login-card">
            <h2 style="color: #1e3a8a; margin-bottom: 0;">CRM MASTER</h2>
            <p style="color: #64748b; margin-top: 5px;">Marketing Group Malaga</p>
        </div>
        
        {"<img src='data:image/png;base64," + logo_b64 + "' class='corner-logo'>" if logo_b64 else ""}
    """, unsafe_allow_html=True)

    # Login-input felter placeret præcis i midten
    # Vi bruger 'empty' containere til at skabe plads omkring login-boksen
    empty_top, col_mid, empty_bot = st.columns([1, 1, 1])
    with col_mid:
        st.markdown("<br><br><br><br><br><br><br><br><br><br><br>", unsafe_allow_html=True)
        u_user = st.text_input("Brugernavn", key="login_u", label_visibility="collapsed", placeholder="Brugernavn")
        p_pass = st.text_input("Adgangskode", type="password", key="login_p", label_visibility="collapsed", placeholder="Adgangskode")
        
        if st.button("Start Arbejdsdag", use_container_width=True, type="primary"):
            rail_u = os.getenv("APP_USER", "admin")
            rail_p = os.getenv("APP_PASSWORD", "admin123")
            
            if u_user == rail_u and p_pass == rail_p:
                st.session_state.authenticated = True
                st.rerun()
            else:
                # Tjek databasen for andre brugere
                if db_engine:
                    with db_engine.connect() as conn:
                        res = conn.execute(text("SELECT password FROM users WHERE username = :u"), {"u": u_user}).fetchone()
                        if res and res[0] == p_pass:
                            st.session_state.authenticated = True
                            st.rerun()
                        else:
                            st.error("Ugyldig adgangskode")
    st.stop()

# --- 4. CRM MASTER (KUN SYNLIG VED LOGIN) ---

MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

# (Herunder genbruger vi din stabile CRM motor)
def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    ren = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=ren).loc[:, ~df.columns.duplicated()]
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def load_options():
    opts = {"networks": ["Partner-ads", "Awin"], "lands": ["DK", "SE", "NO"], "aff_status": ["Godkendt", "Ikke ansøgt"], "dialogs": ["Ikke kontakte", "Kontaktet"]}
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in opts.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: opts[key] = sorted(list(set(opts[key] + stored)))
        except: pass
    return opts

if 'df' not in st.session_state:
    if db_engine:
        try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
        except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)
    else: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)

st.session_state.df = force_clean(st.session_state.df)
opts = load_options()

# --- 5. POP-UP KORT ---
@st.dialog("📝 Klient Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    t1, t2 = st.tabs(["📊 CRM Data", "📓 Noter & Filer"])
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
    with t2:
        upd['Noter'] = st.text_area("📓 Klient Logbog", value=row.get('Noter',''), height=300)
    
    if st.button("💾 GEM"):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if db_engine:
            st.session_state.df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        st.rerun()

# --- 6. WORKSPACE UI ---
with st.sidebar:
    st.header("⚙️ CRM Menu")
    if st.button("🚪 Log ud"):
        st.session_state.authenticated = False
        st.rerun()
    st.divider()
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv")
    st.divider()
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = force_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        if db_engine: st.session_state.df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        st.rerun()

st.title("💼 CRM Master Workspace")
search = st.text_input("🔍 Søg...")
df_v = st.session_state.df.copy()
if search: df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(df_v[[c for c in df_v.columns if c != 'Fil_Data']], column_config={"Website": st.column_config.LinkColumn("Website")}, use_container_width=True, selection_mode="single-row", on_select="rerun", height=600)
if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    client_popup(real_idx)
