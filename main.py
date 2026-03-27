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

# --- 2. DATABASE MOTOR (SIKKER OPSTART) ---
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                # Opret kun tabeller hvis de MANGLER. Vi rører IKKE merchants her.
                conn.execute(text("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)"))
                conn.execute(text("CREATE TABLE IF NOT EXISTS settings (type TEXT, value TEXT)"))
                # Opret admin hvis ingen brugere findes
                res = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
                if res[0] == 0:
                    u = os.getenv("APP_USER", "admin")
                    p = os.getenv("APP_PASSWORD", "admin123")
                    conn.execute(text("INSERT INTO users (username, password) VALUES (:u, :p)"), {"u": u, "p": p})
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. LOGIN & DESIGN LOGIK ---
def login_screen():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if st.session_state.authenticated:
        return True

    # Flot forside design
    st.markdown("""
        <style>
        .login-card {
            background-color: white;
            padding: 40px;
            border-radius: 20px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.1);
            text-align: center;
        }
        .main { background-color: #f0f2f6; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
            <div class='login-card'>
                <h1>💼 Affiliate CRM Master</h1>
                <p>Professionel Lead-håndtering & Database</p>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        
        with st.form("login_form"):
            user = st.text_input("Brugernavn")
            pw = st.text_input("Adgangskode", type="password")
            if st.form_submit_button("Log ind i systemet", use_container_width=True):
                with db_engine.connect() as conn:
                    res = conn.execute(text("SELECT password FROM users WHERE username = :u"), {"u": user}).fetchone()
                    if res and res[0] == pw:
                        st.session_state.authenticated = True
                        st.session_state.current_user = user
                        st.rerun()
                    else:
                        st.error("Forkert brugernavn eller kode")
    return False

# --- 4. DROPDOWN ADMINISTRATION (FIXET: TILFØJER I STEDET FOR AT SLETTE) ---
def load_options():
    # Dine faste grund-indstillinger
    base_opts = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Tradetracker", "Awin", "GJ", "Daisycon", "Shopnello", "TradeDoubler", "Webigans"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "aff_status": ["Godkendt", "Ikke ansøgt", "Afvist", "Afventer", "Pause"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"]
    }
    
    if db_engine:
        try:
            df_s = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in base_opts.keys():
                stored = df_s[df_s['type'] == key]['value'].tolist()
                # Vi lægger gemte ting oveni standarden og fjerner dubletter
                base_opts[key] = sorted(list(set(base_opts[key] + stored)))
        except: pass
    return base_opts

# --- 5. CRM KERNE FUNKTIONER ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    rename = {'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=rename).loc[:, ~df.columns.duplicated()]
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_db(df):
    if db_engine:
        df = force_clean(df)
        df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# --- 6. KLIENT KORT POP-UP ---
@st.dialog("📝 Klient-kort", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()
    
    t1, t2 = st.tabs(["📊 Stamdata & Pipeline", "📓 Noter & Vedhæftninger"])
    upd = {}
    opts = load_options()

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
            upd['Network'] = st.selectbox("Network", opts['networks'], index=opts['networks'].index(row.get('Network')) if row.get('Network') in opts['networks'] else 0)
            upd['Land'] = st.selectbox("Land", opts['lands'], index=opts['lands'].index(row.get('Land')) if row.get('Land') in opts['lands'] else 0)
        
        st.divider()
        st.markdown("##### 📊 Tekniske data")
        ca, cb, cc = st.columns(3)
        for i, f in enumerate(['Date Added', 'MID', 'Produkter', 'EPC', 'Segment', 'Salgs % (sats)', 'Trafik', 'Feed?']):
            target = [ca, cb, cc][i % 3]
            upd[f] = target.text_input(f, value=row.get(f,''))

    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter", value=row.get('Noter',''), height=300)
        st.file_uploader("Vedhæft fil (Gemmes manuelt)", key=f"file_{idx}")

    if st.button("💾 GEM KLIENT KORT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 7. KØR APP ---
if login_screen():
    if 'df' not in st.session_state:
        st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine) if db_engine else pd.DataFrame()
    
    st.session_state.df = force_clean(st.session_state.df)

    # SIDEBAR
    with st.sidebar:
        st.header("⚙️ CRM Indstillinger")
        
        # BRUGER & DROPDOWN PANEL
        with st.expander("👤 Admin Panel"):
            st.subheader("Tilføj Dropdowns")
            t_sel = st.selectbox("Type:", ["networks", "lands", "aff_status", "dialogs"])
            v_new = st.text_input("Nyt navn:")
            if st.button("Tilføj valg") and v_new:
                with db_engine.connect() as conn:
                    conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t":t_sel, "v":v_new})
                    conn.commit()
                st.rerun()
            
            st.divider()
            st.subheader("Opret Bruger")
            nu, np = st.text_input("Nyt Brugernavn:"), st.text_input("Ny Kode:", type="password")
            if st.button("Opret Bruger") and nu and np:
                with db_engine.connect() as conn:
                    conn.execute(text("INSERT INTO users VALUES (:u, :p)"), {"u":nu, "p":np})
                    conn.commit()
                st.success("Bruger oprettet!")

        st.divider()
        st.subheader("📤 Eksport")
        st.download_button("📥 Master (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
        
        if 'sel_idx' in st.session_state and len(st.session_state.sel_idx) > 0:
            sel_df = st.session_state.df.iloc[st.session_state.sel_idx]
            st.download_button("📥 Download VALGTE", sel_df.to_csv(index=False), "udvalgte.csv", use_container_width=True, type="primary")

        st.divider()
        st.header("📥 Import")
        kat_in = st.text_input("Kategori:", "Bolig")
        f_in = st.file_uploader("Vælg fil")
        if f_in and st.button("Flet Data"):
            nd = pd.read_csv(f_in) if f_in.name.endswith('csv') else pd.read_excel(f_in)
            nd = force_clean(nd)
            nd['Kategori'] = kat_in
            st.session_state.df = force_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
            save_db(st.session_state.df); st.rerun()

        if st.button("🚪 Log ud"):
            st.session_state.authenticated = False
            st.rerun()

    # MAIN WORKSPACE
    st.title("💼 Affiliate CRM Master")
    search = st.text_input("🔍 Søg i alt data...", "")
    
    df_v = st.session_state.df.copy()
    if search:
        mask = df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_v = df_v[mask]

    # TABEL MED FLUEBEN
    sel_box = st.dataframe(
        df_v,
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True,
        selection_mode="multi-row",
        on_select="rerun",
        height=600
    )

    st.session_state.sel_idx = sel_box.selection.rows

    if len(st.session_state.sel_idx) == 1:
        real_idx = df_v.index[st.session_state.sel_idx[0]]
        if st.button(f"✏️ Åbn Klient-kort for {df_v.loc[real_idx, 'Virksomhed']}", type="primary"):
            client_popup(real_idx)
    elif len(st.session_state.sel_idx) > 1:
        st.info(f"💡 {len(st.session_state.sel_idx)} rækker valgt. Brug 'Download VALGTE' i menuen.")
