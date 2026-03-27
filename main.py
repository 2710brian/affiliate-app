import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="CRM Master Workspace", layout="wide")

# --- 2. ENKEL ADGANGSKONTROL (INGEN KOMPLICERET LOGIN) ---
# Vi bruger en simpel indtastning i toppen. Er koden forkert, vises intet.
access_code = st.text_input("Indtast CRM Adgangskode for at åbne databasen", type="password")

# Hent koden fra Railway (standard er 'mgm2024')
correct_code = os.getenv("APP_PASSWORD", "mgm2024")

if access_code != correct_code:
    st.warning("Indtast den korrekte adgangskode for at se indholdet.")
    if access_code != "":
        st.error("Forkert kode.")
    st.stop() # Stopper alt her - intet bliver indlæst eller vist

# --- HERFRA STARTER ALT DIT CRM ARBEJDE - KØRER KUN HVIS KODEN ER RIGTIG ---

# --- 3. DATABASE MOTOR ---
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

# --- 4. MASTER DEFINITIONER (1-26) ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

# --- 5. DROPDOWN ADMINISTRATION ---
def load_options():
    opts = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Tradetracker", "Awin", "GJ", "Daisycon", "Shopnello", "TradeDoubler", "Webigans"],
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

# --- 6. DATA RENS & GEM ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    # Fjern dubletter i kolonner med det samme
    df = df.loc[:, ~df.columns.duplicated()].copy()
    rename_map = {'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=rename_map)
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

# Indlæs data til CRM
if 'df' not in st.session_state:
    try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
    except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)

st.session_state.df = force_clean(st.session_state.df)
opts = load_options()

# --- 7. DET STORE KLIENT KORT (POP-UP) ---
@st.dialog("📝 Klient-kort / Detaljer", width="large")
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
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']:
                upd[f] = st.text_input(f, value=row.get(f, ''))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            d_v = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(d_v) if d_v in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr', ''))
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=get_safe_date(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=get_safe_date(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Aff. Info")
            a_v = row.get('Aff. status', 'Ikke ansøgt')
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(a_v) if a_v in opts['aff_status'] else 0)
            upd['Kategori'] = st.text_input("Hovedkategori", value=row.get('Kategori', ''))
            upd['MID'] = st.text_input("MID", value=row.get('MID', ''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter', ''))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC', ''))
        
        st.divider()
        ca, cb, cc = st.columns(3)
        for i, f in enumerate(['Date Added', 'Segment', 'Salgs % (sats)', 'Lead/Fast (sats)', 'Trafik', 'Network', 'Land', 'Feed?']):
            target = [ca, cb, cc][i % 3]
            upd[f] = target.text_input(f, value=row.get(f, ''))

    with t2:
        upd['Noter'] = st.text_area("📓 Klient Logbog", value=row.get('Noter', ''), height=300)
        st.divider()
        if row.get('Fil_Navn'):
            st.info(f"📂 Fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Hent fil</a>', unsafe_allow_html=True)
        up = st.file_uploader("Vedhæft fil", key=f"f_{idx}")
        if up:
            upd['Fil_Navn'] = up.name
            upd['Fil_Data'] = base64.b64encode(up.read()).decode()

    if st.button("💾 GEM ALT PÅ KLIENT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 8. UI SIDEBAR ---
with st.sidebar:
    st.title("⚙️ CRM Master")
    
    with st.expander("🛠️ Administrer Dropdowns"):
        t_sel = st.selectbox("Type:", ["networks", "lands", "aff_status", "dialogs"])
        v_new = st.text_input("Nyt valg:")
        if st.button("Tilføj valg") and v_new:
            add_option(t_sel, v_new); st.rerun()

    st.divider()
    st.subheader("📤 Eksport")
    st.download_button("📥 Master (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'sel_idx' in st.session_state and len(st.session_state.sel_idx) > 0:
        st.download_button("📥 Download VALGTE", st.session_state.df.iloc[st.session_state.sel_idx].to_csv(index=False), "udvalgte.csv", use_container_width=True, type="primary")

    st.divider()
    st.subheader("📥 Import")
    kat_up = st.text_input("Kategori:", "Bolig")
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        st.session_state.df = force_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_db(st.session_state.df); st.rerun()

    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn:
                for t in ['merchants', 'settings']: conn.execute(text(f"DROP TABLE IF EXISTS {t}"))
                conn.commit()
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 9. MAIN CRM ---
st.title("💼 CRM Workspace")
search = st.text_input("🔍 Søg i CRM...", "")
df_v = st.session_state.df.copy()
if search:
    df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

sel = st.dataframe(
    df_v[[c for c in df_v.columns if c != 'Fil_Data']], 
    column_config={"Website": st.column_config.LinkColumn("Website")}, 
    use_container_width=True, selection_mode="multi-row", on_select="rerun", height=600
)
st.session_state.sel_idx = sel.selection.rows

if len(st.session_state.sel_idx) == 1:
    real_idx = df_v.index[st.session_state.sel_idx[0]]
    if st.button(f"✏️ Åbn kort for {df_v.loc[real_idx, 'Virksomhed']}", type="primary"):
        client_popup(real_idx)
