import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime, date
import base64

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide")

# ADGANGSKODE (LÅS)
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
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

# --- 4. DROPDOWN LOGIK ---
def load_options():
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
                defaults[key] = sorted(list(set(defaults[key] + stored)))
        except: pass
    return defaults

def add_dropdown_option(t, v):
    if db_engine and v:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t":t, "v":v})
            conn.commit()

# --- 5. RENSE- OG MERGE MOTOR ---
def robust_repair(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    ren = {'Merchant': 'Virksomhed', 'Programnavn': 'Programnavn', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Status': 'Aff. status', 'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'}
    df = df.rename(columns=ren)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.astype(str).replace(['NaT', 'nan', 'None', '00:00:00'], '')
    return df.reindex(columns=MASTER_COLS, fill_value="")

def save_to_db(df):
    if db_engine:
        df = robust_repair(df)
        df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df.loc[df['Virksomhed'] == '', 'MATCH_KEY'] = df['Programnavn'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
        df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
        df.to_sql('merchants', db_engine, if_exists='replace', index=False)
        return True
    return False

# Initialiser data
if 'df' not in st.session_state:
    try: st.session_state.df = pd.read_sql("SELECT * FROM merchants", db_engine)
    except: st.session_state.df = pd.DataFrame(columns=MASTER_COLS)

st.session_state.df = robust_repair(st.session_state.df)
opts = load_options()

# --- 6. DET STORE KLIENT KORT (POP-UP) ---
@st.dialog("📝 Klient Detaljer & CRM", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed') or row.get('Programnavn') or 'Ukendt'}")
    st.divider()
    
    t1, t2 = st.tabs(["📊 Stamdata & Pipeline", "📓 Noter & Vedhæftninger"])
    upd = {}

    # Hjælpefunktion til sikker dato
    def sd(v):
        try: return pd.to_datetime(v, dayfirst=True).date()
        except: return date.today()

    with t1:
        # SEKTION 1: KONTAKT, PIPELINE, INFO
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']:
                upd[f] = st.text_input(f, value=row.get(f, ''))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            d_val = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(d_val) if d_val in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr', ''))
            
            # DATOER MED KALENDER
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=sd(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=sd(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Info")
            a_val = row.get('Aff. status', 'Ikke ansøgt')
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(a_val) if a_val in opts['aff_status'] else 0)
            upd['Kategori'] = st.text_input("Hovedkategori", value=row.get('Kategori', ''))
            upd['MID'] = st.text_input("MID", value=row.get('MID', ''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter', ''))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC', ''))

        st.divider()
        # SEKTION 2: TEKNISKE SYSTEMDATA
        st.markdown("##### 📊 Tekniske Systemdata")
        ca, cb, cc = st.columns(3)
        with ca:
            # NU MED KALENDERMODUL:
            upd['Date Added'] = st.date_input("Dato tilføjet", value=sd(row.get('Date Added'))).strftime('%d/%m/%Y')
            upd['Programnavn'] = st.text_input("Programnavn (Original)", value=row.get('Programnavn', ''))
        with cb:
            upd['Segment'] = st.text_input("Segment", value=row.get('Segment', ''))
            upd['Salgs % (sats)'] = st.text_input("Salgs %", value=row.get('Salgs % (sats)', ''))
            upd['Lead/Fast (sats)'] = st.text_input("Lead/Fast", value=row.get('Lead/Fast (sats)', ''))
        with cc:
            upd['Trafik'] = st.text_input("Trafik", value=row.get('Trafik', ''))
            upd['Network'] = st.selectbox("Netværk", opts['networks'], index=opts['networks'].index(row.get('Network')) if row.get('Network') in opts['networks'] else 0)
            upd['Land'] = st.selectbox("Land Ikon", opts['lands'], index=opts['lands'].index(row.get('Land')) if row.get('Land') in opts['lands'] else 0)
            upd['Feed?'] = st.text_input("Feed?", value=row.get('Feed?', ''))

    with t2:
        st.markdown("##### 📓 Klient Noter")
        upd['Noter'] = st.text_area("Skriv logbog her...", value=row.get('Noter', ''), height=300)
        
        st.divider()
        st.markdown("##### 📎 Vedhæftede filer")
        if row.get('Fil_Navn'):
            st.info(f"📂 Aktuel fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                b64 = row['Fil_Data']
                href = f'<a href="data:application/octet-stream;base64,{b64}" download="{row["Fil_Navn"]}">👉 Download/Se nuværende fil</a>'
                st.markdown(href, unsafe_allow_html=True)
        
        up = st.file_uploader("Upload ny fil (Billede, PDF, Lyd, Video)", key=f"f_{idx}")
        if up:
            upd['Fil_Navn'] = up.name
            upd['Fil_Data'] = base64.b64encode(up.read()).decode()
            st.success("Fil klar til at blive gemt!")

    if st.button("💾 GEM ALLE ÆNDRINGER PÅ KLIENT", type="primary", use_container_width=True):
        for k,v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_to_db(st.session_state.df): st.rerun()

# --- 7. UI SIDEBAR ---
with st.sidebar:
    st.header("⚙️ CRM Kontrol")
    with st.expander("📊 Sortering"):
        s_col = st.selectbox("Sortér efter:", ["Date Added", "Produkter", "EPC", "Virksomhed", "Dialog"])
        s_asc = st.radio("Orden:", ["Nyeste/Højeste", "Ældste/Laveste"])
        if st.button("Udfør Sortering"):
            st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_asc=="Ældste/Laveste"))
            save_to_db(st.session_state.df); st.rerun()

    with st.expander("🛠️ Administrer Dropdowns"):
        t_sel = st.selectbox("Type:", ["aff_status", "networks", "lands", "dialogs"])
        v_new = st.text_input("Nyt valg:")
        if st.button("Tilføj nu") and v_new: add_dropdown_option(t_sel, v_new); st.rerun()

    st.divider()
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    
    st.divider()
    kat_up = st.text_input("Kategori ved upload:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Flet ny fil")
    if f_up and st.button("Flet & Gem"):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd = nd.rename(columns={'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 'Aff. Status': 'Aff. status'})
        nd['Kategori'] = kat_up
        st.session_state.df = robust_repair(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_to_db(st.session_state.df); st.rerun()

    if st.button("🚪 Lås CRM"):
        st.session_state.auth = False; st.rerun()

# --- 8. HOVEDVISNING ---
st.title("💼 CRM Master Workspace")
search = st.text_input("🔍 Søg i CRM...", "")
df_v = st.session_state.df.copy()
if search:
    df_v = df_v[df_v.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

view_cols = [c for c in df_v.columns if c != 'Fil_Data']
sel = st.dataframe(
    df_v[view_cols], 
    use_container_width=True, 
    selection_mode="single-row", 
    on_select="rerun", 
    height=600,
    column_config={"Website": st.column_config.LinkColumn("Website")}
)

if sel.selection.rows:
    real_idx = df_v.index[sel.selection.rows[0]]
    client_popup(real_idx)
