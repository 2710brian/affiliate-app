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
            return create_engine(db_url, pool_pre_ping=True)
        except: return None
    return None

db_engine = get_engine()

# --- 3. MASTER STRUKTUR (26 KOLONNER) ---
# Jeg har sikret at 'Salgs % (sats)' er her!
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

DIALOG_OPTIONS = [
    "Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", 
    "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", 
    "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"
]

# --- 4. SIKKERHEDS-MOTOR ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']:
        return date.today()
    try:
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        return dt.date() if not pd.isna(dt) else date.today()
    except: return date.today()

def force_clean_data(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    
    # Map kolonne-navne fra dine filer
    rename_map = {
        'Merchant': 'Virksomhed', 'Programnavn': 'Virksomhed',
        'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 
        'DateAdded': 'Date Added', 'Status': 'Aff. status', 
        'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'
    }
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()] 
    
    # Rens alt til ren tekst
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')

    # WEBSITE GENERATOR
    def set_site(r):
        s = str(r.get('Website', '')).strip()
        v = str(r.get('Virksomhed', '')).strip()
        if (s == "" or s == "nan" or s == "None") and v != "":
            clean = re.sub(r'[^a-z0-9]', '', v.lower())
            return f"https://www.{clean}.dk" if clean else ""
        if s != "" and not s.startswith("http"):
            return f"https://{s}"
        return s
    df['Website'] = df.apply(set_site, axis=1)

    # LAND / FLAG SCANNER
    def set_land(r):
        t = " ".join([str(v) for v in r.values]).lower()
        if any(x in t for x in ["denmark", "dk", "dansk"]): return "🇩🇰"
        if any(x in t for x in ["sweden", "se", "svensk"]): return "🇸🇪"
        if any(x in t for x in ["norway", "no", "norsk"]): return "🇳🇴"
        return "🌐"
    df['Land'] = df.apply(set_land, axis=1)

    # Tving Master rækkefølge
    df = df.reindex(columns=MASTER_COLS, fill_value="")
    return df

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
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Gemme-fejl: {e}")
            return False
    return False

# --- 5. INITIALISERING ---
if 'df' not in st.session_state:
    st.session_state.df = load_db()

# --- 6. POP-UP (KOMPLET KLIENT KORT) ---
@st.dialog("📝 Klient Detaljer & Lead Kort", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["📊 CRM Stamdata & Pipeline", "📁 Noter & Info"])
    upd = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            upd['Fornavn'] = st.text_input("Fornavn", value=row.get('Fornavn'))
            upd['Efternavn'] = st.text_input("Efternavn", value=row.get('Efternavn'))
            upd['Mail'] = st.text_input("E-mail", value=row.get('Mail'))
            upd['Tlf'] = st.text_input("Tlf", value=row.get('Tlf'))
            upd['Website'] = st.text_input("Website (URL)", value=row.get('Website'))
        with c2:
            st.markdown("##### ⚙️ Pipeline (Status 1)")
            d_v = row.get('Dialog', 'Ikke kontakte')
            d_i = DIALOG_OPTIONS.index(d_v) if d_v in DIALOG_OPTIONS else 0
            upd['Dialog'] = st.selectbox("Dialog Status", DIALOG_OPTIONS, index=d_i)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr'))
            
            # SIKRE DATO VÆLGERE
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=get_safe_date(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=get_safe_date(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Info (Status 2)")
            upd['Aff. status'] = st.text_input("Aff. status", value=row.get('Aff. status'))
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori'))
            upd['MID'] = st.text_input("MID", value=row.get('MID'))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter'))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC'))

        st.divider()
        st.markdown("##### 📊 Øvrige Data")
        ca, cb, cc = st.columns(3)
        with ca:
            upd['Date Added'] = st.text_input("Date Added", value=row.get('Date Added'))
            upd['Programnavn'] = st.text_input("Programnavn (Original)", value=row.get('Programnavn'))
        with cb:
            upd['Segment'] = st.text_input("Segment", value=row.get('Segment'))
            upd['Salgs % (sats)'] = st.text_input("Salgs % (Vigtig)", value=row.get('Salgs % (sats)'))
            upd['Lead/Fast (sats)'] = st.text_input("Lead/Fast", value=row.get('Lead/Fast (sats)'))
        with cc:
            upd['Trafik'] = st.text_input("Trafik", value=row.get('Trafik'))
            upd['Network'] = st.text_input("Network", value=row.get('Network'))
            upd['Land'] = st.text_input("Land Ikon", value=row.get('Land'))

    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter", value=row.get('Noter'), height=250)
        st.file_uploader("Vedhæft fil til klient", key=f"f_{idx}")

    if st.button("💾 GEM KLIENT KORT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 7. SIDEBAR ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("📤 Eksport")
    if not st.session_state.df.empty:
        st.download_button("📥 Master CSV (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
        if 'filtered' in st.session_state:
            st.download_button("📥 Valgte CSV (Filter)", st.session_state.filtered.to_csv(index=False), "valgte.csv", use_container_width=True)

    st.divider()
    st.header("📊 Sortering")
    s_col = st.selectbox("Sortér efter:", MASTER_COLS[:15])
    s_asc = st.radio("Orden:", ["Nyeste/Højeste", "Laveste/Ældste"])
    if st.button("Sortér nu", use_container_width=True):
        st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_asc=="Laveste/Ældste"))
        save_db(st.session_state.df); st.rerun()

    st.divider()
    st.header("📥 Import")
    kat_up = st.text_input("Kategori ved upload:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Upload fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd['Kategori'] = kat_up
        st.session_state.df = force_clean_data(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_db(st.session_state.df); st.rerun()

    if st.button("🚨 NULSTIL DATABASE"):
        if db_engine:
            with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants")); conn.commit()
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    df_show = st.session_state.df.copy()
    if search:
        mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_show = df_show[mask]
    st.session_state.filtered = df_show

    st.write(f"Antal: **{len(df_show)}**")
    st.info("💡 Klik på boksen til venstre for en række for at åbne kortet.")

    # TABEL
    sel = st.dataframe(
        df_show,
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_show.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("👋 Upload data for at starte.")
