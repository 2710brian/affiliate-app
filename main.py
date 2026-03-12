import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide")

# --- DATABASE FORBINDELSE ---
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            return create_engine(db_url, pool_pre_ping=True)
        except Exception: return None
    return None

db_engine = get_engine()

# --- DEFINER MASTER KOLONNER (1-25) ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Status', 'Dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn'
]

DIALOG_OPTIONS = [
    "Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", 
    "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", 
    "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"
]

# --- SIKKER DATO-RENS ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']:
        return date.today()
    try:
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        return dt.date() if not pd.isna(dt) else date.today()
    except: return date.today()

# --- DEN HÅRDE RENSE-MOTOR ---
def god_mode_clean(df):
    if df.empty: return df
    
    # 1. Omdøb kendte felter
    rename_map = {'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC'}
    df = df.rename(columns=rename_map)
    
    # 2. FJERN ALLE DUBLETT-KOLONNER (Tvinger unikke navne)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 3. Rens tekniske fejl-ord
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')
    
    # 4. Genskab Website hvis den mangler (Auto-Link)
    if 'Virksomhed' in df.columns:
        def make_site(row):
            site = str(row.get('Website', ''))
            if site == "" or site == "None":
                name = str(row.get('Virksomhed', '')).lower().strip().replace(' ', '')
                name = re.sub(r'[^a-z0-9]', '', name)
                return f"https://www.{name}.dk" if name else ""
            return site
        df['Website'] = df.apply(make_site, axis=1)

    # 5. Tving rækkefølge og fjern alt skrald
    for c in MASTER_COLS:
        if c not in df.columns: df[c] = ""
            
    return df[MASTER_COLS]

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return god_mode_clean(df)
        except: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = god_mode_clean(df)
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Save Error: {e}")
            return False
    return False

# --- SESSION START ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- KLIENT-KORT POPUP ---
@st.dialog("📋 CRM Klient-kort", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["📊 CRM Data", "📁 Dokumenter & Noter"])
    updated = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            updated['Fornavn'] = st.text_input("Fornavn", value=row.get('Fornavn'))
            updated['Efternavn'] = st.text_input("Efternavn", value=row.get('Efternavn'))
            updated['Mail'] = st.text_input("E-mail", value=row.get('Mail'))
            updated['Tlf'] = st.text_input("Tlf", value=row.get('Tlf'))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            d_val = row.get('Dialog', 'Ikke kontakte')
            d_idx = DIALOG_OPTIONS.index(d_val) if d_val in DIALOG_OPTIONS else 0
            updated['Dialog'] = st.selectbox("Status", DIALOG_OPTIONS, index=d_idx)
            updated['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr'))
            
            d_pick = get_safe_date(row.get('Opflg. dato'))
            updated['Opflg. dato'] = st.date_input("Opfølgning", value=d_pick).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Info")
            updated['Website'] = st.text_input("Website", value=row.get('Website'))
            updated['Kategori'] = st.text_input("Kategori", value=row.get('Kategori'))
            updated['Status'] = st.text_input("Status", value=row.get('Status'))

    with t2:
        st.markdown("##### 📓 Interne Noter")
        updated['Noter'] = st.text_area("Info om kunden", value=row.get('Noter'), height=250)
        
        st.divider()
        st.file_uploader("Upload fil til klient (PDF, Lyd, Billede)", key=f"file_{idx}")

    if st.button("💾 GEM KLIENT", type="primary", use_container_width=True):
        for k, v in updated.items(): st.session_state.df.at[idx, k] = v
        if save_data(st.session_state.df): st.rerun()

# --- SIDEBAR ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("📤 Eksport")
    if not st.session_state.df.empty:
        st.download_button("📥 Master CSV (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
        if 'filtered' in st.session_state:
            st.download_button("📥 Valgte CSV (Filter)", st.session_state.filtered.to_csv(index=False), "udvalgte.csv", use_container_width=True)

    st.divider()
    st.header("📊 Sortering")
    s_col = st.selectbox("Sortér efter:", ["Date Added", "Produkter", "EPC", "Virksomhed", "Dialog"])
    s_asc = st.radio("Orden:", ["Højeste/Nyeste", "Laveste/Ældste"])
    if st.button("Udfør Sortering", use_container_width=True):
        st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_asc=="Laveste/Ældste"))
        save_data(st.session_state.df); st.rerun()

    st.divider()
    st.header("📥 Import")
    kat_up = st.text_input("Vælg kategori:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Vælg fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd['Kategori'] = kat_up
        st.session_state.df = god_mode_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_data(st.session_state.df); st.rerun()

    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants")); conn.commit()
        st.session_state.df = pd.DataFrame(); st.rerun()

# --- CRM TABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    df_show = st.session_state.df.copy()
    if search:
        mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_show = df_show[mask]
    st.session_state.filtered = df_show

    st.write(f"Antal: {len(df_show)}")
    st.info("💡 Klik på boksen helt til venstre for at åbne kortet.")

    # TABEL (Tvinger unikke kolonner før visning så det aldrig crasher)
    view_df = df_show.loc[:, ~df_show.columns.duplicated()]
    
    sel = st.dataframe(
        view_df,
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_show.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("👋 Databasen er tom. Upload en fil.")
