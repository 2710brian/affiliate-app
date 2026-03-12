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

# --- 3. DEFINER MASTER STRUKTUR (26 KOLONNER) ---
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

# --- 4. RENSE- OG MAPPING-MOTOR ---
def detect_land_icon(row):
    # Scan hele rækken efter DK-tegn
    search_text = " ".join([str(val) for val in row.values]).lower()
    if any(x in search_text for x in ["denmark", "dk", "dansk"]): return "🇩🇰"
    if any(x in search_text for x in ["sweden", "se", "svensk", "sverige"]): return "🇸🇪"
    if any(x in search_text for x in ["norway", "no", "norsk", "norge"]): return "🇳🇴"
    return "🌐"

def force_master_structure(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    
    # Map gamle navne til de nye
    rename_map = {
        'Merchant': 'Virksomhed', 
        'Product Count': 'Produkter', 
        'EPC (nøgletal)': 'EPC',
        'DateAdded': 'Date Added',
        'Status': 'Aff. status',
        'Dato': 'Kontakt dato'
    }
    df = df.rename(columns=rename_map)
    
    # Fjern dubletter i kolonnenavne
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Rens teknisk skrald og tving alt til tekst
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')
    
    # Sæt flag/land hvis det mangler
    df['Land'] = df.apply(detect_land_icon, axis=1)

    # Automatisk Website-link hvis det mangler
    if 'Virksomhed' in df.columns:
        def site_gen(r):
            s = str(r.get('Website', ''))
            if s == "" or s == "None":
                clean = re.sub(r'[^a-z0-9]', '', str(r.get('Virksomhed', '')).lower())
                return f"https://www.{clean}.dk" if clean else ""
            return s
        df['Website'] = df.apply(site_gen, axis=1)

    # Reindex: Slet ukendte kolonner og tilføj dem der mangler fra MASTER_COLS
    df = df.reindex(columns=MASTER_COLS, fill_value="")
    
    return df

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return force_master_structure(df)
        except: return pd.DataFrame(columns=MASTER_COLS)
    return pd.DataFrame(columns=MASTER_COLS)

def save_data(df):
    if db_engine:
        try:
            df = force_master_structure(df)
            # Unik nøgle til databasen
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Databasefejl: {e}")
            return False
    return False

# --- 5. INITIALISERING ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- 6. POP-UP (KORREKT MAPPING AF ALLE FELTER) ---
@st.dialog("📋 Klient Detaljer & CRM", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["🏠 Stamdata & Dialog", "📁 Dokumenter & Noter"])
    upd = {}

    with t1:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("##### 📞 Kontakt")
            upd['Fornavn'] = st.text_input("Fornavn", value=row.get('Fornavn'))
            upd['Efternavn'] = st.text_input("Efternavn", value=row.get('Efternavn'))
            upd['Mail'] = st.text_input("E-mail", value=row.get('Mail'))
            upd['Tlf'] = st.text_input("Tlf", value=row.get('Tlf'))
            upd['Website'] = st.text_input("Website", value=row.get('Website'))
        
        with col2:
            st.markdown("##### ⚙️ Pipeline (Status 1)")
            # Dialog dropdown
            d_val = row.get('Dialog', 'Ikke kontakte')
            d_idx = DIALOG_OPTIONS.index(d_val) if d_val in DIALOG_OPTIONS else 0
            upd['Dialog'] = st.selectbox("Dialog Status", DIALOG_OPTIONS, index=d_idx)
            
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr'))
            
            # Opflg Dato vælger
            try: d_o = pd.to_datetime(row['Opflg. dato'], dayfirst=True).date()
            except: d_o = date.today()
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=d_o).strftime('%d/%m/%Y')
            
            # Kontakt Dato vælger
            try: d_k = pd.to_datetime(row['Kontakt dato'], dayfirst=True).date()
            except: d_k = date.today()
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=d_k).strftime('%d/%m/%Y')

        with col3:
            st.markdown("##### 📈 Info (Status 2)")
            upd['Aff. status'] = st.text_input("Aff. status", value=row.get('Aff. status'))
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori'))
            upd['MID'] = st.text_input("MID", value=row.get('MID'))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter'))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC'))

        st.divider()
        st.markdown("##### 📊 Øvrig Data")
        c_a, c_b, c_c = st.columns(3)
        with c_a:
            upd['Segment'] = st.text_input("Segment", value=row.get('Segment'))
            upd['Salgs % (sats)'] = st.text_input("Salgs %", value=row.get('Salgs % (sats)'))
        with c_b:
            upd['Lead/Fast (sats)'] = st.text_input("Lead/Fast", value=row.get('Lead/Fast (sats)'))
            upd['Trafik'] = st.text_input("Trafik", value=row.get('Trafik'))
        with c_c:
            upd['Feed?'] = st.text_input("Feed?", value=row.get('Feed?'))
            upd['Network'] = st.text_input("Network", value=row.get('Network'))

    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter (Logbog)", value=row.get('Noter'), height=250)
        st.file_uploader("Vedhæft filer (Info gemmes manuelt)", key=f"f_{idx}")

    if st.button("💾 GEM KLIENT KORT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_data(st.session_state.df): st.rerun()

# --- 7. UI ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("📤 Eksport")
    st.download_button("📥 Master CSV (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'filtered' in st.session_state:
        st.download_button("📥 Udvalgte CSV (Filter)", st.session_state.filtered.to_csv(index=False), "udvalgte.csv", use_container_width=True)

    st.divider()
    st.header("📊 Sortering")
    s_col = st.selectbox("Sortér efter:", ["Date Added", "Produkter", "EPC", "Virksomhed", "Dialog", "Kontakt dato"])
    s_asc = st.radio("Orden:", ["Højeste/Nyeste", "Laveste/Ældste"])
    if st.button("Udfør Sortering", use_container_width=True):
        st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_asc=="Laveste/Ældste"))
        save_data(st.session_state.df); st.rerun()

    st.divider()
    st.header("📥 Import")
    kat_up = st.text_input("Kategori ved upload:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Vælg fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd['Kategori'] = kat_up
        st.session_state.df = force_master_structure(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_data(st.session_state.df); st.rerun()

    if st.button("🚨 NULSTIL DATABASE"):
        if db_engine:
            with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants")); conn.commit()
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. CRM TABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i alt...", "")
    df_show = st.session_state.df.copy()
    if search:
        mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_show = df_show[mask]
    st.session_state.filtered = df_show

    st.write(f"Antal: **{len(df_show)}**")
    st.info("💡 Klik på den lille grå boks helt til venstre for at åbne kortet.")

    # TABELVISNING
    sel = st.dataframe(
        df_show,
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_show.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("👋 Databasen er tom. Upload en fil.")
