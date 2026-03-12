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

# --- MASTER STRUKTUR ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Status', 'Dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

DIALOG_OPTIONS = [
    "Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", 
    "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", 
    "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"
]

# --- SIKKER DATO-FUNKTION ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']:
        return date.today()
    try:
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        if pd.isna(dt): return date.today()
        return dt.date()
    except:
        return date.today()

# --- RENSE-MOTOR ---
def robust_clean(df):
    if df.empty: return df
    
    # 1. Fjern dublerede kolonner med det samme
    df = df.loc[:, ~df.columns.duplicated()]
    
    # 2. Omdøb kendte felter
    rename_map = {
        'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 
        'EPC (nøgletal)': 'EPC', 'DateAdded': 'Date Added'
    }
    df = df.rename(columns=rename_map)
    
    # 3. Rens tekniske fejl-ord
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')
    
    # 4. Genskab Website hvis den mangler
    if 'Website' in df.columns:
        df['Website'] = df.apply(lambda r: f"https://www.{str(r['Virksomhed']).lower().strip().replace(' ','')}.dk" 
                                if r['Website'] == "" and r['Virksomhed'] != "" else r['Website'], axis=1)

    # 5. Sikr alle kolonner
    for c in MASTER_COLS:
        if c not in df.columns: df[c] = ""
            
    return df[MASTER_COLS]

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return robust_clean(df)
        except: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = robust_clean(df)
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Databasefejl: {e}")
            return False
    return False

# --- SESSION INITIALISERING ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- POP-UP KORT (CRM BOARD) ---
@st.dialog("📝 CRM Klient-kort", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["📊 Stamdata & Dialog", "📁 Dokumenter & Noter"])
    new_data = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            new_data['Fornavn'] = st.text_input("Fornavn", value=row.get('Fornavn'))
            new_data['Efternavn'] = st.text_input("Efternavn", value=row.get('Efternavn'))
            new_data['Mail'] = st.text_input("E-mail", value=row.get('Mail'))
            new_data['Tlf'] = st.text_input("Tlf", value=row.get('Tlf'))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            d_val = row.get('Dialog', 'Ikke kontakte')
            d_idx = DIALOG_OPTIONS.index(d_val) if d_val in DIALOG_OPTIONS else 0
            new_data['Dialog'] = st.selectbox("Status", DIALOG_OPTIONS, index=d_idx)
            new_data['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr'))
            
            # SIKKER DATO VÆLGER
            d_pick = get_safe_date(row.get('Opflg. dato'))
            new_data['Opflg. dato'] = st.date_input("Næste opfølgning", value=d_pick).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Info")
            new_data['Website'] = st.text_input("Website", value=row.get('Website'))
            new_data['Kategori'] = st.text_input("Kategori", value=row.get('Kategori'))
            new_data['Status'] = st.text_input("Status", value=row.get('Status'))

    with t2:
        st.markdown("##### 📓 Interne Noter")
        new_data['Noter'] = st.text_area("Logbog / Info", value=row.get('Noter'), height=200)
        
        st.divider()
        st.markdown("##### 📎 Vedhæftninger")
        if row.get('Fil_Navn'):
            st.info(f"Aktuel fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Hent dokument</a>', unsafe_allow_html=True)
        
        up = st.file_uploader("Upload fil til klient", key=f"file_{idx}")
        if up:
            new_data['Fil_Navn'] = up.name
            new_data['Fil_Data'] = base64.b64encode(up.read()).decode()

    if st.button("💾 GEM ÆNDRINGER", type="primary", use_container_width=True):
        for k, v in new_data.items(): st.session_state.df.at[idx, k] = v
        if save_data(st.session_state.df): st.rerun()

# --- UI START ---
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
    s_asc = st.radio("Rækkefølge:", ["Højeste/Nyeste", "Laveste/Ældste"])
    if st.button("Udfør Sortering", use_container_width=True):
        st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_asc=="Laveste/Ældste"))
        save_data(st.session_state.df); st.rerun()

    st.divider()
    st.header("📥 Import")
    k_up = st.text_input("Vælg kategori:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Vælg fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd['Kategori'] = k_up
        # Vi sikrer os at vi ikke har dublerede kolonner i filen
        nd = nd.loc[:, ~nd.columns.duplicated()]
        st.session_state.df = robust_clean(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_data(st.session_state.df); st.rerun()

    if st.button("🚨 Nulstil Alt"):
        if db_engine:
            with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants")); conn.commit()
        st.session_state.df = pd.DataFrame(); st.rerun()

# --- HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    df_show = st.session_state.df.copy()
    if search:
        mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_show = df_show[mask]
    st.session_state.filtered = df_show

    st.write(f"Antal: {len(df_show)}")
    st.info("💡 Klik på den lille boks helt til venstre i tabellen for at åbne klient-kortet.")

    # TABEL
    sel = st.dataframe(
        df_show[[c for c in df_show.columns if c != 'Fil_Data']],
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_show.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("Databasen er tom. Upload en fil for at starte.")
