import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime

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

# --- DEFINER DIN MASTER STRUKTUR (1-25) ---
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

# --- RENSE FUNKTION ---
def clean_master_df(df):
    if df.empty: return df
    
    # 1. Map navne (fjerner dubletter som Merchant/Virksomhed)
    rename_map = {
        'Merchant': 'Virksomhed', 
        'Product Count': 'Produkter', 
        'EPC (nøgletal)': 'EPC',
        'DateAdded': 'Date Added'
    }
    df = df.rename(columns=rename_map)
    
    # 2. Fjern dublerede kolonnenavne (VIGTIGT!)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # 3. Konverter alt til tekst og rens
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')
    
    # 4. Sikr at alle MASTER_COLS findes præcis én gang
    for c in MASTER_COLS:
        if c not in df.columns:
            df[c] = ""
            
    return df[MASTER_COLS]

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return clean_master_df(df)
        except: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = clean_master_df(df)
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
            return False
    return False

# --- INITIALISERING ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- POP-UP KLIENT KORT ---
@st.dialog("📋 Klient Detaljer & CRM", width="large")
def client_card(real_df_index):
    row = st.session_state.df.loc[real_df_index].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    tab1, tab2 = st.tabs(["🏠 CRM Data & Dialog", "📂 Dokumenter & Info"])
    new_vals = {}

    with tab1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            new_vals['Fornavn'] = st.text_input("Fornavn", value=row.get('Fornavn'))
            new_vals['Efternavn'] = st.text_input("Efternavn", value=row.get('Efternavn'))
            new_vals['Mail'] = st.text_input("E-mail", value=row.get('Mail'))
            new_vals['Tlf'] = st.text_input("Tlf", value=row.get('Tlf'))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            d_idx = DIALOG_OPTIONS.index(row['Dialog']) if row['Dialog'] in DIALOG_OPTIONS else 0
            new_vals['Dialog'] = st.selectbox("Dialog Status", DIALOG_OPTIONS, index=d_idx)
            new_vals['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr'))
            
            # DATO VÆLGERE
            try: d_opflg = pd.to_datetime(row['Opflg. dato'], dayfirst=True).date()
            except: d_opflg = datetime.now().date()
            new_vals['Opflg. dato'] = st.date_input("Næste Opfølgning", value=d_opflg).strftime('%d/%m/%Y')
        
        with c3:
            st.markdown("##### 📈 Info")
            new_vals['Kategori'] = st.text_input("Kategori", value=row.get('Kategori'))
            new_vals['Status'] = st.text_input("Netværk Status", value=row.get('Status'))
            new_vals['Website'] = st.text_input("Website", value=row.get('Website'))

        st.divider()
        new_vals['Noter'] = st.text_area("📓 Interne Noter om kunden", value=row.get('Noter'), height=150)

    with tab2:
        st.subheader("📎 Filer")
        if row.get('Fil_Navn'):
            st.success(f"Fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">Hent fil</a>', unsafe_allow_html=True)
        
        up = st.file_uploader("Upload ny fil", key=f"up_{real_df_index}")
        if up:
            new_vals['Fil_Navn'] = up.name
            new_vals['Fil_Data'] = base64.b64encode(up.read()).decode()

    if st.button("💾 GEM KLIENT", type="primary", use_container_width=True):
        for k, v in new_vals.items(): st.session_state.df.at[real_df_index, k] = v
        if save_data(st.session_state.df): st.rerun()

# --- UI ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("📤 Eksport")
    st.download_button("📥 Master (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'filtered_df' in st.session_state:
        st.download_button("📥 Udvalgte", st.session_state.filtered_df.to_csv(index=False), "udvalgte.csv", use_container_width=True)

    st.divider()
    st.header("📊 Sortering")
    s_col = st.selectbox("Sortér efter:", ["Date Added", "Produkter", "EPC", "Virksomhed", "Kategori"])
    s_asc = st.radio("Rækkefølge:", ["Højeste/Nyeste", "Laveste/Ældste"])
    if st.button("Udfør Sortering"):
        st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_asc=="Laveste/Ældste"))
        save_data(st.session_state.df)
        st.rerun()

    st.divider()
    st.header("📥 Import")
    kat_upload = st.text_input("Kategori ved upload:", "Bolig, Have og Interiør")
    f = st.file_uploader("Upload fil")
    if f and st.button("Flet & Gem"):
        nd = pd.read_csv(f) if f.name.endswith('csv') else pd.read_excel(f)
        nd['Kategori'] = kat_upload
        st.session_state.df = clean_master_df(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_data(st.session_state.df)
        st.rerun()

    st.divider()
    if st.button("🚨 NULSTIL DATABASE"):
        if db_engine:
            with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants")); conn.commit()
        st.session_state.df = pd.DataFrame(); st.rerun()

# --- HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    df_view = st.session_state.df.copy()
    if search:
        mask = df_view.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_view = df_view[mask]
    st.session_state.filtered_df = df_view

    st.write(f"Antal: {len(df_view)}")
    st.info("💡 Klik på boksen til venstre for at åbne kortet.")

    # Vis tabellen uden Fil_Data (for hastighed)
    sel = st.dataframe(
        df_view[[c for c in df_view.columns if c != 'Fil_Data']],
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_view.index[sel.selection.rows[0]]
        client_card(real_idx)
else:
    st.info("Databasen er tom.")
