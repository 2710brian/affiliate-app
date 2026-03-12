import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
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
        except Exception:
            return None
    return None

db_engine = get_engine()

# --- DEFINER DIN MASTER RÆKKEFØLGE (1-22) ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Status', 'Dato', 'Network', 'Land'
]

# --- HJÆLPEFUNKTIONER ---
def detect_land_icon(row):
    # Kig i Network, Programnavn eller Virksomhed efter DK
    search_text = " ".join([str(val) for val in row.values]).lower()
    if any(x in search_text for x in ["denmark", "dk", "dansk"]): return "🇩🇰"
    if any(x in search_text for x in ["sweden", "se", "svensk", "sverige"]): return "🇸🇪"
    if any(x in search_text for x in ["norway", "no", "norsk", "norge"]): return "🇳🇴"
    return "🌐"

def format_date_clean(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '']: return ""
    try:
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        return dt.strftime('%d/%m/%Y') if not pd.isna(dt) else str(val)
    except: return str(val)

def final_clean_and_sort(df):
    if df.empty: return df
    
    # 1. Omdøb kendte navne til dine Master-navne
    rename_map = {
        'Merchant': 'Virksomhed',
        'Product Count': 'Produkter',
        'EPC (nøgletal)': 'EPC',
        'DateAdded': 'Date Added'
    }
    df = df.rename(columns=rename_map)
    
    # 2. Rens tekniske fejl-ord
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0'], '')
    
    # 3. Sikr at alle 22 kolonner findes (opret dem hvis de mangler)
    for c in MASTER_COLS:
        if c not in df.columns:
            df[c] = ""
            
    # 4. Sæt lande-flag hvis det mangler
    if 'Land' not in df.columns or (df['Land'] == "").all():
        df['Land'] = df.apply(detect_land_icon, axis=1)

    # 5. Rens datoer og tal
    for col in ['Date Added', 'Dato', 'Kontaktet']:
        df[col] = df[col].apply(format_date_clean)
    
    if 'Produkter' in df.columns:
        df['Produkter'] = pd.to_numeric(df['Produkter'].str.replace(r'[^0-9]', '', regex=True), errors='coerce').fillna(0).astype(int)
    if 'EPC' in df.columns:
        df['EPC'] = pd.to_numeric(df['EPC'].str.replace(',', '.'), errors='coerce').fillna(0.0)
        
    return df[MASTER_COLS + [c for c in df.columns if c not in MASTER_COLS and c != 'MATCH_KEY']]

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return final_clean_and_sort(df)
        except: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = final_clean_and_sort(df)
            # Lav MATCH_KEY til databasen
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl: {e}")
    return False

# --- INITIALISERING ---
if 'df' not in st.session_state or st.session_state.df is None:
    st.session_state.df = load_data()

# --- POP-UP REDIGERING ---
@st.dialog("✏️ Hurtig-Redigering")
def edit_popup(row_idx, real_df_index):
    row = st.session_state.df.loc[real_df_index].to_dict()
    st.write(f"### {row.get('Virksomhed')}")
    
    c1, c2 = st.columns(2)
    with c1:
        fnavn = st.text_input("Fornavn", value=row.get('Fornavn', ''))
        enavn = st.text_input("Efternavn", value=row.get('Efternavn', ''))
        mail = st.text_input("E-mail", value=row.get('Mail', ''))
    with c2:
        tlf = st.text_input("Tlf", value=row.get('Tlf', ''))
        status = st.selectbox("Status", ["Ikke ansøgt", "Godkendt", "Afvist", "Lead oprettet", "Kontaktet"], index=0)
        k_dato = st.date_input("Kontaktet dato")

    noter = st.text_area("Noter", value=row.get('Kontaktet', '') if not k_dato else "")

    if st.button("💾 Gem lead"):
        st.session_state.df.at[real_df_index, 'Fornavn'] = fnavn
        st.session_state.df.at[real_df_index, 'Efternavn'] = enavn
        st.session_state.df.at[real_df_index, 'Mail'] = mail
        st.session_state.df.at[real_df_index, 'Tlf'] = tlf
        st.session_state.df.at[real_df_index, 'Status'] = status
        st.session_state.df.at[real_df_index, 'Kontaktet'] = k_dato.strftime('%d/%m/%Y')
        if save_data(st.session_state.df):
            st.rerun()

# --- UI ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("⚙️ Kontrol")
    if st.button("💾 GEM ÆNDRINGER", type="primary", use_container_width=True):
        save_data(st.session_state.df)
        st.success("Gemt!")

    st.markdown("---")
    # SIDEBAR SORTERING
    st.subheader("📊 Sortering")
    s_col = st.selectbox("Sortér efter:", ["Date Added", "Produkter", "EPC", "Virksomhed", "Status"])
    s_order = st.radio("Orden:", ["Nyeste/Højeste", "Ældste/Laveste"])
    if st.button("Udfør Sortering"):
        st.session_state.df = st.session_state.df.sort_values(s_col, ascending=(s_order == "Ældste/Laveste"))
        save_data(st.session_state.df)
        st.rerun()

    st.markdown("---")
    st.subheader("📥 Import")
    kat = st.text_input("Kategori:", "Bolig, Have og Interiør")
    f = st.file_uploader("Upload fil")
    if f and st.button("Flet"):
        nd = pd.read_csv(f) if f.name.endswith('csv') else pd.read_excel(f)
        nd['Kategori'] = kat
        st.session_state.df = final_clean_and_sort(pd.concat([st.session_state.df, nd]))
        save_data(st.session_state.df)
        st.rerun()

    if st.button("🚨 Nulstil Alt"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants"))
                conn.commit()
        st.session_state.df = pd.DataFrame()
        st.rerun()

# --- CRM TABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg...", "")
    df_view = st.session_state.df.copy()
    if search:
        mask = df_view.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_view = df_view[mask]

    st.info("💡 Klik på firkanten til venstre for en række for at redigere.")
    
    sel = st.dataframe(
        df_view,
        column_config={"Website": st.column_config.LinkColumn("Website"), "MATCH_KEY": None},
        use_container_width=True,
        selection_mode="single-row",
        on_select="rerun"
    )

    if sel.selection.rows:
        idx = sel.selection.rows[0]
        real_idx = df_view.index[idx]
        if st.button(f"✏️ Rediger {df_view.iloc[idx]['Virksomhed']}", type="primary", use_container_width=True):
            edit_popup(idx, real_idx)
else:
    st.info("👋 Upload data for at starte.")
