import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime

# --- KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Pro", layout="wide")

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

# --- DIN MASTER RÆKKEFØLGE (1-22) ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Status', 'Dato', 'Network', 'Land'
]

# --- HJÆLPEFUNKTIONER ---
def format_date_clean(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return ""
    try:
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        return dt.strftime('%d/%m/%Y') if not pd.isna(dt) else str(val).split(' ')[0]
    except: return str(val).split(' ')[0]

def final_clean_and_sort(df):
    if df.empty: return df
    
    # Omdøb før rens
    rename_map = {'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC'}
    df = df.rename(columns=rename_map)
    
    # Rens for tekniske ord
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')
    
    # Konverter tal (EPC og Produkter) - SIKKER METODE
    if 'Produkter' in df.columns:
        df['Produkter'] = pd.to_numeric(df['Produkter'].astype(str).str.replace(r'[^0-9]', '', regex=True), errors='coerce').fillna(0).astype(int)
    if 'EPC' in df.columns:
        df['EPC'] = pd.to_numeric(df['EPC'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0.0)

    # Formater alle dato-kolonner
    for col in ['Date Added', 'Dato', 'Kontaktet']:
        if col in df.columns:
            df[col] = df[col].apply(format_date_clean)

    # Tving master rækkefølge
    for c in MASTER_COLS:
        if c not in df.columns: df[c] = ""
    
    return df[MASTER_COLS]

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
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
            return False
    return False

# --- SESSION STATE ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- POP-UP REDIGERING (ALLE 22 FELTER) ---
@st.dialog("📝 Klient-kort / Hurtig-redigering", width="large")
def edit_popup(real_df_index):
    row = st.session_state.df.loc[real_df_index].to_dict()
    st.write(f"## {row.get('Virksomhed')}")
    st.divider()

    # Vi deler de 22 felter op i 3 kolonner i pop-up'en
    c1, c2, c3 = st.columns(3)
    new_values = {}

    for i, col in enumerate(MASTER_COLS):
        target_col = [c1, c2, c3][i % 3]
        with target_col:
            if col in ['Status']:
                options = ["Ikke ansøgt", "Godkendt", "Afvist", "Lead oprettet", "Kontaktet", "Afventer"]
                default_idx = options.index(row[col]) if row[col] in options else 0
                new_values[col] = st.selectbox(col, options, index=default_idx)
            elif col in ['Kontaktet', 'Date Added', 'Dato']:
                new_values[col] = st.text_input(col, value=row.get(col, ""))
            else:
                new_values[col] = st.text_input(col, value=row.get(col, ""))

    st.divider()
    st.subheader("📎 Dokumenter & Billeder")
    att = st.file_uploader("Upload info til klient (PDF, JPG, PNG)", key=f"file_{real_df_index}")
    if att:
        st.success(f"Fil registreret: {att.name}")
        new_values['Vedhæftning'] = att.name # Vi gemmer navnet i databasen

    if st.button("💾 GEM KLIENT-DATA", type="primary", use_container_width=True):
        for k, v in new_values.items():
            if k in st.session_state.df.columns:
                st.session_state.df.at[real_df_index, k] = v
        if save_data(st.session_state.df):
            st.rerun()

# --- UI START ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("📤 Eksport")
    if not st.session_state.df.empty:
        st.download_button("📥 Download Master (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
        if 'filtered_df' in st.session_state:
            st.download_button("📥 Download Udvalgte", st.session_state.filtered_df.to_csv(index=False), "udvalgte.csv", use_container_width=True)

    st.markdown("---")
    st.header("📥 Import")
    kat = st.text_input("Kategori for upload:", "Bolig, Have og Interiør")
    f = st.file_uploader("Upload fil")
    if f and st.button("Flet & Gem"):
        nd = pd.read_csv(f) if f.name.endswith('csv') else pd.read_excel(f)
        nd['Kategori'] = kat
        st.session_state.df = final_clean_and_sort(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_data(st.session_state.df)
        st.rerun()

    st.markdown("---")
    if st.button("🚨 Nulstil"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants"))
                conn.commit()
        st.session_state.df = pd.DataFrame()
        st.rerun()

# --- CRM HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM (Navn, Kategori, Mail...)", "")
    
    display_df = st.session_state.df.copy()
    if search:
        mask = display_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        display_df = display_df[mask]
    
    st.session_state.filtered_df = display_df
    
    st.subheader(f"Antal annoncører: {len(display_df)}")
    st.info("💡 Marker rækken til venstre og tryk på 'Rediger Klient' herunder.")

    # TABEL
    sel = st.dataframe(
        display_df,
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True,
        selection_mode="single-row",
        on_select="rerun",
        height=600
    )

    if sel.selection.rows:
        row_idx = sel.selection.rows[0]
        real_idx = display_df.index[row_idx]
        if st.button(f"✏️ Rediger Klient: {display_df.iloc[row_idx]['Virksomhed']}", type="primary", use_container_width=True):
            edit_popup(real_idx)
else:
    st.info("👋 Databasen er tom. Upload en fil for at starte.")
