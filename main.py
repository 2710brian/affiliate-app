import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text

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

# --- TYPE-MOTOR (Sikrer Sortering A-Z og 0-9) ---
def clean_and_fix_types(df):
    if df.empty:
        return df
    
    # 1. Rens for tekniske ord
    df = df.replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'unknown'], '')
    
    # 2. TVING rækkefølge af vigtigste kolonner hvis de findes
    order = ['Merchant', 'Website', 'Kategori', 'Status', 'Product Count', 'EPC (nøgletal)', 'Mail', 'Kontaktet']
    existing_order = [c for c in order if c in df.columns]
    other_cols = [c for c in df.columns if c not in existing_order and c != 'MATCH_KEY']
    df = df[existing_order + other_cols]

    # 3. Konverter tal-kolonner til rigtige tal (så sortering virker)
    if 'Product Count' in df.columns:
        df['Product Count'] = pd.to_numeric(df['Product Count'].astype(str).str.replace(r'[^0-9]', '', regex=True), errors='coerce').fillna(0).astype(int)
    
    if 'EPC (nøgletal)' in df.columns:
        # Håndterer både dansk komma og engelsk punktum
        df['EPC (nøgletal)'] = df['EPC (nøgletal)'].astype(str).str.replace(',', '.')
        df['EPC (nøgletal)'] = pd.to_numeric(df['EPC (nøgletal)'], errors='coerce').fillna(0.0)

    return df

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return clean_and_fix_types(df)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = clean_and_fix_types(df)
            if 'MATCH_KEY' in df.columns:
                df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
    return False

# --- SESSION STATE ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- HJÆLPEFUNKTIONER ---
def clean_name_for_match(name):
    if not name or str(name) == "None": return "temp_" + os.urandom(4).hex()
    name = str(name).lower().strip()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', name)
    return re.sub(r'[^a-z0-9]', '', name)

def generate_website_link(name):
    if not name or "http" in str(name): return name
    clean = str(name).lower().strip().replace(" ", "").replace(".dk", "").replace(".se", "")
    clean = re.sub(r'[^a-z0-9]', '', clean)
    return f"https://www.{clean}.dk" if clean else ""

st.title("💼 Affiliate CRM Pro")

# --- SIDEPANEL ---
with st.sidebar:
    st.header("⚙️ Kontrolpanel")
    
    if st.button("💣 NULSTIL ALT", use_container_width=True):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants"))
                conn.commit()
        st.session_state.df = pd.DataFrame()
        st.rerun()

    if not st.session_state.df.empty:
        if st.button("💾 GEM ALLE RETTELSER", type="primary", use_container_width=True):
            if save_data(st.session_state.df):
                st.success("Gemt permanent!")
                st.rerun()

    st.markdown("---")
    st.header("📥 Import")
    valgt_kategori = st.text_input("Kategori for upload:", "Bolig, Have og Interiør")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
    
    if uploaded_file:
        if st.button("Flet & Opdater CRM", use_container_width=True):
            file_ext = uploaded_file.name.split('.')[-1]
            new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
            new_data = clean_and_fix_types(new_data)
            
            name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in new_data.columns), None)
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name_for_match)
                new_data['Kategori'] = valgt_kategori
                if 'Website' not in new_data.columns:
                    new_data['Website'] = new_data[name_col].apply(generate_website_link)
                
                new_data = new_data.drop_duplicates('MATCH_KEY')

                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    # Rens eksisterende for dubletter
                    st.session_state.df = st.session_state.df.drop_duplicates('MATCH_KEY')
                    
                    existing = st.session_state.df.set_index('MATCH_KEY')
                    incoming = new_data.set_index('MATCH_KEY')
                    
                    # Opdater kun tal og status
                    cols_to_upd = [c for c in incoming.columns if c in ['Product Count', 'EPC (nøgletal)', 'Status', 'Kategori', 'Website']]
                    existing.update(incoming[cols_to_upd])
                    st.session_state.df = existing.combine_first(incoming).reset_index()
                
                save_data(st.session_state.df)
                st.rerun()

    # --- KATEGORI FILTER ---
    if not st.session_state.df.empty and 'Kategori' in st.session_state.df.columns:
        st.markdown("---")
        st.header("🔍 Filtre")
        kat_list = ["Alle"] + sorted(list(st.session_state.df['Kategori'].unique()))
        valgt_kat_filter = st.selectbox("Vis kun kategori:", kat_list)

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg efter navn, mail eller noter...", "")
    
    df_to_show = st.session_state.df.copy()
    
    # 1. Anvend Kategori filter
    if 'valgt_kat_filter' in locals() and valgt_kat_filter != "Alle":
        df_to_show = df_to_show[df_to_show['Kategori'] == valgt_kat_filter]

    # 2. Anvend Søgning
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    st.write(f"Antal viste: **{len(df_to_show)}**")

    # Kolonne Konfiguration (Definerer datatyper til sortering)
    column_config = {
        "Website": st.column_config.LinkColumn("Website", width="medium"),
        "Product Count": st.column_config.NumberColumn("Produkter", format="%d", help="Sortering: Klik her"),
        "EPC (nøgletal)": st.column_config.NumberColumn("EPC", format="%.2f", help="Sortering: Klik her"),
        "Merchant": st.column_config.TextColumn("Virksomhed", pinned=True),
        "MATCH_KEY": None # Skjul altid
    }

    # EDITERBAR TABEL
    # Her kan du nu flytte kolonner ved at trække i dem i browseren
    edited_df = st.data_editor(
        df_to_show,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=700,
        key="crm_final_pro_sorting"
    )

    if not edited_df.equals(df_to_show):
        # Gem kun rettelserne ind i master-df
        st.session_state.df.update(edited_df)
else:
    st.info("👋 Upload en fil for at starte dit CRM.")
