import streamlit as st
import pandas as pd
import re
import os
import sqlalchemy
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Affiliate CRM", layout="wide")

# --- DATABASE FORBINDELSE ---
def get_db_connection():
    # Railway tilføjer automatisk DATABASE_URL
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url)
            # Test forbindelsen
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception as e:
            st.sidebar.error(f"Databasefejl: {e}")
            return None
    return None

engine = get_db_connection()

def load_from_db():
    if engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", engine)
            return df.fillna("")
        except:
            # Hvis tabellen ikke findes endnu
            return pd.DataFrame()
    return pd.DataFrame()

def save_to_db(df):
    if engine:
        try:
            # Forbered links før gem
            for col in ['Merchant', 'Programnavn', 'Website']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: f"https://www.{str(x).lower().strip().replace(' ', '')}.dk" 
                                            if isinstance(x, str) and '.' not in x and len(x) > 2 else x)
            
            # Gem til SQL (overskriver den gamle tabel med nyeste data)
            df.to_sql("merchants", engine, if_exists="replace", index=False)
            return True
        except Exception as e:
            st.error(f"Kunne ikke gemme: {e}")
            return False
    return False

# Indlæs data én gang ved start
if 'df' not in st.session_state or st.session_state.df.empty:
    st.session_state.df = load_from_db()

# --- HJÆLPEFUNKTIONER ---
def detect_land(row):
    row_str = " ".join(row.values.astype(str)).lower()
    if any(x in row_str for x in ["denmark", " dk", "dansk"]): return "🇩🇰"
    if any(x in row_str for x in ["sweden", " se", "svensk", "sverige"]): return "🇸🇪"
    if any(x in row_str for x in ["norway", " no", "norsk", "norge"]): return "🇳🇴"
    return "🌐"

def clean_name(name):
    if pd.isna(name): return ""
    name = str(name).lower()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', name)
    return re.sub(r'[^a-z0-9]', '', name).strip()

st.title("💼 Affiliate CRM")

# --- SIDEPANEL ---
with st.sidebar:
    st.header("🏆 CRM Status")
    if engine:
        st.success("✅ Forbundet til Railway Database")
    else:
        st.error("❌ Ingen Database (Bruger kun hukommelse)")

    if not st.session_state.df.empty:
        if st.button("💾 GEM ALT PERMANENT", use_container_width=True, type="primary"):
            if save_to_db(st.session_state.df):
                st.success("Gemt i databasen!")
                st.rerun()

    st.markdown("---")
    st.header("📥 Import / Opdater")
    uploaded_file = st.file_uploader("Upload fil", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
        new_data = new_data.fillna("")
        
        if st.button("Flet & Gem i Database", use_container_width=True):
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                new_data['Land'] = new_data.apply(detect_land, axis=1)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    # Merge logik
                    existing = st.session_state.df.set_index('MATCH_KEY')
                    incoming = new_data.set_index('MATCH_KEY')
                    existing.update(incoming)
                    st.session_state.df = existing.combine_first(incoming).reset_index()
                
                save_to_db(st.session_state.df)
                st.rerun()

    st.markdown("---")
    if not st.session_state.df.empty:
        st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "crm_master.csv", use_container_width=True)

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    
    df_to_show = st.session_state.df
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    # Konfiguration
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small"),
        "Merchant": st.column_config.LinkColumn("Website"),
        "Programnavn": st.column_config.LinkColumn("Website"),
        "MATCH_KEY": None 
    }

    # EDITERBAR TABEL
    edited_df = st.data_editor(
        df_to_show,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=700,
        key="crm_editor"
    )

    # Synkroniser ændringer tilbage til session_state
    if not edited_df.equals(df_to_show):
        st.session_state.df.update(edited_df)
else:
    st.info("👋 Upload en fil for at starte databasen.")
