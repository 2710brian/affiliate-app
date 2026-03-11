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

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            # FIX: Vi konverterer alle kolonner til tekst for at undgå 'Dato-fejlen'
            return df.astype(str).replace("None", "").replace("nan", "")
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            # Auto-link logik
            for col in ['Merchant', 'Programnavn', 'Website']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: f"https://www.{str(x).lower().strip().replace(' ', '')}.dk" 
                                            if isinstance(x, str) and len(x) > 2 and '.' not in x and not x.startswith('http') else x)
            
            # Gem som tekst i databasen for at bevare formatering
            df.astype(str).to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
    return False

# --- SESSION STATE ---
if 'df' not in st.session_state or st.session_state.df.empty:
    st.session_state.df = load_data()

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

st.title("💼 Affiliate CRM Pro")

# --- SIDEPANEL ---
with st.sidebar:
    st.header("🏆 Kontrolpanel")
    if db_engine:
        st.success("✅ Database Online")
    else:
        st.error("❌ Database Offline")

    if not st.session_state.df.empty:
        if st.button("💾 GEM ALLE RETTELSER", type="primary", use_container_width=True):
            if save_data(st.session_state.df):
                st.success("Alt er gemt!")
                st.rerun()

    st.markdown("---")
    st.header("📥 Import / Sync")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
    
    if uploaded_file:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
        # Konverter ny data til tekst med det samme
        new_data = new_data.astype(str).replace("nan", "").replace("None", "")
        
        if st.button("Flet & Opdater CRM", use_container_width=True):
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                new_data['Land'] = new_data.apply(detect_land, axis=1)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    # Sikr MATCH_KEY på eksisterende data
                    if 'MATCH_KEY' not in st.session_state.df.columns:
                        st.session_state.df['MATCH_KEY'] = st.session_state.df[next((c for c in potential_names if c in st.session_state.df.columns), name_col)].apply(clean_name)
                    
                    st.session_state.df = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                
                save_data(st.session_state.df)
                st.rerun()

# --- CRM HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i alt (navn, dato, land, kategori...)", "")
    
    df_to_show = st.session_state.df.copy()
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    # Kolonne konfiguration
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small", pinned=True),
        "Merchant": st.column_config.LinkColumn("Website", pinned=True),
        "Programnavn": st.column_config.LinkColumn("Website"),
        "MATCH_KEY": None 
    }

    st.write(f"Antal annoncører: **{len(df_to_show)}**")

    # TABEL MED FAST TOP OG SORTERING
    edited_df = st.data_editor(
        df_to_show,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=800,
        key="crm_editor_pro_v2"
    )

    if not edited_df.equals(df_to_show):
        st.session_state.df.update(edited_df)
else:
    st.info("👋 Upload din database for at aktivere CRM.")
