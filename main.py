import streamlit as st
import pandas as pd
import re
import os
import sqlalchemy

st.set_page_config(page_title="Affiliate CRM", layout="wide")

# --- DATABASE FORBINDELSE ---
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        return sqlalchemy.create_engine(db_url)
    return None

engine = get_db_connection()

def load_from_db():
    if engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", engine)
            return df.fillna("")
        except:
            return pd.DataFrame()
    return pd.DataFrame()

def save_to_db(df):
    if engine:
        # Sikrer link-format før gem
        for col in ['Merchant', 'Programnavn']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: f"https://www.{str(x).lower().strip().replace(' ', '')}.dk" 
                                        if isinstance(x, str) and '.' not in x and len(x) > 2 else x)
        df.to_sql("merchants", engine, if_exists="replace", index=False)

if 'df' not in st.session_state:
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

st.title("💼 Affiliate CRM & Lead Database")

# --- SIDEPANEL ---
with st.sidebar:
    st.header("🏆 CRM Kontrol")
    
    if not st.session_state.df.empty:
        if st.button("💾 GEM ALLE RETTELSER", use_container_width=True, type="primary"):
            save_to_db(st.session_state.df)
            st.success("CRM opdateret permanent!")
            st.rerun()

    st.markdown("---")
    st.header("📥 Import / Opdatering")
    uploaded_file = st.file_uploader("Upload fil med ændringer", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
        new_data = new_data.fillna("")
        
        if st.button("Flet & Opdater Data"):
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                if 'Land' not in new_data.columns:
                    new_data['Land'] = new_data.apply(detect_land, axis=1)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    if 'MATCH_KEY' not in st.session_state.df.columns:
                        st.session_state.df['MATCH_KEY'] = st.session_state.df[name_col].apply(clean_name)
                    
                    # LOGIK: Vi sætter MATCH_KEY som index for begge
                    existing_df = st.session_state.df.set_index('MATCH_KEY')
                    incoming_df = new_data.set_index('MATCH_KEY')
                    
                    # 1. Update: Overskriver eksisterende rækker med NYE værdier fra filen
                    existing_df.update(incoming_df)
                    
                    # 2. Combine_first: Udfylder huller (nye kolonner/rækker) uden at slette eksisterende
                    # Dette sikrer at dine manuelle noter ikke bliver overskrevet af tomme felter i filen
                    final_df = existing_df.combine_first(incoming_df)
                    
                    st.session_state.df = final_df.reset_index()
                
                save_to_db(st.session_state.df)
                st.success("CRM opdateret med ændringer!")
                st.rerun()

    st.markdown("---")
    st.header("📤 Eksport")
    if not st.session_state.df.empty:
        st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "crm_master.csv", use_container_width=True)

    if st.button("🚨 Nulstil alt", use_container_width=True):
        if engine:
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS merchants"))
        st.session_state.df = pd.DataFrame()
        st.rerun()

# --- CRM VISNING ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    
    df_crm = st.session_state.df.copy()
    if search:
        mask = df_crm.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_crm = df_crm[mask]
    
    # Kolonne Konfiguration
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small"),
        "Merchant": st.column_config.LinkColumn("Website"),
        "Programnavn": st.column_config.LinkColumn("Website"),
        "MATCH_KEY": None 
    }

    # Tabellen viser dine data, og du kan sortere ved at klikke på overskrifterne
    st.session_state.df = st.data_editor(
        df_crm,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=800,
        key="crm_editor"
    )
else:
    st.info("👋 Databasen er klar. Upload en fil for at starte dit CRM.")
