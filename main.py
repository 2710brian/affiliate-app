import streamlit as st
import pandas as pd
import re
import os
import sqlalchemy

st.set_page_config(page_title="Affiliate Master Database", layout="wide")

# --- DATABASE LOGIK ---
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        return sqlalchemy.create_engine(db_url)
    return None

engine = get_db_connection()

def load_data():
    if engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", engine)
            return df.fillna("")
        except:
            return pd.DataFrame()
    return pd.DataFrame()

def save_to_db(df):
    if engine:
        df.to_sql("merchants", engine, if_exists="replace", index=False)

# Funktion til at skabe et automatisk link ud fra navnet
def auto_generate_link(name):
    if not name or "http" in str(name): return name
    # Rens navnet for mærkelige tegn og mellemrum
    clean = str(name).lower().strip()
    clean = re.sub(r'[^a-z0-9]', '', clean)
    if clean:
        return f"https://www.{clean}.dk" # Vi gætter på .dk som standard
    return name

if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- HJÆLPEFUNKTIONER ---
def get_flag(network_str):
    s = str(network_str).lower()
    if "denmark" in s or " dk" in s: return "🇩🇰"
    if "sweden" in s or " se" in s: return "🇸🇪"
    if "norway" in s or " no" in s: return "🇳🇴"
    return "🌐"

def clean_name(name):
    if pd.isna(name): return ""
    name = str(name).lower()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as', '', name)
    return re.sub(r'[^a-z0-9]', '', name).strip()

st.title("🚀 Affiliate Master Database")

# --- SIDEPANEL ---
with st.sidebar:
    st.header("Importer Data")
    uploaded_file = st.file_uploader("Upload fil", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
        new_data = new_data.fillna("")
        
        if st.button("Opdater & Samkør"):
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                
                # Sæt flag
                if 'Network' in new_data.columns:
                    new_data['Land'] = new_data['Network'].apply(get_flag)
                
                # AUTOMATISK LINK GENERERING
                # Hvis Merchant/Programnavn ikke er et link endnu, så lav et gæt
                new_data[name_col] = new_data[name_col].apply(auto_generate_link)

                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    st.session_state.df = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                
                save_to_db(st.session_state.df)
                st.success("Database opdateret med automatiske links!")

    st.markdown("---")
    if not st.session_state.df.empty:
        st.download_button("📥 Master (Alt)", st.session_state.df.to_csv(index=False), "master_full.csv")

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg efter alt...", "")
    
    df_filtered = st.session_state.df.copy()
    if search:
        mask = df_filtered.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_filtered = df_filtered[mask]

    # Kolonne konfiguration - Gør navne-kolonnen til et link
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small"),
        "Merchant": st.column_config.LinkColumn("Website (Auto-Link)"),
        "Programnavn": st.column_config.LinkColumn("Website (Auto-Link)"),
        "MATCH_KEY": None 
    }

    st.write(f"Antal rækker: **{len(df_filtered)}**")

    edited_df = st.data_editor(
        df_filtered,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=600,
        key="main_editor"
    )

    if st.button("💾 Gem alle rettelser"):
        st.session_state.df = edited_df
        save_to_db(edited_df)
        st.success("Gemt!")

else:
    st.info("Upload din fil for at generere din database.")
