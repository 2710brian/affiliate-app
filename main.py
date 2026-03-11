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
            # Identificer navne-kolonne
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                # Tilføj flag/land hvis det ikke findes
                if 'Land' not in new_data.columns:
                    new_data['Land'] = new_data.get('Network', '').apply(get_flag)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    # Gør eksisterende data klar til merge
                    if 'MATCH_KEY' not in st.session_state.df.columns:
                        st.session_state.df['MATCH_KEY'] = st.session_state.df[name_col].apply(clean_name)
                    
                    st.session_state.df = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                
                save_to_db(st.session_state.df)
                st.success("Database opdateret!")

    st.markdown("---")
    st.header("Eksport")
    if not st.session_state.df.empty:
        # Master Export
        st.download_button("📥 Master (Alt)", st.session_state.df.to_csv(index=False), "master_full.csv")

    if st.button("🚨 Ryd alt"):
        st.session_state.df = pd.DataFrame()
        if engine:
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS merchants"))
        st.rerun()

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg efter alt (navn, land, kategori...)", "")
    
    # Filtrerings-logik
    df_filtered = st.session_state.df.copy()
    if search:
        mask = df_filtered.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_filtered = df_filtered[mask]

    # Konfiguration af kolonner (Flag, Links, Sortering)
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small", help="Nationalitet"),
        "Merchant": st.column_config.LinkColumn("Website", help="Klik for at åbne"),
        "Programnavn": st.column_config.LinkColumn("Website", help="Klik for at åbne"),
        "MATCH_KEY": None # Skjul denne kolonne
    }

    st.write(f"Viser **{len(df_filtered)}** annoncører")

    # DEN EDITERBARE TABEL
    # Her kan brugeren sortere, flytte kolonner og rette
    edited_df = st.data_editor(
        df_filtered,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=600,
        key="main_editor"
    )

    # Gem-sektion
    col_save, col_exp_filter = st.columns([1, 1])
    with col_save:
        if st.button("💾 Gem alle rettelser permanent"):
            # Vi opdaterer kun de rækker der er i det filtrerede view tilbage i master
            st.session_state.df.update(edited_df)
            save_to_db(st.session_state.df)
            st.success("Ændringer gemt i databasen!")

    with col_exp_filter:
        # Download kun det man har søgt frem
        st.download_button("📥 Download filtrerede data", edited_df.to_csv(index=False), "filtreret_export.csv")

else:
    st.info("Upload en fil for at starte databasen.")
