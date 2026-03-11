import streamlit as st
import pandas as pd
import re
import os
import sqlalchemy

st.set_page_config(page_title="Affiliate Master Database", layout="wide")

# --- DATABASE FORBINDELSE (Railway PostgreSQL) ---
def get_db_connection():
    # Railway giver automatisk en DATABASE_URL med
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        # Fix for SQLAlchemy hvis URL starter med postgres:// (skal være postgresql://)
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        return sqlalchemy.create_engine(db_url)
    return None

engine = get_db_connection()
DB_FILE = "master_database.csv"

def load_data():
    if engine:
        try:
            return pd.read_sql("SELECT * FROM merchants", engine).fillna("")
        except:
            return pd.DataFrame()
    if os.path.exists(DB_FILE):
        return pd.read_csv(DB_FILE).fillna("")
    return pd.DataFrame()

def save_data(df):
    if engine:
        df.to_sql("merchants", engine, if_exists="replace", index=False)
    else:
        df.to_csv(DB_FILE, index=False)

if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- HJÆLPEFUNKTIONER ---
def get_flag(network_str):
    network_str = str(network_str).lower()
    if "denmark" in network_str or " dk" in network_str: return "🇩🇰"
    if "sweden" in network_str or " se" in network_str: return "🇸🇪"
    if "norway" in network_str or " no" in network_str: return "🇳🇴"
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
            name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in new_data.columns), None)
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                # Tilføj flag automatisk hvis netværk findes
                if 'Network' in new_data.columns:
                    new_data['Land'] = new_data['Network'].apply(get_flag)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    st.session_state.df = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                
                save_data(st.session_state.df)
                st.success("Database opdateret!")

    st.markdown("---")
    if not st.session_state.df.empty:
        st.download_button("📥 Download Master CSV", st.session_state.df.to_csv(index=False), "master.csv")

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg...", "")
    
    df_display = st.session_state.df.copy()
    if search:
        mask = df_display.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_display = df_display[mask]

    # Visning af flag og links i data_editor
    # Vi bruger st.column_config til at lave klikbare links
    column_config = {
        "Merchant": st.column_config.LinkColumn("Website Link"),
        "Programnavn": st.column_config.LinkColumn("Website Link"),
        "Land": st.column_config.TextColumn("Land", width="small")
    }

    st.write(f"Antal annoncører: {len(df_display)}")

    edited_df = st.data_editor(
        df_display,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=600
    )

    if st.button("💾 Gem alle rettelser"):
        st.session_state.df = edited_df
        save_data(edited_df)
        st.success("Gemt i databasen!")
else:
    st.info("Upload en fil for at starte.")
