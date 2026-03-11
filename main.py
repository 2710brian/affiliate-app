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
        # Sikrer links format før gem
        for col in ['Merchant', 'Programnavn', 'Website']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: f"https://www.{str(x).lower().strip().replace(' ', '')}.dk" 
                                        if isinstance(x, str) and len(x) > 2 and not x.startswith('http') and '.' not in x 
                                        else x)
        df.to_sql("merchants", engine, if_exists="replace", index=False)

if 'df' not in st.session_state:
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
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as', '', name)
    return re.sub(r'[^a-z0-9]', '', name).strip()

st.title("🚀 Affiliate Master Database")

# --- SIDEPANEL: KONTROL CENTER ---
with st.sidebar:
    st.header("⚙️ Kontrol Center")
    
    # GEM KNAP ØVERST
    if not st.session_state.df.empty:
        if st.button("💾 GEM ALLE RETTELSER", use_container_width=True, type="primary"):
            # Her gemmer vi hvad der ligger i st.session_state fra editoren
            if 'main_editor' in st.session_state:
                # Opdater master med ændringer fra editor
                edits = st.session_state.main_editor.get('edited_rows', {})
                # Vi gemmer den nuværende session state df
                save_to_db(st.session_state.df)
                st.success("Alt er gemt i databasen!")

    st.markdown("---")
    st.header("📥 Import")
    uploaded_file = st.file_uploader("Upload fil", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
        new_data = new_data.fillna("")
        
        if st.button("Opdater & Samkør", use_container_width=True):
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                new_data['Land'] = new_data.apply(detect_land, axis=1)
                
                # Auto-link generation før merge
                new_data[name_col] = new_data[name_col].apply(
                    lambda x: f"https://www.{str(x).lower().strip().replace(' ', '')}.dk" 
                    if isinstance(x, str) and '.' not in x else x
                )

                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    st.session_state.df = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                
                save_to_db(st.session_state.df)
                st.rerun()

    st.markdown("---")
    st.header("📤 Eksport")
    if not st.session_state.df.empty:
        # Master Export
        st.download_button("📥 Download MASTER (Alt)", st.session_state.df.to_csv(index=False), "master_full.csv", use_container_width=True)
        
        # Filtreret Export (vises kun hvis der er data i appen)
        if 'filtered_data' in st.session_state:
             st.download_button("📥 Download UDVALGTE", st.session_state.filtered_data.to_csv(index=False), "udvalgte_leads.csv", use_container_width=True)

    if st.button("🚨 Ryd alt", use_container_width=True):
        st.session_state.df = pd.DataFrame()
        if engine:
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS merchants"))
        st.rerun()

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i databasen...", "")
    
    # Filter logik
    df_temp = st.session_state.df.copy()
    if search:
        mask = df_temp.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_temp = df_temp[mask]
    
    # Gem det filtrerede data så det kan downloades fra sidebaren
    st.session_state.filtered_data = df_temp

    # Konfiguration
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small"),
        "Merchant": st.column_config.LinkColumn("Website"),
        "Programnavn": st.column_config.LinkColumn("Website"),
        "MATCH_KEY": None 
    }

    st.write(f"Viser **{len(df_temp)}** annoncører")

    # Den editerbare tabel
    # Sortering foregår ved at klikke på overskrifterne
    st.session_state.df = st.data_editor(
        df_temp,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=700,
        key="main_editor"
    )

else:
    st.info("Start med at uploade en fil i venstre side.")
