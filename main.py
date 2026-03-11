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
        # Auto-link logik før gem
        for col in ['Merchant', 'Programnavn', 'Website']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: f"https://{str(x).lower().strip().replace(' ', '')}.dk" 
                                        if isinstance(x, str) and len(x) > 2 and not x.startswith('http') and '.' not in x 
                                        else x)
        df.to_sql("merchants", engine, if_exists="replace", index=False)

if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- FORBEDRET FLAG-LOGIK ---
def detect_land(row):
    # Vi kigger i alle celler i rækken efter DK, SE, NO eller landenavne
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

# --- SIDEPANEL: EXCEL-STYLE FILTRE ---
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
                # Sæt flag baseret på indholdet
                new_data['Land'] = new_data.apply(detect_land, axis=1)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    if 'MATCH_KEY' not in st.session_state.df.columns:
                        st.session_state.df['MATCH_KEY'] = st.session_state.df[next((c for c in potential_names if c in st.session_state.df.columns))].apply(clean_name)
                    
                    st.session_state.df = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                
                save_to_db(st.session_state.df)
                st.success("Database opdateret med flag og links!")

    st.markdown("---")
    st.header("Excel Filtre")
    if not st.session_state.df.empty:
        if 'Land' in st.session_state.df.columns:
            selected_land = st.multiselect("Filtrer på Land", st.session_state.df['Land'].unique())
        
        st.markdown("---")
        st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "full_database.csv")

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i hele databasen...", "")
    
    df_filtered = st.session_state.df.copy()
    
    # Anvend sidebar filtre
    if 'Land' in df_filtered.columns and selected_land:
        df_filtered = df_filtered[df_filtered['Land'].isin(selected_land)]
    
    # Anvend søgning
    if search:
        mask = df_filtered.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_filtered = df_filtered[mask]

    # Kolonne Opsætning
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small", help="Klik for at sortere"),
        "Merchant": st.column_config.LinkColumn("Website"),
        "Programnavn": st.column_config.LinkColumn("Website"),
        "MATCH_KEY": None 
    }

    st.write(f"Antal rækker: **{len(df_filtered)}**")
    st.caption("💡 TIP: Klik på kolonne-navnet for at sortere. Træk i kolonnen for at flytte den.")

    # DEN STORE EDITERBARE TABEL
    edited_df = st.data_editor(
        df_filtered,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=700,
        key="excel_editor" # Fast key sikrer at sortering bevares bedre
    )

    if st.button("💾 Gem rettelser permanent"):
        st.session_state.df.update(edited_df)
        save_to_db(st.session_state.df)
        st.success("Gemt! Sortering og rettelser er nu i databasen.")

else:
    st.info("Upload en fil for at starte.")
