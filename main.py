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

# Rens tekniske fejl-værdier
def clean_values(df):
    return df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0'], '')

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return clean_values(df)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            # Gør links klikbare
            for col in ['Merchant', 'Programnavn', 'Website']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: f"https://www.{str(x).lower().strip().replace(' ', '')}.dk" 
                                            if isinstance(x, str) and len(x) > 2 and '.' not in x and not str(x).startswith('http') else x)
            
            df = clean_values(df)
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
    if pd.isna(name) or name == "": return "unknown"
    name = str(name).lower().strip()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', name)
    return re.sub(r'[^a-z0-9]', '', name)

st.title("💼 Affiliate CRM Pro")

# --- SIDEPANEL ---
with st.sidebar:
    st.header("⚙️ CRM Indstillinger")
    
    if st.button("💣 NULSTIL ALT (Slet alt)", use_container_width=True):
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
    st.header("📥 Import / Sync")
    valgt_kategori = st.text_input("Kategori for denne fil:", "Bolig, Have og Interiør")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
    
    if uploaded_file:
        if st.button("Flet & Opdater CRM", use_container_width=True):
            file_ext = uploaded_file.name.split('.')[-1]
            new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
            new_data = clean_values(new_data)
            new_data['Kategori'] = valgt_kategori
            
            name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in new_data.columns), None)
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name_for_match)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    existing_name_col = next((c for c in ['Merchant', 'Programnavn'] if c in st.session_state.df.columns), name_col)
                    st.session_state.df['MATCH_KEY'] = st.session_state.df[existing_name_col].apply(clean_name_for_match)
                    
                    combined = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                    st.session_state.df = clean_values(combined)
                
                save_data(st.session_state.df)
                st.rerun()

    # --- KOLONNE STYRING ---
    if not st.session_state.df.empty:
        st.markdown("---")
        st.header("👁️ Visning")
        alle_kolonner = list(st.session_state.df.columns)
        if 'MATCH_KEY' in alle_kolonner: alle_kolonner.remove('MATCH_KEY')
        
        # Her kan du bestemme rækkefølgen manuelt
        default_order = ['Land', 'Merchant', 'Kategori', 'Status', 'Product Count', 'EPC (nøgletal)', 'Mail', 'Kontaktet']
        # Find ud af hvilke af de ønskede kolonner der faktisk findes
        actual_default = [c for c in default_order if c in alle_kolonner]
        
        valgte_kolonner = st.multiselect("Vælg/sorter kolonner:", alle_kolonner, default=actual_default)

# --- CRM HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg efter alt...", "")
    
    # Filtrer data
    df_to_show = st.session_state.df.copy()
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    # Kolonne Konfiguration
    column_config = {
        "Land": st.column_config.TextColumn("Land", width="small", pinned=True),
        "Merchant": st.column_config.LinkColumn("Website", pinned=True),
        "Programnavn": st.column_config.LinkColumn("Website"),
    }

    # Sortering og Rækkefølge
    st.write(f"Antal: **{len(df_to_show)}**")
    
    # Brug kun de valgte kolonner fra sidebaren
    if valgte_kolonner:
        df_to_show = df_to_show[valgte_kolonner]

    edited_df = st.data_editor(
        df_to_show,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=650,
        key="stable_crm_editor"
    )

    # Gem ændringer tilbage i session
    if not edited_df.equals(df_to_show):
        st.session_state.df.update(edited_df)
else:
    st.info("👋 Upload en fil for at starte dit CRM.")
