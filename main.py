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

# --- INTELLIGENT RENSNING OG TYPE-KONVERTERING ---
def optimize_dtypes(df):
    if df.empty:
        return df
    
    # 1. Erstat tekniske fejl-ord med rigtige tomme værdier
    df = df.replace(['NaT', 'nan', 'None', '<NA>', 'nan.0'], '')
    
    # 2. Gør "Product Count" til tal (fjern tusindtal-separatorer)
    if 'Product Count' in df.columns:
        df['Product Count'] = df['Product Count'].astype(str).str.replace(r'[,.]', '', regex=True)
        df['Product Count'] = pd.to_numeric(df['Product Count'], errors='coerce').fillna(0).astype(int)
    
    # 3. Gør "EPC" til kommatal
    if 'EPC (nøgletal)' in df.columns:
        df['EPC (nøgletal)'] = df['EPC (nøgletal)'].astype(str).str.replace(',', '.')
        df['EPC (nøgletal)'] = pd.to_numeric(df['EPC (nøgletal)'], errors='coerce').fillna(0.0)
        
    return df

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return optimize_dtypes(df)
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
            
            # Gem i databasen
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
    return False

if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- HJÆLPEFUNKTIONER ---
def clean_name_for_match(name):
    if pd.isna(name) or name == "": return "unknown_" + os.urandom(4).hex()
    name = str(name).lower().strip()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as', '', name)
    return re.sub(r'[^a-z0-9]', '', name)

st.title("💼 Affiliate CRM Pro")

# --- SIDEPANEL ---
with st.sidebar:
    st.header("⚙️ CRM Indstillinger")
    
    if st.button("💾 GEM ALLE RETTELSER", type="primary", use_container_width=True):
        if save_data(st.session_state.df):
            st.success("Gemt permanent!")
            st.rerun()

    st.markdown("---")
    st.header("📥 Import")
    valgt_kategori = st.text_input("Kategori:", "Bolig, Have og Interiør")
    uploaded_file = st.file_uploader("Upload fil", type=['csv', 'xlsx'])
    
    if uploaded_file:
        if st.button("Flet data ind i CRM", use_container_width=True):
            file_ext = uploaded_file.name.split('.')[-1]
            new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
            new_data = optimize_dtypes(new_data)
            new_data['Kategori'] = valgt_kategori
            
            name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in new_data.columns), None)
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name_for_match)
                
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    st.session_state.df['MATCH_KEY'] = st.session_state.df[next((c for c in ['Merchant', 'Programnavn'] if c in st.session_state.df.columns), name_col)].apply(clean_name_for_match)
                    # Merge logik
                    existing = st.session_state.df.set_index('MATCH_KEY')
                    incoming = new_data.set_index('MATCH_KEY')
                    existing.update(incoming[['Product Count', 'EPC (nøgletal)']].filter(items=incoming.columns))
                    st.session_state.df = existing.combine_first(incoming).reset_index()
                
                save_data(st.session_state.df)
                st.rerun()

    st.markdown("---")
    if st.button("🚨 Slet alt"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants"))
                conn.commit()
        st.session_state.df = pd.DataFrame()
        st.rerun()

# --- CRM TABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg (navn, mail, kategori...)", "")
    
    df_to_show = st.session_state.df.copy()
    
    # Kolonne-orden
    all_cols = list(df_to_show.columns)
    important = [c for c in ['Land', 'Merchant', 'Kategori', 'Status', 'Product Count', 'EPC (nøgletal)', 'Mail', 'Kontaktet'] if c in all_cols]
    others = [c for c in all_cols if c not in important and c != 'MATCH_KEY']
    df_to_show = df_to_show[important + others]

    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    st.write(f"Antal rækker: **{len(df_to_show)}**")

    # TABEL-KONFIGURATION (Her muliggøres rigtig sortering)
    edited_df = st.data_editor(
        df_to_show,
        column_config={
            "Land": st.column_config.TextColumn("Land", width="small", pinned=True),
            "Merchant": st.column_config.LinkColumn("Website", pinned=True),
            "Product Count": st.column_config.NumberColumn("Produkter", format="%d"),
            "EPC (nøgletal)": st.column_config.NumberColumn("EPC", format="%.2f"),
        },
        use_container_width=True,
        num_rows="dynamic",
        height=700,
        key="pro_crm_final_v8"
    )

    if not edited_df.equals(df_to_show):
        st.session_state.df.update(edited_df)
else:
    st.info("👋 Databasen er tom.")
