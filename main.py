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

# --- INTELLIGENT TYPE-KONVERTERING ---
def optimize_dtypes(df):
    if df.empty:
        return df
    
    # Standard rens for alle tekstfelter
    df = df.replace(['NaT', 'nan', 'None', '<NA>', 'nan.0'], '')
    
    # Intelligent konvertering af Product Count (hvis den findes)
    if 'Product Count' in df.columns:
        df['Product Count'] = df['Product Count'].astype(str).str.replace(r'[,.]', '', regex=True)
        df['Product Count'] = pd.to_numeric(df['Product Count'], errors='coerce').fillna(0).astype(int)
    
    # Intelligent konvertering af EPC (hvis den findes)
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
    
    if st.button("💣 NULSTIL ALT", use_container_width=True):
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
    st.header("📥 Import")
    valgt_kategori = st.text_input("Kategori for denne fil:", "Bolig, Have og Interiør")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
    
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
                    # Tjek for MATCH_KEY i eksisterende data
                    if 'MATCH_KEY' not in st.session_state.df.columns:
                        ex_name_col = next((c for c in ['Merchant', 'Programnavn'] if c in st.session_state.df.columns), name_col)
                        st.session_state.df['MATCH_KEY'] = st.session_state.df[ex_name_col].apply(clean_name_for_match)
                    
                    existing = st.session_state.df.set_index('MATCH_KEY')
                    incoming = new_data.set_index('MATCH_KEY')
                    
                    # FEJLSIKRING: Find kun kolonner der faktisk findes i den nye fil
                    cols_to_update = [c for c in incoming.columns if c in ['Product Count', 'EPC (nøgletal)', 'Status', 'Network']]
                    if cols_to_update:
                        existing.update(incoming[cols_to_update])
                    
                    st.session_state.df = existing.combine_first(incoming).reset_index()
                
                save_data(st.session_state.df)
                st.success("Flettet uden fejl!")
                st.rerun()

# --- CRM HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    
    df_to_show = st.session_state.df.copy()
    
    # Kolonne-orden
    all_cols = list(df_to_show.columns)
    important = [c for c in ['Merchant', 'Kategori', 'Status', 'Product Count', 'EPC (nøgletal)', 'Mail', 'Kontaktet'] if c in all_cols]
    others = [c for c in all_cols if c not in important and c != 'MATCH_KEY']
    df_to_show = df_to_show[important + others]

    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    # TABEL-KONFIGURATION
    column_config = {
        "Merchant": st.column_config.LinkColumn("Website", pinned=True),
        "Product Count": st.column_config.NumberColumn("Produkter", format="%d"),
        "EPC (nøgletal)": st.column_config.NumberColumn("EPC", format="%.2f"),
    }

    st.write(f"Antal rækker: **{len(df_to_show)}**")

    edited_df = st.data_editor(
        df_to_show,
        column_config=column_config,
        use_container_width=True,
        num_rows="dynamic",
        height=750,
        key="crm_final_v9"
    )

    if not edited_df.equals(df_to_show):
        st.session_state.df.update(edited_df)
else:
    st.info("👋 Upload din fil for at starte.")
