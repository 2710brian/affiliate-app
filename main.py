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

# --- RENSE- OG TYPE-MOTOR (Vigtig for sortering) ---
def clean_and_fix_types(df):
    if df.empty:
        return df
    
    # Fjern tekniske fejl-ord
    df = df.replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'unknown'], '')
    
    # Gør Product Count til tal (fjern punktum/komma først)
    if 'Product Count' in df.columns:
        df['Product Count'] = df['Product Count'].astype(str).str.replace(r'[,.]', '', regex=True)
        df['Product Count'] = pd.to_numeric(df['Product Count'], errors='coerce').fillna(0).astype(int)
    
    # Gør EPC til tal
    if 'EPC (nøgletal)' in df.columns:
        df['EPC (nøgletal)'] = df['EPC (nøgletal)'].astype(str).str.replace(',', '.')
        df['EPC (nøgletal)'] = pd.to_numeric(df['EPC (nøgletal)'], errors='coerce').fillna(0.0)

    # Rens alle andre kolonner for 'None' tekst
    for col in df.columns:
        df[col] = df[col].astype(str).replace('None', '')
        
    return df

# --- HJÆLPEFUNKTIONER ---
def clean_name_for_match(name):
    if not name or name == "" or name == "None": return "temp_" + os.urandom(4).hex()
    name = str(name).lower().strip()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', name)
    return re.sub(r'[^a-z0-9]', '', name)

def generate_website_link(name):
    if not name or name == "" or "http" in str(name): return name
    clean = str(name).lower().strip().replace(" ", "").replace(".dk", "").replace(".se", "")
    clean = re.sub(r'[^a-z0-9]', '', clean)
    return f"https://www.{clean}.dk" if clean else ""

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return clean_and_fix_types(df)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = clean_and_fix_types(df)
            # Slet dubletter før gem for at undgå 'Duplicate labels' fejl
            if 'MATCH_KEY' in df.columns:
                df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
    return False

# --- SESSION STATE ---
if 'df' not in st.session_state or st.session_state.df is None:
    st.session_state.df = load_data()

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
                st.success("Alt er gemt!")
                st.rerun()

    st.markdown("---")
    st.header("📥 Import / Sync")
    valgt_kategori = st.text_input("Kategori for denne fil:", "Bolig, Have og Interiør")
    uploaded_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
    
    if uploaded_file:
        if st.button("Flet & Opdater CRM", use_container_width=True):
            file_ext = uploaded_file.name.split('.')[-1]
            new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
            new_data = clean_and_fix_types(new_data)
            
            name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in new_data.columns), None)
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name_for_match)
                new_data['Kategori'] = valgt_kategori
                # Lav links hvis de mangler
                if 'Website' not in new_data.columns:
                    new_data['Website'] = new_data[name_col].apply(generate_website_link)
                
                # Fjern dubletter i den indkommende fil før merge
                new_data = new_data.drop_duplicates('MATCH_KEY')

                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    # Gør eksisterende data klar
                    ex_name_col = next((c for c in ['Merchant', 'Programnavn'] if c in st.session_state.df.columns), name_col)
                    st.session_state.df['MATCH_KEY'] = st.session_state.df[ex_name_col].apply(clean_name_for_match)
                    # Fjern eksisterende dubletter for at undgå crashet
                    st.session_state.df = st.session_state.df.drop_duplicates('MATCH_KEY')
                    
                    existing = st.session_state.df.set_index('MATCH_KEY')
                    incoming = new_data.set_index('MATCH_KEY')
                    
                    # Opdater kun tal og status
                    cols_to_upd = [c for c in incoming.columns if c in ['Product Count', 'EPC (nøgletal)', 'Status', 'Kategori', 'Website']]
                    existing.update(incoming[cols_to_upd])
                    
                    # Flet resten
                    st.session_state.df = existing.combine_first(incoming).reset_index()
                
                save_data(st.session_state.df)
                st.rerun()

    # --- KOLONNE VISNING ---
    if not st.session_state.df.empty:
        st.markdown("---")
        st.header("👁️ Visning")
        alle_cols = [c for c in list(st.session_state.df.columns) if c != 'MATCH_KEY']
        order = ['Merchant', 'Website', 'Kategori', 'Status', 'Product Count', 'EPC (nøgletal)', 'Mail', 'Kontaktet']
        default_order = [c for c in order if c in alle_cols] + [c for c in alle_cols if c not in order]
        valgte_cols = st.multiselect("Vælg rækkefølge:", alle_cols, default=default_order)

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    
    df_to_show = st.session_state.df.copy()
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    # Brug kun valgte kolonner
    if valgte_cols:
        df_to_show = df_to_show[valgte_cols]

    st.write(f"Antal unikke annoncører: **{len(df_to_show)}**")

    # EDITERBAR TABEL
    # ColumnConfig sørger for rigtige typer til sortering
    edited_df = st.data_editor(
        df_to_show,
        column_config={
            "Merchant": st.column_config.TextColumn("Navn", pinned=True),
            "Website": st.column_config.LinkColumn("Website", pinned=True),
            "Product Count": st.column_config.NumberColumn("Produkter", format="%d"),
            "EPC (nøgletal)": st.column_config.NumberColumn("EPC", format="%.2f"),
        },
        use_container_width=True,
        num_rows="dynamic",
        height=700,
        key="crm_final_fixed_v11"
    )

    if not edited_df.equals(df_to_show):
        st.session_state.df.update(edited_df)
else:
    st.info("👋 Upload en fil for at starte dit CRM.")
