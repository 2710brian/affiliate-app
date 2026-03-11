import streamlit as st
import pandas as pd
import re

st.set_page_config(page_title="Affiliate Lead Maskine", layout="wide")

if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()

# Hjælpefunktion til at rense navne (fjerner .dk, ApS, landekoder osv.)
def clean_name(name):
    if pd.isna(name): return ""
    name = str(name).lower()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', name)
    return re.sub(r'[^a-z0-9]', '', name).strip()

st.title("🚀 Affiliate Lead Maskine")

# --- MENU ---
with st.sidebar:
    st.header("Importer Data")
    uploaded_file = st.file_uploader("Upload CSV eller Excel", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
            
        if st.button("Opdater & Samkør"):
            # Identificer kolonner i den nye fil
            cols = new_data.columns
            name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in cols), None)
            mail_col = next((c for c in ['Mail', 'Email', 'E-mail'] if c in cols), None)
            mid_col = next((c for c in ['MID', 'Merchant ID'] if c in cols), None)

            if not name_col:
                st.error("Kunne ikke finde en navne-kolonne.")
            else:
                # Opret en unik 'MATCH_KEY' baseret på hvad vi har
                # Vi kombinerer rensede navne og emails for at skabe et sikkert match
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                if mail_col:
                    new_data['MATCH_KEY'] = new_data['MATCH_KEY'] + new_data[mail_col].fillna('').str.lower().str.strip()

                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    # Gør det samme for eksisterende data
                    ex_cols = st.session_state.df.columns
                    ex_name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in ex_cols), None)
                    ex_mail_col = next((c for c in ['Mail', 'Email', 'E-mail'] if c in ex_cols), None)
                    
                    st.session_state.df['MATCH_KEY'] = st.session_state.df[ex_name_col].apply(clean_name)
                    if ex_mail_col:
                        st.session_state.df['MATCH_KEY'] = st.session_state.df['MATCH_KEY'] + st.session_state.df[ex_mail_col].fillna('').str.lower().str.strip()

                    # Sæt MATCH_KEY som index for at opdatere
                    st.session_state.df.set_index('MATCH_KEY', inplace=True)
                    new_data.set_index('MATCH_KEY', inplace=True)
                    
                    # 1. Opdater eksisterende felter (hvis den nye fil har nyere info)
                    st.session_state.df.update(new_data)
                    # 2. Udfyld tomme felter og tilføj helt nye rækker
                    st.session_state.df = st.session_state.df.combine_first(new_data)
                    
                    st.session_state.df.reset_index(inplace=True)
                
                st.success(f"Databasen opdateret! Total antal: {len(st.session_state.df)}")

# --- VISNING ---
if not st.session_state.df.empty:
    search = st.text_input("Søg efter navn, email, kategori eller MID...", "")
    
    # Filtrering baseret på søgning
    df_to_show = st.session_state.df
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]
    
    # Vis data
    st.dataframe(df_to_show.drop(columns=['MATCH_KEY'], errors='ignore'), use_container_width=True)
    
    # Download
    csv = st.session_state.df.to_csv(index=False).encode('utf-8')
    st.sidebar.download_button("📥 Download Master-Database", csv, "affiliate_master_db.csv", "text/csv")
else:
    st.info("Upload din store liste for at starte.")
