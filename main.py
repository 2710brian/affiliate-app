import streamlit as st
import pandas as pd
import re
import os

st.set_page_config(page_title="Affiliate Lead Maskine", layout="wide")

# --- DATA-LAGRING ---
DB_FILE = "master_database.csv"

def load_data():
    if os.path.exists(DB_FILE):
        df = pd.read_csv(DB_FILE)
        return df.fillna("")
    return pd.DataFrame()

def save_data(df):
    df.to_csv(DB_FILE, index=False)

if 'df' not in st.session_state:
    st.session_state.df = load_data()

def clean_name(name):
    if pd.isna(name): return ""
    name = str(name).lower()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', name)
    return re.sub(r'[^a-z0-9]', '', name).strip()

st.title("🚀 Affiliate Master Database")

# --- SIDEPANEL: IMPORT OG EKSPORT ---
with st.sidebar:
    st.header("Importer Data")
    uploaded_file = st.file_uploader("Upload CSV eller Excel", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
        new_data = new_data.fillna("")
        
        if st.button("Opdater & Samkør"):
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    if 'MATCH_KEY' not in st.session_state.df.columns:
                        ex_name_col = next((c for c in potential_names if c in st.session_state.df.columns), name_col)
                        st.session_state.df['MATCH_KEY'] = st.session_state.df[ex_name_col].apply(clean_name)
                    
                    st.session_state.df = st.session_state.df.set_index('MATCH_KEY').combine_first(new_data.set_index('MATCH_KEY')).reset_index()
                
                save_data(st.session_state.df)
                st.success("Database opdateret!")

    st.markdown("---")
    st.header("Eksport")
    if not st.session_state.df.empty:
        csv_data = st.session_state.df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Master CSV",
            data=csv_data,
            file_name="affiliate_master_export.csv",
            mime="text/csv",
        )

    if st.button("🚨 Ryd alt"):
        st.session_state.df = pd.DataFrame()
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        st.rerun()

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg efter navn, EPC, kategori...", "")
    
    df_to_show = st.session_state.df.copy()
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    if 'MATCH_KEY' in df_to_show.columns:
        df_to_show = df_to_show.drop(columns=['MATCH_KEY'])

    st.write(f"Antal rækker: **{len(df_to_show)}**")
    st.info("💡 Du kan rette direkte i cellerne herunder. Husk at trykke på 'Gem rettelser' knappen nederst.")

    # Editerbar tabel
    edited_df = st.data_editor(
        df_to_show,
        use_container_width=True,
        num_rows="dynamic",
        height=500
    )

    if st.button("💾 Gem rettelser i databasen"):
        # Vi sikrer os at MATCH_KEY kommer med tilbage hvis den findes
        if not st.session_state.df.empty and 'MATCH_KEY' in st.session_state.df.columns:
            # Vi genbruger MATCH_KEY fra den oprindelige dataframe baseret på Merchant navnet
            # Dette er en sikkerhed så samkøringen stadig virker efter redigering
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            name_col = next((c for c in potential_names if c in edited_df.columns), None)
            if name_col:
                edited_df['MATCH_KEY'] = edited_df[name_col].apply(clean_name)
        
        st.session_state.df = edited_df
        save_data(edited_df)
        st.success("Alle ændringer gemt!")
else:
    st.info("Upload din liste i venstre side for at se databasen.")
