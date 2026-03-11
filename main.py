import streamlit as st
import pandas as pd
import re
import os

st.set_page_config(page_title="Affiliate Lead Maskine", layout="wide")

# --- DATA-LAGRING (Hukommelse) ---
DB_FILE = "master_database.csv"

def load_data():
    if os.path.exists(DB_FILE):
        return pd.read_csv(DB_FILE)
    return pd.DataFrame()

def save_data(df):
    df.to_csv(DB_FILE, index=False)

if 'df' not in st.session_state:
    st.session_state.df = load_data()

# Hjælpefunktion til at rense navne
def clean_name(name):
    if pd.isna(name): return ""
    name = str(name).lower()
    name = re.sub(r'\.dk|\.se|\.no|\.com|aps|a/s|as|dk|se|no', '', name)
    return re.sub(r'[^a-z0-9]', '', name).strip()

st.title("🚀 Affiliate Lead Maskine")

# --- SIDEPANEL: IMPORT & INDSTILLINGER ---
with st.sidebar:
    st.header("Indstillinger")
    
    # 1. VISNING ANTAL
    items_per_page = st.selectbox("Annoncører pr. side", [25, 50, 100], index=0)
    
    st.markdown("---")
    st.header("Importer Data")
    uploaded_file = st.file_uploader("Upload fil", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        new_data = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
        
        if st.button("Opdater & Samkør"):
            name_col = next((c for c in ['Merchant', 'Programnavn', 'Annoncør'] if c in new_data.columns), None)
            if name_col:
                new_data['MATCH_KEY'] = new_data[name_col].apply(clean_name)
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                else:
                    st.session_state.df['MATCH_KEY'] = st.session_state.df.get('MATCH_KEY', st.session_state.df[next((c for c in ['Merchant', 'Programnavn'] if c in st.session_state.df.columns))].apply(clean_name))
                    st.session_state.df.set_index('MATCH_KEY', inplace=True)
                    new_data.set_index('MATCH_KEY', inplace=True)
                    st.session_state.df.update(new_data)
                    st.session_state.df = st.session_state.df.combine_first(new_data)
                    st.session_state.df.reset_index(inplace=True)
                
                save_data(st.session_state.df)
                st.success("Data gemt permanent!")

    if st.button("Ryd Database"):
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        st.session_state.df = pd.DataFrame()
        st.rerun()

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    # Søgning
    search = st.text_input("🔍 Søg i alt...", "")
    
    df_to_show = st.session_state.df.copy()
    if search:
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]

    # 2. PAGINERING LOGIK
    total_items = len(df_to_show)
    num_pages = (total_items // items_per_page) + (1 if total_items % items_per_page > 0 else 0)
    
    page = st.number_input("Side", min_value=1, max_value=max(1, num_pages), step=1)
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    
    st.write(f"Viser {start_idx+1}-{min(end_idx, total_items)} af {total_items} annoncører")

    # 3. LEAD KORT (VISNING)
    subset = df_to_show.iloc[start_idx:end_idx]

    for index, row in subset.iterrows():
        # Find navn og vigtigste info
        name = row.get('Merchant', row.get('Programnavn', 'Ukendt'))
        
        with st.container(border=True):
            col1, col2, col3 = st.columns([2, 3, 1])
            
            with col1:
                st.subheader(name)
                st.caption(f"Netværk: {row.get('Network', 'N/A')}")
                st.write(f"📦 Produkter: {row.get('Product Count', '0')}")
                st.write(f"📈 EPC: {row.get('EPC (nøgletal)', 'N/A')}")

            with col2:
                # REDIGERING AF LEADS
                new_mail = st.text_input("E-mail", value=str(row.get('Mail', '')), key=f"mail_{index}")
                new_note = st.text_area("Kontakt-log", value=str(row.get('Kontaktet', '')), key=f"note_{index}", height=68)
                
                if st.button("Gem Lead Info", key=f"save_{index}"):
                    st.session_state.df.at[index, 'Mail'] = new_mail
                    st.session_state.df.at[index, 'Kontaktet'] = new_note
                    save_data(st.session_state.df)
                    st.toast(f"Gemt: {name}")

            with col3:
                st.write("Handlinger")
                if st.button("🗑️ Slet", key=f"del_{index}"):
                    st.session_state.df = st.session_state.df.drop(index)
                    save_data(st.session_state.df)
                    st.rerun()

else:
    st.info("Upload en fil for at komme i gang.")
