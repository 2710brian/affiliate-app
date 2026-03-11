import streamlit as st
import pandas as pd

# --- KONFIGURATION ---
st.set_page_config(page_title="Affiliate Lead Maskine", layout="wide")

# --- DATABASE LOGIK ---
# Vi bruger session_state til at holde på data, mens du er logget ind
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()

st.title("🚀 Affiliate Lead Maskine")

# --- UPLOAD SEKTION ---
st.sidebar.header("Importer Data")
uploaded_file = st.sidebar.file_saver = st.sidebar.file_uploader("Upload din CSV eller Excel fil", type=['csv', 'xlsx'])

if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            new_data = pd.read_csv(uploaded_file)
        else:
            new_data = pd.read_excel(uploaded_file)
        
        if st.sidebar.button("Indlæs data til maskinen"):
            st.session_state.df = new_data
            st.sidebar.success(f"Indlæst {len(new_data)} rækker!")
    except Exception as e:
        st.sidebar.error(f"Fejl ved indlæsning: {e}")

# --- DASHBOARD ---
if st.session_state.df.empty:
    st.info("👋 Velkommen! Start med at uploade din fil i menuen til venstre.")
else:
    # Søgning
    search_query = st.text_input("🔍 Søg efter annoncør...", "")
    
    filtered_df = st.session_state.df
    if search_query:
        # Vi søger i 'Merchant' eller 'Programnavn' alt efter hvad kolonnen hedder
        col_name = 'Merchant' if 'Merchant' in filtered_df.columns else 'Programnavn'
        filtered_df = filtered_df[filtered_df[col_name].astype(str).str.contains(search_query, case=False)]

    st.write(f"Viser **{len(filtered_df)}** annoncører")

    # Vis kort
    for index, row in filtered_df.iterrows():
        name = row['Merchant'] if 'Merchant' in row else row['Programnavn']
        with st.expander(f"🏢 {name}"):
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                st.write("**Info**")
                # Vi viser de kolonner der findes i din fil
                for col in ['Network', 'Kategori', 'Status', 'Product Count']:
                    if col in row: st.write(f"{col}: {row[col]}")
            
            with c2:
                st.write("**Lead Data**")
                # Her kan du rette (bemærk: gemmes kun i denne session indtil vi kobler rigtig database på)
                mail = st.text_input("E-mail", value=row.get('Mail', ''), key=f"mail_{index}")
                noter = st.text_area("Noter", value=row.get('Kontaktet', ''), key=f"note_{index}")

            with c3:
                if st.button("❌ Slet", key=f"del_{index}"):
                    st.session_state.df = st.session_state.df.drop(index)
                    st.rerun()

# --- EKSPORT ---
if not st.session_state.df.empty:
    st.sidebar.download_button("📥 Download opdateret liste", st.session_state.df.to_csv(index=False), "mine_leads.csv")
