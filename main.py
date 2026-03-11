import streamlit as st
import pandas as pd

st.set_page_config(page_title="Affiliate Lead Maskine", layout="wide")

if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()

st.title("🚀 Affiliate Lead Maskine")

# --- MENU ---
with st.sidebar:
    st.header("Importer Data")
    uploaded_file = st.file_uploader("Upload CSV eller Excel", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_ext = uploaded_file.name.split('.')[-1]
        # Læs filen
        if file_ext == 'csv':
            new_data = pd.read_csv(uploaded_file)
        else:
            new_data = pd.read_excel(uploaded_file)
            
        if st.button("Opdater Database"):
            # Find ud af hvad navne-kolonnen hedder i den nye fil
            # Vi tjekker de mest almindelige navne fra dine filer
            potential_names = ['Merchant', 'Programnavn', 'Annoncør']
            new_name_col = next((c for c in potential_names if c in new_data.columns), None)
            
            if not new_name_col:
                st.error("Kunne ikke finde en kolonne med navne (Merchant eller Programnavn)")
            else:
                if st.session_state.df.empty:
                    st.session_state.df = new_data
                    # Lav en standard 'ID' kolonne baseret på navnet til mapping
                    st.session_state.df['ID_KEY'] = st.session_state.df[new_name_col].astype(str).str.lower().str.strip()
                else:
                    # Gør det samme for den nye data
                    new_data['ID_KEY'] = new_data[new_name_col].astype(str).str.lower().str.strip()
                    
                    # Sæt ID_KEY som index for at kunne opdatere præcist
                    st.session_state.df.set_index('ID_KEY', inplace=True)
                    new_data.set_index('ID_KEY', inplace=True)
                    
                    # MAGIEN: combine_first tager eksisterende data og udfylder huller fra ny data.
                    # update tager ny data og overskriver de felter i den gamle data, der er ændret.
                    st.session_state.df.update(new_data) # Opdater eksisterende info
                    st.session_state.df = st.session_state.df.combine_first(new_data) # Tilføj nye rækker og kolonner
                    
                    # Ryd op i index så det bliver en normal tabel igen
                    st.session_state.df.reset_index(inplace=True)
                
                st.success("Databasen er opdateret og huller er udfyldt!")

# --- VISNING ---
if not st.session_state.df.empty:
    # Søgning
    search = st.text_input("Søg i din samlede database (navn, kategori, EPC...)", "")
    
    df_to_show = st.session_state.df
    if search:
        # Søg på tværs af alle kolonner
        mask = df_to_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_to_show = df_to_show[mask]
    
    # Vis som tabel
    st.dataframe(df_to_show, use_container_width=True)
    
    st.write(f"Antal annoncører i alt: {len(st.session_state.df)}")
    
    # DOWNLOAD KNAP
    csv = st.session_state.df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download din Master-Database", csv, "affiliate_master_db.csv", "text/csv")
else:
    st.info("Upload din første fil for at starte (f.eks. den store liste med 1062 annoncører).")
