import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime

# --- KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide")

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

# --- DEFINER DIN MASTER RÆKKEFØLGE ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Status', 'Dato', 'Network', 'Land', 
    'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

DIALOG_OPTIONS = [
    "Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", 
    "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", 
    "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", 
    "Droppet", "Call"
]

# --- HJÆLPEFUNKTIONER (FIXET FOR FEJL) ---
def final_clean_and_sort(df):
    if df.empty: return df
    
    # Omdøb kendte felter
    rename_map = {'Merchant': 'Virksomhed', 'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC'}
    df = df.rename(columns=rename_map)
    
    # TVING alt til tekst før rens for at undgå AttributeError
    df = df.astype(str)
    df = df.replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')
    
    # Sikr at alle kolonner findes
    for c in MASTER_COLS:
        if c not in df.columns: df[c] = ""
    
    return df[MASTER_COLS]

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return final_clean_and_sort(df)
        except: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = final_clean_and_sort(df)
            # Lav unikt match-id baseret på virksomhedsnavn
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl ved gem: {e}")
            return False
    return False

# --- SESSION STATE ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- POP-UP KLIENT KORT ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
def client_card(real_df_index):
    # Hent data for den valgte række
    row = st.session_state.df.loc[real_df_index].to_dict()
    
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    tab1, tab2 = st.tabs(["📋 Stamdata & Dialog", "📁 Dokumenter & Filer"])
    
    updates = {}

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📞 Kontaktinformation")
            updates['Fornavn'] = st.text_input("Fornavn", value=row.get('Fornavn', ''))
            updates['Efternavn'] = st.text_input("Efternavn", value=row.get('Efternavn', ''))
            updates['Mail'] = st.text_input("E-mail adresse", value=row.get('Mail', ''))
            updates['Tlf'] = st.text_input("Direkte telefon", value=row.get('Tlf', ''))
            updates['Website'] = st.text_input("Hjemmeside URL", value=row.get('Website', ''))
        
        with col2:
            st.markdown("#### ⚙️ Pipeline & Status")
            current_dialog = row.get('Dialog', 'Ikke kontakte')
            d_idx = DIALOG_OPTIONS.index(current_dialog) if current_dialog in DIALOG_OPTIONS else 0
            updates['Dialog'] = st.selectbox("Dialog / Opfølgning", DIALOG_OPTIONS, index=d_idx)
            
            updates['Ticketnr'] = st.text_input("Ticket nr. #", value=row.get('Ticketnr', ''))
            updates['Opflg. dato'] = st.text_input("Næste opfølgningsdato", value=row.get('Opflg. dato', ''))
            
            status_opts = ["Ikke ansøgt", "Godkendt", "Afvist", "Kontaktet"]
            curr_status = row.get('Status', 'Ikke ansøgt')
            s_idx = status_opts.index(curr_status) if curr_status in status_opts else 0
            updates['Status'] = st.selectbox("Netværk Status", status_opts, index=s_idx)

        st.divider()
        st.markdown("#### 📓 Interne Noter (Logbog)")
        # HER KAN DU SKRIVE DINE NOTER
        updates['Noter'] = st.text_area("Skriv vigtig info om kunden her...", value=row.get('Noter', ''), height=200)

    with tab2:
        st.subheader("📎 Vedhæftede filer")
        if row.get('Fil_Navn'):
            st.success(f"📂 Aktuel fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                # Muliggør download af eksisterende fil
                b64 = row['Fil_Data']
                href = f'<a href="data:application/octet-stream;base64,{b64}" download="{row["Fil_Navn"]}">👉 Klik her for at downloade/se filen</a>'
                st.markdown(href, unsafe_allow_html=True)
        
        st.markdown("---")
        new_file = st.file_uploader("Upload nyt dokument (PDF, Billede, Lyd, Video)", type=['pdf', 'png', 'jpg', 'mp3', 'mp4', 'wav', 'docx'])
        if new_file:
            updates['Fil_Navn'] = new_file.name
            updates['Fil_Data'] = base64.b64encode(new_file.read()).decode()
            st.info("Ny fil er klar til at blive gemt.")

    if st.button("💾 GEM ALLE ÆNDRINGER PÅ KLIENT", type="primary", use_container_width=True):
        for k, v in updates.items():
            st.session_state.df.at[real_df_index, k] = v
        if save_data(st.session_state.df):
            st.rerun()

# --- UI START ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("📤 Eksport")
    if not st.session_state.df.empty:
        st.download_button("📥 Download Master (CSV)", st.session_state.df.to_csv(index=False), "crm_master.csv", use_container_width=True)
        if 'filtered_df' in st.session_state:
            st.download_button("📥 Download Udvalgte", st.session_state.filtered_df.to_csv(index=False), "udvalgte_leads.csv", use_container_width=True)

    st.markdown("---")
    st.header("📥 Import")
    kat_name = st.text_input("Kategori for upload:", "Bolig, Have og Interiør")
    f = st.file_uploader("Vælg fil (CSV eller Excel)")
    if f and st.button("Flet & Gem data"):
        file_ext = f.name.split('.')[-1]
        new_data = pd.read_csv(f) if file_ext == 'csv' else pd.read_excel(f)
        new_data['Kategori'] = kat_name
        # Flet med eksisterende
        st.session_state.df = final_clean_and_sort(pd.concat([st.session_state.df, new_data], ignore_index=True))
        save_data(st.session_state.df)
        st.rerun()

    st.markdown("---")
    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS merchants"))
                conn.commit()
        st.session_state.df = pd.DataFrame()
        st.rerun()

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", placeholder="Søg på tværs af alle felter...")
    
    display_df = st.session_state.df.copy()
    if search:
        mask = display_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        display_df = display_df[mask]
    
    st.session_state.filtered_df = display_df
    
    st.subheader(f"Antal annoncører: {len(display_df)}")
    st.info("💡 Klik på boksen helt til venstre for en række for at åbne Klient-kortet.")

    # Vis tabellen uden de tunge fildata
    view_cols = [c for c in display_df.columns if c != 'Fil_Data']
    
    sel = st.dataframe(
        display_df[view_cols],
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True,
        selection_mode="single-row",
        on_select="rerun",
        height=600
    )

    # AUTO-POPUP NÅR EN RÆKKE VÆLGES
    if sel.selection.rows:
        row_idx = sel.selection.rows[0]
        real_idx = display_df.index[row_idx]
        client_card(real_idx)
else:
    st.info("👋 Databasen er tom. Upload en fil for at starte dit CRM.")
