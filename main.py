import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
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

# --- HJÆLPEFUNKTIONER ---
def format_date_clean(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '']:
        return ""
    try:
        # Håndterer forskellige inputformater og spytter DD/MM/YYYY ud
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        if pd.isna(dt): return str(val)
        return dt.strftime('%d/%m/%Y')
    except:
        return str(val)

def final_clean(df):
    if df.empty: return df
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0'], '')
    
    # Rens datoer
    for col in ['Date Added', 'Dato', 'Kontaktet']:
        if col in df.columns:
            df[col] = df[col].apply(format_date_clean)
            
    # Rens tal til sortering
    if 'Produkter' in df.columns:
        df['Produkter'] = pd.to_numeric(df['Produkter'].str.replace(r'[^0-9]', '', regex=True), errors='coerce').fillna(0).astype(int)
    if 'EPC' in df.columns:
        df['EPC'] = pd.to_numeric(df['EPC'].str.replace(',', '.'), errors='coerce').fillna(0.0)
    
    # TVING DIN RÆKKEFØLGE (1-21)
    ønsket_orden = [
        'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
        'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
        'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
        'Status', 'Dato', 'Network', 'Land'
    ]
    eksisterende_orden = [c for c in ønsket_orden if c in df.columns]
    resten = [c for c in df.columns if c not in eksisterende_orden and c != 'MATCH_KEY']
    return df[eksisterende_orden + resten]

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return final_clean(df)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    if db_engine:
        try:
            df = final_clean(df)
            if 'MATCH_KEY' in df.columns:
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

# --- POP-UP DIALOG (HURTIG REDIGERING) ---
@st.dialog("✏️ Hurtig-Redigering af Lead")
def edit_popup(row_index):
    # Hent den aktuelle række fra dataframe
    row_data = st.session_state.df.iloc[row_index].to_dict()
    
    st.write(f"### {row_data.get('Virksomhed', 'Ukendt kunde')}")
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        fnavn = st.text_input("Fornavn", value=row_data.get('Fornavn', ''))
        enavn = st.text_input("Efternavn", value=row_data.get('Efternavn', ''))
        mail = st.text_input("E-mail", value=row_data.get('Mail', ''))
        tlf = st.text_input("Telefon", value=row_data.get('Tlf', ''))
    
    with col2:
        status_options = ["Ikke ansøgt", "Godkendt", "Afvist", "Lead oprettet", "Kontaktet", "Afventer"]
        current_status = row_data.get('Status', 'Ikke ansøgt')
        status_idx = status_options.index(current_status) if current_status in status_options else 0
        status = st.selectbox("Status", status_options, index=status_idx)
        
        # Dato håndtering
        try:
            val_kontakt = row_data.get('Kontaktet', '')
            init_date = pd.to_datetime(val_kontakt, dayfirst=True).date() if val_kontakt else datetime.now().date()
        except:
            init_date = datetime.now().date()
            
        kontakt_dato = st.date_input("Kontaktet den (Dato)", value=init_date)
    
    noter = st.text_area("Yderligere noter", value=row_data.get('Kontaktet', '') if not kontakt_dato else "")

    if st.button("💾 Gem og luk", type="primary", use_container_width=True):
        # Opdater rækken i session state
        st.session_state.df.at[row_index, 'Fornavn'] = fnavn
        st.session_state.df.at[row_index, 'Efternavn'] = enavn
        st.session_state.df.at[row_index, 'Mail'] = mail
        st.session_state.df.at[row_index, 'Tlf'] = tlf
        st.session_state.df.at[row_index, 'Status'] = status
        st.session_state.df.at[row_index, 'Kontaktet'] = kontakt_dato.strftime('%d/%m/%Y')
        
        # Gem til databasen
        if save_data(st.session_state.df):
            st.toast("Lead opdateret!")
            st.rerun()

# --- UI START ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("⚙️ Kontrolcenter")
    
    if st.button("💾 GEM MANUELLE RETTELSER", type="primary", use_container_width=True):
        if save_data(st.session_state.df):
            st.success("Alt er gemt!")
            st.rerun()

    st.markdown("---")
    if not st.session_state.df.empty:
        st.subheader("📊 Sortering")
        sort_col = st.selectbox("Sortér efter:", ["Date Added", "Produkter", "EPC", "Status", "Trafik"])
        sort_asc = st.radio("Rækkefølge:", ["Nyeste/Højeste", "Ældste/Laveste"])
        if st.button("Sortér nu"):
            st.session_state.df = st.session_state.df.sort_values(sort_col, ascending=(sort_asc == "Ældste/Laveste"))
            save_data(st.session_state.df)
            st.rerun()

    st.markdown("---")
    st.subheader("📤 Eksport")
    st.download_button("📥 Master CSV", st.session_state.df.to_csv(index=False), "crm_master.csv", use_container_width=True)

# --- HOVEDVINDUE ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg efter annoncør...", placeholder="Skriv navn, mail eller kategori her...")
    
    display_df = st.session_state.df.copy()
    if search:
        mask = display_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        display_df = display_df[mask]

    st.write(f"Antal rækker fundet: **{len(display_df)}**")
    st.info("💡 Klik på den grå boks til venstre for en række for at vælge den, og åbn derefter pop-up'en.")

    # TABEL MED KORREKT SELECTION MODE
    selection = st.dataframe(
        display_df,
        column_config={
            "Website": st.column_config.LinkColumn("Website"),
            "MATCH_KEY": None
        },
        use_container_width=True,
        hide_index=False,
        on_select="rerun",
        selection_mode="single-row"  # HER VAR FEJLEN (skal være bindestreg)
    )

    # VIS REDIGERINGS-KNAP HVIS EN RÆKKE ER VALGT
    if len(selection.selection.rows) > 0:
        # Find det rigtige index i den oprindelige dataframe
        selected_row_index = selection.selection.rows[0]
        actual_df_index = display_df.index[selected_row_index]
        
        virksomhed_navn = display_df.iloc[selected_row_index]['Virksomhed']
        
        st.divider()
        if st.button(f"✏️ Rediger lead for: {virksomhed_navn}", type="primary", use_container_width=True):
            edit_popup(actual_df_index)

else:
    st.info("👋 Upload din database for at starte.")
