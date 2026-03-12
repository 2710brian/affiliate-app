import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text

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
        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
        if pd.isna(dt): return str(val)
        return dt.strftime('%d/%m/%Y')
    except:
        return str(val)

def final_clean(df):
    if df.empty: return df
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0'], '')
    for col in ['Date Added', 'Dato', 'Kontaktet']:
        if col in df.columns:
            df[col] = df[col].apply(format_date_clean)
    if 'Produkter' in df.columns:
        df['Produkter'] = pd.to_numeric(df['Produkter'].str.replace(r'[^0-9]', '', regex=True), errors='coerce').fillna(0).astype(int)
    if 'EPC' in df.columns:
        df['EPC'] = pd.to_numeric(df['EPC'].str.replace(',', '.'), errors='coerce').fillna(0.0)
    
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
@st.dialog("✏️ Hurtig-Redigering")
def edit_popup(index):
    row = st.session_state.df.iloc[index].copy()
    st.subheader(f"Kunde: {row['Virksomhed']}")
    
    col1, col2 = st.columns(2)
    with col1:
        new_fornavn = st.text_input("Fornavn", value=row.get('Fornavn', ''))
        new_efternavn = st.text_input("Efternavn", value=row.get('Efternavn', ''))
        new_mail = st.text_input("E-mail", value=row.get('Mail', ''))
    with col2:
        new_tlf = st.text_input("Tlf", value=row.get('Tlf', ''))
        new_status = st.selectbox("Status", ["Ikke ansøgt", "Godkendt", "Afvist", "Lead oprettet", "Kontaktet"], 
                                  index=["Ikke ansøgt", "Godkendt", "Afvist", "Lead oprettet", "Kontaktet"].index(row.get('Status', 'Ikke ansøgt')) if row.get('Status') in ["Ikke ansøgt", "Godkendt", "Afvist", "Lead oprettet", "Kontaktet"] else 0)
        # Dato vælger
        try:
            current_date = pd.to_datetime(row.get('Kontaktet', ''), dayfirst=True)
            if pd.isna(current_date): current_date = None
        except: current_date = None
        new_kontakt_dato = st.date_input("Kontaktet Dato", value=current_date)

    new_noter = st.text_area("Kontakt-log / Noter", value=row.get('Kontaktet', '') if current_date is None else "")

    if st.button("💾 Gem ændringer nu", type="primary", use_container_width=True):
        st.session_state.df.at[index, 'Fornavn'] = new_fornavn
        st.session_state.df.at[index, 'Efternavn'] = new_efternavn
        st.session_state.df.at[index, 'Mail'] = new_mail
        st.session_state.df.at[index, 'Tlf'] = new_tlf
        st.session_state.df.at[index, 'Status'] = new_status
        if new_kontakt_dato:
            st.session_state.df.at[index, 'Kontaktet'] = new_kontakt_dato.strftime('%d/%m/%Y')
        
        if save_data(st.session_state.df):
            st.success("Gemt i databasen!")
            st.rerun()

# --- UI START ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("⚙️ Kontrolcenter")
    
    if st.button("💾 GEM TABEL-RETTELSER", type="primary", use_container_width=True):
        if save_data(st.session_state.df):
            st.success("Alt er gemt!")
            st.rerun()

    st.markdown("---")
    if not st.session_state.df.empty:
        st.subheader("📊 Sortering")
        sort_col = st.selectbox("Sortér efter:", ["Dato Added", "Produkter", "EPC", "Status", "Trafik"])
        sort_asc = st.radio("Rækkefølge:", ["Nyeste/Højeste", "Ældste/Laveste"])
        if st.button("Udfør Sortering"):
            st.session_state.df = st.session_state.df.sort_values(sort_col, ascending=(sort_asc == "Ældste/Laveste"))
            save_data(st.session_state.df)
            st.rerun()

    st.markdown("---")
    st.subheader("📤 Eksport")
    st.download_button("📥 Master CSV", st.session_state.df.to_csv(index=False), "crm_full.csv", use_container_width=True)

# --- CRM HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg efter annoncør...", "")
    
    display_df = st.session_state.df.copy()
    if search:
        mask = display_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        display_df = display_df[mask]

    # --- HURTIG-REDIGERING LOGIK ---
    st.write(f"Antal rækker: **{len(display_df)}**")
    st.info("💡 Marker en række i tabellen for at åbne hurtig-redigering.")

    # TABEL MED SELECTION
    event = st.dataframe(
        display_df,
        column_config={
            "Website": st.column_config.LinkColumn("Website"),
            "MATCH_KEY": None
        },
        use_container_width=True,
        hide_index=False,
        on_select="rerun",
        selection_mode="single_row"
    )

    # Hvis brugeren vælger en række, vis redigerings-knap
    if event.selection.rows:
        selected_index = display_df.index[event.selection.rows[0]]
        if st.button(f"✏️ Åbn hurtig-redigering for {display_df.loc[selected_index, 'Virksomhed']}", type="primary"):
            edit_popup(selected_index)

else:
    st.info("👋 Upload din database for at starte.")
