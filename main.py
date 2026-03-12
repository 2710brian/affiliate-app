import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Master", layout="wide")

# --- 2. DATABASE FORBINDELSE ---
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            return create_engine(db_url, pool_pre_ping=True)
        except: return None
    return None

db_engine = get_engine()

# --- 3. MASTER STRUKTUR ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter'
]

DIALOG_OPTIONS = [
    "Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", 
    "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", 
    "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"
]

# --- 4. RENSE-MOTOR (Sikrer rækkefølge og fjerner % kolonner fra visning) ---
def clean_master_df(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    
    # Map navne fra CSV til CRM (Billede 2 & 3)
    rename_map = {
        'Programnavn': 'Virksomhed',
        'Merchant': 'Virksomhed', 
        'Product Count': 'Produkter', 
        'EPC (nøgletal)': 'EPC',
        'Aff. Status': 'Aff. status', # Billede 3 fix
        'DateAdded': 'Date Added',
        'Status': 'Aff. status',
        'Dato': 'Kontakt dato'
    }
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Rens alt til ren tekst
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')
    
    # Website Generator (Billede 1 fix)
    def site_gen(r):
        s = str(r.get('Website', '')).strip()
        v = str(r.get('Virksomhed', '')).strip()
        if (s == "" or s == "None" or s == "nan") and v != "":
            clean = re.sub(r'[^a-z0-9]', '', v.lower())
            return f"https://www.{clean}.dk" if clean else ""
        return s
    df['Website'] = df.apply(site_gen, axis=1)

    # Land/Flag
    def set_land(r):
        txt = " ".join([str(v) for v in r.values]).lower()
        if any(x in txt for x in ["denmark", "dk", "dansk"]): return "🇩🇰"
        if any(x in txt for x in ["sweden", "se", "svensk"]): return "🇸🇪"
        return "🌐"
    df['Land'] = df.apply(set_land, axis=1)

    return df.reindex(columns=MASTER_COLS, fill_value="")

def load_data():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return clean_master_df(df)
        except: return pd.DataFrame(columns=MASTER_COLS)
    return pd.DataFrame(columns=MASTER_COLS)

def save_data(df):
    if db_engine:
        try:
            df = clean_master_df(df)
            df['MATCH_KEY'] = df['Virksomhed'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True)
            df = df.drop_duplicates('MATCH_KEY', keep='first')
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Fejl: {e}")
            return False
    return False

# --- SESSION START ---
if 'df' not in st.session_state:
    st.session_state.df = load_data()

# --- POP-UP (CRM BOARD MED ALLE FELTER) ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["📊 CRM Data & Pipeline", "📁 Noter & Dokumenter"])
    upd = {}

    with t1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            upd['Fornavn'] = st.text_input("Fornavn", value=row.get('Fornavn'))
            upd['Efternavn'] = st.text_input("Efternavn", value=row.get('Efternavn'))
            upd['Mail'] = st.text_input("E-mail", value=row.get('Mail'))
            upd['Tlf'] = st.text_input("Tlf", value=row.get('Tlf'))
            upd['Website'] = st.text_input("Website", value=row.get('Website'))
        with c2:
            st.markdown("##### ⚙️ Status & Datoer")
            # STATUS 1: DIALOG (DROPDOWN)
            d_val = row.get('Dialog', 'Ikke kontakte')
            d_idx = DIALOG_OPTIONS.index(d_val) if d_val in DIALOG_OPTIONS else 0
            upd['Dialog'] = st.selectbox("Dialog Status", DIALOG_OPTIONS, index=d_idx)
            
            # KALENDER VÆLGERE
            try: d_o = pd.to_datetime(row.get('Opflg. dato'), dayfirst=True).date()
            except: d_o = date.today()
            upd['Opflg. dato'] = st.date_input("Opfølgningsdato", value=d_o).strftime('%d/%m/%Y')
            
            try: d_k = pd.to_datetime(row.get('Kontakt dato'), dayfirst=True).date()
            except: d_k = date.today()
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=d_k).strftime('%d/%m/%Y')
        
        with c3:
            st.markdown("##### 📈 Aff. Info")
            # STATUS 2: AFF STATUS
            upd['Aff. status'] = st.text_input("Aff. status", value=row.get('Aff. status'))
            upd['Kategori'] = st.text_input("Kategori", value=row.get('Kategori'))
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr'))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC'))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter'))

        st.divider()
        st.markdown("##### 📊 Øvrige Systemfelter")
        ca, cb, cc = st.columns(3)
        with ca:
            upd['MID'] = st.text_input("MID", value=row.get('MID'))
            upd['Programnavn'] = st.text_input("Programnavn", value=row.get('Programnavn'))
        with cb:
            upd['Segment'] = st.text_input("Segment", value=row.get('Segment'))
            upd['Salgs % (sats)'] = st.text_input("Salgs %", value=row.get('Salgs % (sats)'))
        with cc:
            upd['Trafik'] = st.text_input("Trafik", value=row.get('Trafik'))
            upd['Land'] = st.text_input("Land Ikon", value=row.get('Land'))

    with t2:
        upd['Noter'] = st.text_area("📓 Interne Noter om kunden", value=row.get('Noter'), height=250)
        st.file_uploader("Vedhæft fil (Lyd, PDF, Billede)", key=f"file_{idx}")

    if st.button("💾 GEM KLIENT KORT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_data(st.session_state.df): st.rerun()

# --- 7. UI ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("📤 Eksport")
    st.download_button("📥 Master (Alt)", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)
    if 'filtered' in st.session_state:
        st.download_button("📥 Valgte (Søgt)", st.session_state.filtered.to_csv(index=False), "valgte.csv", use_container_width=True)

    st.divider()
    st.header("📥 Import")
    kat_up = st.text_input("Kategori for upload:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Vælg fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd['Kategori'] = kat_up
        st.session_state.df = clean_master_df(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_data(st.session_state.df); st.rerun()

    if st.button("🚨 NULSTIL DATABASE"):
        if db_engine:
            with db_engine.connect() as conn: conn.execute(text("DROP TABLE IF EXISTS merchants")); conn.commit()
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 8. HOVEDTABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i CRM...", "")
    df_view = st.session_state.df.copy()
    
    if search:
        mask = df_view.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_view = df_view[mask]
    st.session_state.filtered = df_view

    # --- FILTRERING AF VISNING (GEM KOLONNER FRA TABEL) ---
    hide_cols = ['MATCH_KEY', 'Date Added']
    # Fjern kolonner med % i navnet fra hovedtabellen
    hide_cols += [c for c in df_view.columns if '%' in c]
    
    view_cols = [c for c in df_view.columns if c not in hide_cols]

    st.write(f"Antal annoncører: **{len(df_view)}**")
    st.info("💡 Klik til venstre for en række for at åbne kortet.")

    sel = st.dataframe(
        df_view[view_cols],
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_view.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("Databasen er tom. Upload en fil.")
