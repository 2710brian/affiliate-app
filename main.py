import streamlit as st
import pandas as pd
import os
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import base64
from datetime import datetime, date

# --- 1. KONFIGURATION ---
st.set_page_config(page_title="Affiliate CRM Pro", layout="wide")

# --- 2. DATABASE FORBINDELSE ---
def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("CREATE TABLE IF NOT EXISTS settings (type TEXT, value TEXT)"))
                conn.commit()
            return engine
        except: return None
    return None

db_engine = get_engine()

# --- 3. MASTER STRUKTUR ---
MASTER_COLS = [
    'Date Added', 'Kategori', 'MID', 'Virksomhed', 'Website', 'Programnavn', 
    'Produkter', 'Segment', 'Salgs % (sats)', 'EPC', 'Lead/Fast (sats)', 
    'Trafik', 'Feed?', 'Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Kontaktet', 
    'Aff. status', 'Kontakt dato', 'Network', 'Land', 'Ticketnr', 'Dialog', 'Opflg. dato', 'Noter', 'Fil_Navn', 'Fil_Data'
]

# --- 4. DROPDOWN ADMINISTRATION ---
def load_options():
    defaults = {
        "networks": ["Partner-ads", "addrevenue", "Adtraction", "Tradetracker", "Awin", "GJ", "Daisycon", "Shopnello", "TradeDoubler", "Webigans"],
        "lands": ["DK", "SE", "NO", "FI", "ES", "DE", "UK", "US", "NL"],
        "dialogs": ["Ikke kontakte", "Affiliate Audit", "Dialog i gang", "Oplæg sendt", "Infomail sendt", "Cold Mail", "Nystartet", "Kontaktet", "Mediebureau", "Vundet", "Tabt", "Følg op 1 mdr", "Følg op 3 mdr", "Følg op 6 mdr", "Droppet", "Call"],
        "aff_status": ["Godkendt", "Ikke ansøgt", "Afvist", "Afventer", "Pause"]
    }
    if db_engine:
        try:
            df_opt = pd.read_sql("SELECT * FROM settings", db_engine)
            for key in defaults.keys():
                stored = df_opt[df_opt['type'] == key]['value'].tolist()
                if stored: defaults[key] = sorted(list(set(stored)))
        except: pass
    return defaults

def add_option(opt_type, value):
    if db_engine and value:
        with db_engine.connect() as conn:
            conn.execute(text("INSERT INTO settings (type, value) VALUES (:t, :v)"), {"t": opt_type, "v": value})
            conn.commit()

# --- 5. RENSE-MOTOR ---
def get_safe_date(val):
    if not val or str(val).lower() in ['nat', 'nan', 'none', '', '00:00:00']: return date.today()
    try: return pd.to_datetime(val, dayfirst=True, errors='coerce').date() or date.today()
    except: return date.today()

def force_clean_data(df):
    if df.empty: return pd.DataFrame(columns=MASTER_COLS)
    rename_map = {
        'Programnavn': 'Virksomhed', 'Merchant': 'Virksomhed', 
        'Product Count': 'Produkter', 'EPC (nøgletal)': 'EPC', 
        'DateAdded': 'Date Added', 'Status': 'Aff. status', 
        'Dato': 'Kontakt dato', 'Aff. Status': 'Aff. status'
    }
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()] 
    df = df.astype(str).replace(['NaT', 'nan', 'None', '<NA>', 'nan.0', 'None.0', '00:00:00'], '')

    # Website Generator
    def set_site(r):
        s, v = str(r.get('Website', '')).strip(), str(r.get('Virksomhed', '')).strip()
        if (s == "" or s == "None" or s == "nan") and v != "":
            c = re.sub(r'[^a-z0-9]', '', v.lower())
            return f"https://www.{c}.dk" if c else ""
        return s
    df['Website'] = df.apply(set_site, axis=1)
    
    return df.reindex(columns=MASTER_COLS, fill_value="")

def load_db():
    if db_engine:
        try:
            df = pd.read_sql("SELECT * FROM merchants", db_engine)
            return force_clean_data(df)
        except: return pd.DataFrame(columns=MASTER_COLS)
    return pd.DataFrame(columns=MASTER_COLS)

def save_db(df):
    if db_engine:
        try:
            df = force_clean_data(df)
            df['MATCH_KEY'] = df['Virksomhed'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()))
            df = df.drop_duplicates('MATCH_KEY', keep='first').drop(columns=['MATCH_KEY'])
            df.to_sql('merchants', db_engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Save Error: {e}")
            return False
    return False

# --- 6. INITIALISERING ---
if 'df' not in st.session_state:
    st.session_state.df = load_db()
opts = load_options()

# --- 7. KLIENT KORT POP-UP ---
@st.dialog("📝 Klient-kort / CRM Detaljer", width="large")
def client_popup(idx):
    row = st.session_state.df.loc[idx].to_dict()
    st.title(f"🏢 {row.get('Virksomhed')}")
    st.divider()

    t1, t2 = st.tabs(["📊 CRM Data & System", "📓 Noter & Vedhæftninger"])
    upd = {}

    with t1:
        # Øverste del: Kontakt og Status
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("##### 📞 Kontakt")
            for f in ['Fornavn', 'Efternavn', 'Mail', 'Tlf', 'Website']:
                upd[f] = st.text_input(f, value=row.get(f, ''))
        with c2:
            st.markdown("##### ⚙️ Pipeline")
            d_v = row.get('Dialog', 'Ikke kontakte')
            upd['Dialog'] = st.selectbox("Dialog Status", opts['dialogs'], index=opts['dialogs'].index(d_v) if d_v in opts['dialogs'] else 0)
            upd['Ticketnr'] = st.text_input("Ticket #", value=row.get('Ticketnr', ''))
            upd['Opflg. dato'] = st.date_input("Næste opfølgning", value=get_safe_date(row.get('Opflg. dato'))).strftime('%d/%m/%Y')
            upd['Kontakt dato'] = st.date_input("Kontakt dato", value=get_safe_date(row.get('Kontakt dato'))).strftime('%d/%m/%Y')
        with c3:
            st.markdown("##### 📈 Aff. Info")
            a_v = row.get('Aff. status', 'Ikke ansøgt')
            upd['Aff. status'] = st.selectbox("Aff. status", opts['aff_status'], index=opts['aff_status'].index(a_v) if a_v in opts['aff_status'] else 0)
            upd['Kategori'] = st.text_input("Hovedkategori", value=row.get('Kategori', ''))
            upd['MID'] = st.text_input("MID", value=row.get('MID', ''))
            upd['Produkter'] = st.text_input("Produkter", value=row.get('Produkter', ''))
            upd['EPC'] = st.text_input("EPC", value=row.get('EPC', ''))

        st.divider()
        st.markdown("##### 📊 Tekniske Systemdata")
        ca, cb, cc = st.columns(3)
        with ca:
            upd['Date Added'] = st.date_input("Dato tilføjet", value=get_safe_date(row.get('Date Added'))).strftime('%d/%m/%Y')
            upd['Programnavn'] = st.text_input("Programnavn (Original)", value=row.get('Programnavn', ''))
        with cb:
            upd['Segment'] = st.text_input("Segment", value=row.get('Segment', ''))
            upd['Salgs % (sats)'] = st.text_input("Salgs %", value=row.get('Salgs % (sats)', ''))
            upd['Lead/Fast (sats)'] = st.text_input("Lead/Fast", value=row.get('Lead/Fast (sats)', ''))
        with cc:
            upd['Network'] = st.selectbox("Netværk", opts['networks'], index=opts['networks'].index(row.get('Network')) if row.get('Network') in opts['networks'] else 0)
            upd['Land'] = st.selectbox("Land Ikon", opts['lands'], index=opts['lands'].index(row.get('Land')) if row.get('Land') in opts['lands'] else 0)
            upd['Trafik'] = st.text_input("Trafik", value=row.get('Trafik', ''))

    with t2:
        st.markdown("##### 📓 Klient Logbog & Noter")
        upd['Noter'] = st.text_area("Skriv alle opdateringer og vigtig info her...", value=row.get('Noter', ''), height=300)
        
        st.divider()
        st.markdown("##### 📎 Vedhæftede filer (Billeder, Lyd, PDF)")
        if row.get('Fil_Navn'):
            st.info(f"📂 Nuværende fil: {row['Fil_Navn']}")
            if row.get('Fil_Data'):
                st.markdown(f'<a href="data:application/octet-stream;base64,{row["Fil_Data"]}" download="{row["Fil_Navn"]}">👉 Hent/Se fil</a>', unsafe_allow_html=True)
        
        up = st.file_uploader("Upload ny info/fil til klient", key=f"up_{idx}")
        if up:
            upd['Fil_Navn'] = up.name
            upd['Fil_Data'] = base64.b64encode(up.read()).decode()
            st.success("Ny fil klar!")

    if st.button("💾 GEM ALT PÅ KLIENT", type="primary", use_container_width=True):
        for k, v in upd.items(): st.session_state.df.at[idx, k] = v
        if save_db(st.session_state.df): st.rerun()

# --- 8. UI START ---
st.title("💼 Affiliate CRM Master")

with st.sidebar:
    st.header("⚙️ Kontrol & Dropdowns")
    with st.expander("🛠️ Ret i Dropdowns"):
        t_sel = st.selectbox("Vælg type:", ["networks", "lands", "dialogs", "aff_status"])
        v_new = st.text_input("Tilføj nyt valg:")
        if st.button("Tilføj nu") and v_new: add_option(t_sel, v_new); st.rerun()

    st.divider()
    st.header("📤 Eksport")
    st.download_button("📥 Master Export", st.session_state.df.to_csv(index=False), "master.csv", use_container_width=True)

    st.divider()
    st.header("📥 Import")
    kat_up = st.text_input("Kategori:", "Bolig, Have og Interiør")
    f_up = st.file_uploader("Vælg fil")
    if f_up and st.button("Flet & Gem", use_container_width=True):
        nd = pd.read_csv(f_up) if f_up.name.endswith('csv') else pd.read_excel(f_up)
        nd = force_clean_data(nd)
        nd['Kategori'] = kat_up
        st.session_state.df = force_clean_data(pd.concat([st.session_state.df, nd], ignore_index=True))
        save_db(st.session_state.df); st.rerun()

    if st.button("🚨 Nulstil Database"):
        if db_engine:
            with db_engine.connect() as conn:
                for t in ['merchants', 'settings']: conn.execute(text(f"DROP TABLE IF EXISTS {t}"))
                conn.execute(text("CREATE TABLE settings (type TEXT, value TEXT)"))
                conn.commit()
        st.session_state.df = pd.DataFrame(columns=MASTER_COLS); st.rerun()

# --- 9. TABEL ---
if not st.session_state.df.empty:
    search = st.text_input("🔍 Søg i alt...", "")
    df_show = st.session_state.df.copy()
    if search:
        mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
        df_show = df_show[mask]

    # Skjul Fil_Data i oversigten
    view_cols = [c for c in df_show.columns if c != 'Fil_Data']
    
    sel = st.dataframe(
        df_show[view_cols],
        column_config={"Website": st.column_config.LinkColumn("Website")},
        use_container_width=True, selection_mode="single-row", on_select="rerun", height=600
    )

    if sel.selection.rows:
        real_idx = df_show.index[sel.selection.rows[0]]
        client_popup(real_idx)
else:
    st.info("👋 Upload data for at starte.")
