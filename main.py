import streamlit as st
import pandas as pd

# --- KONFIGURATION ---
st.set_page_config(page_title="Affiliate Lead Maskine", layout="wide")

# --- DATA-HÅNDTERING (DATABASE) ---
# Vi bruger 'session_state' til at gemme dine rettelser midlertidigt. 
# Senere forbinder vi dette til en rigtig database på Railway.
if 'df' not in st.session_state:
    # Her er et uddrag af de data, du sendte mig før, samkørt.
    initial_data = [
        {"Merchant": "3-nordic", "Kategori": "Bolig, Have og interiør", "Network": "Partner-ads", "Produkter": "3,146", "EPC": "0.69", "Kommission": "8,00 %", "Status": "Godkendt", "Mail": "", "Noter": ""},
        {"Merchant": "Boligcenter.dk", "Kategori": "Bolig, Have og interiør", "Network": "Partner-ads", "Produkter": "546,119", "EPC": "N/A", "Kommission": "N/A", "Status": "Godkendt", "Mail": "", "Noter": "Kontaktet 28/02/2026"},
        {"Merchant": "AndLight DK", "Kategori": "Bolig, Have og interiør", "Network": "Partner-ads", "Produkter": "31,166", "EPC": "4.23", "Kommission": "7,00 %", "Status": "Afvist", "Mail": "", "Noter": ""},
        {"Merchant": "Badeshop", "Kategori": "Bolig, Have og interiør", "Network": "Partner-ads", "Produkter": "54", "EPC": "1.02", "Kommission": "5,00 %", "Status": "Godkendt", "Mail": "", "Noter": ""},
    ]
    st.session_state.df = pd.DataFrame(initial_data)

# --- APP UI ---
st.title("🚀 Affiliate Lead Maskine")

# Søgning og Filtrering
col_search, col_filter = st.columns([2, 1])
with col_search:
    search_query = st.text_input("🔍 Søg efter annoncør (navn eller kategori)...", "")
with col_filter:
    categories = ["Alle"] + list(st.session_state.df['Kategori'].unique())
    selected_cat = st.selectbox("Filter: Kategori", categories)

# Filtrér data baseret på søgning
filtered_df = st.session_state.df.copy()
if selected_cat != "Alle":
    filtered_df = filtered_df[filtered_df['Kategori'] == selected_cat]
if search_query:
    filtered_df = filtered_df[filtered_df['Merchant'].str.contains(search_query, case=False)]

# --- VISNING AF ANNONCØR-KORT ---
st.write(f"Antal annoncører fundet: **{len(filtered_df)}**")

for index, row in filtered_df.iterrows():
    # Vi skaber et kort-agtigt layout med en 'expander' eller en 'container'
    with st.expander(f"🏢 {row['Merchant']} | {row['Kategori']} ({row['Status']})", expanded=True):
        c1, c2, c3 = st.columns([1, 1, 1])
        
        with c1:
            st.markdown("**Netværks Info**")
            st.write(f"🌐 Netværk: {row['Network']}")
            st.write(f"📦 Produkter: {row['Produkter']}")
            st.write(f"💰 EPC: {row['EPC']}")
            st.write(f"📈 Kommission: {row['Kommission']}")
        
        with c2:
            st.markdown("**Lead Detaljer**")
            # Her kan du redigere e-mail og noter
            new_mail = st.text_input("E-mail", value=row['Mail'], key=f"mail_{index}")
            new_notes = st.text_area("Kontakt-log / Noter", value=row['Noter'], key=f"notes_{index}", height=68)
            
            if st.button("Gem ændringer", key=f"save_{index}"):
                st.session_state.df.at[index, 'Mail'] = new_mail
                st.session_state.df.at[index, 'Noter'] = new_notes
                st.success("Lead opdateret!")

        with c3:
            st.markdown("**Handlinger**")
            st.write("Skal denne fjernes?")
            if st.button("❌ Slet Annoncør", key=f"del_{index}"):
                st.session_state.df = st.session_state.df.drop(index)
                st.rerun()

# --- EKSPORT ---
st.sidebar.markdown("---")
st.sidebar.download_button(
    label="📥 Download Master-liste (CSV)",
    data=st.session_state.df.to_csv(index=False),
    file_name="affiliate_leads_export.csv",
    mime="text/csv"
)
