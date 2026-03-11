import streamlit as st

from config import API_URL
from api_client import check_health
from views.consumption import render as render_consumption

st.set_page_config(
    page_title="Energy Monitoring",
    page_icon="⚡",
    layout="wide",
)

# Sidebar
st.sidebar.title("Energy Monitoring")
st.sidebar.caption(f"API : `{API_URL}`")

# Health check dans la sidebar
health = check_health()
if health and health.get("status") == "ok":
    st.sidebar.success("API connectee")
else:
    st.sidebar.error("API injoignable")

page = st.sidebar.radio("Navigation", ["Consommation"])

# Routing
if page == "Consommation":
    render_consumption()
