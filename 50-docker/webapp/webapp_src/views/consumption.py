import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date, timedelta

from api_client import get_consumption_forecast


def render():
    st.header("Prevision de consommation Linky (72h)")

    default_date = date.today()
    start_date = st.date_input("Date de debut", value=default_date)

    if st.button("Charger la prevision 72h"):
        frames = []
        missing_days = []

        with st.spinner("Chargement des 3 jours..."):
            for i in range(3):
                d = start_date + timedelta(days=i)
                data = get_consumption_forecast(d)
                if data is None:
                    missing_days.append(str(d))
                    continue
                df_day = pd.DataFrame(data["predictions"])
                frames.append(df_day)

        if not frames:
            st.warning(f"Aucune prevision disponible a partir du {start_date}.")
            return

        if missing_days:
            st.info(f"Donnees manquantes pour : {', '.join(missing_days)}")

        df = pd.concat(frames, ignore_index=True)
        df["hour"] = pd.to_datetime(df["hour"])
        df = df.sort_values("hour").drop_duplicates(subset="hour")

        end_date = start_date + timedelta(days=2)

        # Graphique 72h avec intervalle de confiance
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df["hour"], y=df["upper"],
            mode="lines",
            line=dict(width=0),
            showlegend=False,
        ))
        fig.add_trace(go.Scatter(
            x=df["hour"], y=df["lower"],
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor="rgba(68,114,196,0.2)",
            name="Intervalle de confiance (80%)",
        ))
        fig.add_trace(go.Scatter(
            x=df["hour"], y=df["predicted"],
            mode="lines+markers",
            name="Prediction SARIMA",
            line=dict(color="#4472C4", width=2),
            marker=dict(size=3),
        ))

        fig.update_layout(
            title=f"Prevision consommation — {start_date} au {end_date}",
            xaxis_title="Date / Heure",
            yaxis_title="kWh",
            hovermode="x unified",
            xaxis=dict(
                dtick=6 * 3600 * 1000,  # tick toutes les 6h
                tickformat="%a %d %Hh",
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )

        st.plotly_chart(fig, use_container_width=True)

        # KPIs
        col1, col2, col3 = st.columns(3)
        col1.metric("Total prevu", f"{df['predicted'].sum():.1f} kWh")
        col2.metric("Pic max", f"{df['predicted'].max():.2f} kWh")
        col3.metric("Creux min", f"{df['predicted'].min():.2f} kWh")

        # Tableau detaille
        with st.expander("Donnees detaillees"):
            display_df = df.copy()
            display_df["hour"] = display_df["hour"].dt.strftime("%a %d %H:%M")
            display_df.columns = ["Heure", "Prediction (kWh)", "Borne basse", "Borne haute"]
            st.dataframe(display_df, use_container_width=True, hide_index=True)
