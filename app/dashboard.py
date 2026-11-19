"""Dashboard metier des vols - Spark / Delta Gold / Streamlit."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="Observatoire des vols", layout="wide")
PINK = "#ff78b7"
PURPLE = "#b59aff"
PALETTE = [PINK, PURPLE, "#ffb6d5", "#75d5c6", "#f5bb83", "#7f9de5"]
px.defaults.template = "plotly_dark"
px.defaults.color_discrete_sequence = PALETTE
st.markdown("""<style>
.stApp {background:radial-gradient(ellipse at 85% 0%,#2b152b 0%,#111016 48%,#09090d 100%);color:#f8e7f2}
.block-container {max-width:1450px;padding-top:1.7rem}
h1,h2,h3 {color:#ffa0ce!important}
[data-testid="stSidebar"] {background:#19131e;border-right:1px solid #50304b}
[data-testid="stMetric"] {background:#241925;border:1px solid #623653;border-radius:16px;padding:16px}
[data-testid="stMetricValue"] {color:#ffa0ce!important}
[data-testid="stPlotlyChart"] {background:#1d1722;border:1px solid #55344c;border-radius:18px;padding:8px}
[data-testid="stTabs"] button[role="tab"] {color:#eab9d4!important;font-weight:650}
[data-testid="stTabs"] button[aria-selected="true"] {color:#ff8fc6!important}
</style>""", unsafe_allow_html=True)

@st.cache_resource
def spark_session():
    return SparkSession.builder.appName("Observatoire_Vols_Metier").getOrCreate()

@st.cache_data(ttl=600, show_spinner="Chargement des tables Gold...")
def load_data():
    spark = spark_session()
    paths = {
        "pareto": "/lakehouse/gold/airline_delay_pareto",
        "monthly": "/lakehouse/gold/monthly_flight_trends",
        "ranking": "/lakehouse/gold/top_airlines_by_origin",
        "segments": "/lakehouse/gold/airline_segments",
    }
    return {k: spark.read.format("delta").load(path).toPandas() for k, path in paths.items()}

def chart(fig, height=420):
    fig.update_layout(
        height=height, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#f5dbea"), margin=dict(l=20,r=25,t=55,b=35),
        xaxis=dict(gridcolor="#38283c"), yaxis=dict(gridcolor="#38283c"),
        legend=dict(bgcolor="rgba(0,0,0,0)"), hoverlabel=dict(bgcolor="#312036"),
    )
    st.plotly_chart(fig, use_container_width=True)

def number(x, digits=0):
    return f"{x:,.{digits}f}".replace(",", " ").replace(".", ",")

st.title("Observatoire des vols")
st.caption("Analyse métier : retards, annulations, trafic et présence dans les aéroports")
with st.sidebar:
    st.header("Filtres")
    if st.button("Actualiser les données", use_container_width=True):
        load_data.clear()
        st.rerun()
try:
    data = load_data()
except Exception as exc:
    st.error("Impossible de lire les tables Delta Gold.")
    st.exception(exc)
    st.stop()

p = data["pareto"].copy()
m = data["monthly"].copy()
a = data["ranking"].copy()
s = data["segments"].copy()
for col in ["total_flights", "avg_arrival_delay", "p90_arrival_delay", "cancellation_rate"]:
    s[col] = pd.to_numeric(s[col], errors="coerce")
s["airline"] = s["airline"].astype(str)
for col in ["total_flights", "total_delay_minutes", "median_delay", "iqr_delay", "cumulative_delay_pct"]:
    p[col] = pd.to_numeric(p[col], errors="coerce")
for col in ["total_flights", "growth_pct"]:
    m[col] = pd.to_numeric(m[col], errors="coerce")
a["total_flights"] = pd.to_numeric(a["total_flights"], errors="coerce")
m["month_start"] = pd.to_datetime(m["month_start"], errors="coerce")
p["airline"] = p["airline"].astype(str)
a["airline"] = a["airline"].astype(str)
a["origin_airport"] = a["origin_airport"].astype(str)
p = p.dropna(subset=["total_flights", "total_delay_minutes"])
p = p[p["total_flights"] > 0]
p["delay_per_flight"] = p["total_delay_minutes"] / p["total_flights"]

with st.sidebar:
    selected = st.multiselect("Compagnies", sorted(p["airline"].unique()), default=sorted(p["airline"].unique()))
    top_n = st.slider("Nombre d'éléments affichés", 5, 20, 10)
p_sel = p[p["airline"].isin(selected)].copy()
a_sel = a[a["airline"].isin(selected)].copy()
s_sel = s[s["airline"].isin(selected)].copy()

c1,c2,c3,c4 = st.columns(4)
c1.metric("Vols (compagnies sélectionnées)", number(p_sel["total_flights"].sum()))
c2.metric("Compagnies", len(p_sel))
c3.metric("Minutes de retard cumulées", number(p_sel["total_delay_minutes"].sum()))
c4.metric("Retard positif par vol", f"{number(p_sel['total_delay_minutes'].sum()/p_sel['total_flights'].sum(),1)} min" if p_sel["total_flights"].sum() else "—")
st.caption("Le retard positif par vol est calculé sur tous les vols, y compris ceux sans retard ; ce n'est pas un taux de vols retardés.")

t1,t2,t3,t4 = st.tabs(["Retards", "Fiabilité des compagnies", "Trafic mensuel", "Aéroports"])

with t1:
    st.subheader("Quelles compagnies accumulent le plus de retard ?")
    if p_sel.empty:
        st.info("Sélectionnez au moins une compagnie.")
    else:
        d = p_sel.nlargest(top_n, "total_delay_minutes").sort_values("total_delay_minutes")
        fig = px.bar(d, x="total_delay_minutes", y="airline", orientation="h", color="delay_per_flight",
                     color_continuous_scale=["#50375f", "#ad5899", "#ff99ca"],
                     labels={"total_delay_minutes":"Minutes de retard cumulées", "airline":"Compagnie", "delay_per_flight":"Retard / vol (min)"},
                     title="Volume total de retard — taille et intensité")
        chart(fig, max(360, 38*len(d)+140))
        st.caption("Le volume total dépend du nombre de vols : la couleur montre le retard positif moyen par vol.")
        st.subheader("Qui accumule le plus de retard par vol ?")
        d = p_sel.nlargest(top_n, "delay_per_flight").sort_values("delay_per_flight")
        fig = px.bar(d, x="delay_per_flight", y="airline", orientation="h", color_discrete_sequence=[PURPLE],
                     labels={"delay_per_flight":"Minutes de retard positif par vol", "airline":"Compagnie"},
                     title="Comparaison corrigée du volume de vols")
        chart(fig, max(360, 38*len(d)+140))
        st.info("Cet indicateur permet de comparer les compagnies sans privilégier automatiquement celles qui réalisent le plus de vols.")

with t2:
    st.subheader("Fiabilité : retards et annulations par compagnie")
    st.caption("Comparaison des compagnies à partir de la table Gold airline_segments. Les indicateurs ne constituent pas un score unique.")
    d = s_sel.dropna(subset=["avg_arrival_delay", "p90_arrival_delay", "cancellation_rate"]).copy()
    if d.empty:
        st.info("Aucune donnée de fiabilité disponible pour les compagnies sélectionnées.")
    else:
        d["cancellation_pct"] = d["cancellation_rate"] * 100
        d["p90_arrival_delay"] = d["p90_arrival_delay"]
        st.markdown("**1. Quelles compagnies annulent le plus de vols ?**")
        cancel = d.sort_values("cancellation_pct", ascending=False).head(top_n).sort_values("cancellation_pct")
        fig = px.bar(cancel, x="cancellation_pct", y="airline", orientation="h",
                     color="cancellation_pct", color_continuous_scale=["#56406c", "#ad5899", "#ff99ca"],
                     labels={"cancellation_pct":"Vols annulés (%)", "airline":"Compagnie"},
                     title="Taux d'annulation par compagnie")
        chart(fig, max(360, 38 * len(cancel) + 140))
        st.markdown("**2. Quelles compagnies présentent les retards les plus importants ?**")
        delay = d.sort_values("avg_arrival_delay", ascending=False).head(top_n)
        long = delay.melt(id_vars="airline", value_vars=["avg_arrival_delay", "p90_arrival_delay"],
                          var_name="indicateur", value_name="minutes")
        long["indicateur"] = long["indicateur"].map({"avg_arrival_delay":"Retard moyen", "p90_arrival_delay":"Retard P90"})
        fig = px.bar(long, x="airline", y="minutes", color="indicateur", barmode="group",
                     color_discrete_map={"Retard moyen":PINK, "Retard P90":PURPLE},
                     labels={"airline":"Compagnie", "minutes":"Minutes", "indicateur":"Mesure"},
                     title="Retard moyen et 90e percentile à l'arrivée")
        chart(fig)
        st.caption("P90 : 90 % des retards d'arrivée observés sont inférieurs ou égaux à cette valeur. Les vols annulés peuvent ne pas avoir de retard d'arrivée mesuré.")
        st.markdown("**3. Existe-t-il un compromis entre ponctualité et annulations ?**")
        fig = px.scatter(d, x="avg_arrival_delay", y="cancellation_pct", size="total_flights",
                         color="p90_arrival_delay", text="airline",
                         color_continuous_scale=["#5d487e", "#a96cac", "#ff96c9"],
                         hover_data={"total_flights":":,.0f", "p90_arrival_delay":":.1f", "cancellation_pct":":.2f"},
                         labels={"avg_arrival_delay":"Retard moyen à l'arrivée (min)",
                                 "cancellation_pct":"Taux d'annulation (%)",
                                 "p90_arrival_delay":"Retard P90 (min)", "total_flights":"Nombre de vols"},
                         title="Retard moyen vs taux d'annulation")
        fig.update_traces(textposition="top center")
        chart(fig, 500)
        st.info("En bas à gauche : compagnies ayant à la fois un retard moyen et un taux d'annulation plus faibles. La taille des bulles indique le nombre de vols.")
        st.dataframe(d[["airline", "total_flights", "avg_arrival_delay", "p90_arrival_delay", "cancellation_pct"]]
                     .rename(columns={"airline":"Compagnie", "total_flights":"Vols",
                                      "avg_arrival_delay":"Retard moyen (min)", "p90_arrival_delay":"P90 (min)",
                                      "cancellation_pct":"Annulations (%)"})
                     .sort_values("Annulations (%)"), hide_index=True, use_container_width=True)

with t3:
    st.subheader("Quand le trafic est-il le plus important ?")
    d = m.dropna(subset=["month_start", "total_flights"]).sort_values("month_start")
    if d.empty:
        st.info("Aucune donnée mensuelle.")
    else:
        fig = px.line(d, x="month_start", y="total_flights", markers=True,
                      labels={"month_start":"Mois", "total_flights":"Nombre de vols"}, title="Évolution mensuelle du trafic")
        fig.update_traces(line=dict(color=PINK, width=3), marker=dict(size=9))
        chart(fig)
        peak = d.loc[d["total_flights"].idxmax()]
        st.info(f"Pic de trafic : {peak['month_start']:%m/%Y} — {number(peak['total_flights'])} vols.")
        g = d.dropna(subset=["growth_pct"]).copy()
        if not g.empty:
            fig = px.bar(g, x="month_start", y="growth_pct", color="growth_pct",
                         color_continuous_scale=["#8f82d5", "#4b3c5a", "#ff83bd"],
                         labels={"month_start":"Mois", "growth_pct":"Variation (%)"},
                         title="Hausse ou baisse du trafic par rapport au mois précédent")
            fig.add_hline(y=0, line_color="#d9b8d1")
            chart(fig)
        st.caption("Ces tendances sont globales : la table mensuelle ne contient pas de détail par compagnie.")

with t4:
    st.subheader("Où les compagnies sont-elles les plus présentes ?")
    if a_sel.empty:
        st.info("Aucun aéroport pour la sélection.")
    else:
        d = a_sel.nlargest(top_n, "total_flights").sort_values("total_flights")
        d["label"] = d["origin_airport"] + " · " + d["airline"]
        fig = px.bar(d, x="total_flights", y="label", orientation="h", color="airline",
                     labels={"total_flights":"Vols de la compagnie dominante", "label":"Aéroport · compagnie", "airline":"Compagnie"},
                     title="Aéroports avec les plus grands volumes de la compagnie dominante")
        chart(fig, max(400, 37*len(d)+150))
        counts = a_sel.groupby("airline", as_index=False)["origin_airport"].nunique().rename(columns={"origin_airport":"airports_dominated"})
        counts = counts.nlargest(top_n, "airports_dominated").sort_values("airports_dominated")
        fig = px.bar(counts, x="airports_dominated", y="airline", orientation="h", color_discrete_sequence=[PURPLE],
                     labels={"airports_dominated":"Nombre d'aéroports où la compagnie est 1re", "airline":"Compagnie"},
                     title="Quelles compagnies dominent le plus d'aéroports ?")
        chart(fig, max(350, 37*len(counts)+140))
        st.caption("La table Gold ne conserve que la compagnie classée première par aéroport. Les graphiques ne représentent pas l'ensemble des compagnies de chaque aéroport.")

st.divider()
st.caption("Sources : Delta Gold — airline_delay_pareto, monthly_flight_trends, top_airlines_by_origin")
