import streamlit as st
import plotly.express as px
from pyspark.sql import SparkSession

st.set_page_config(page_title="Lakehouse Analytics", layout="wide")
st.title("Explorateur Data Lakehouse (HDFS + Delta Lake)")


@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .appName("StreamlitGoldReader")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@st.cache_data(ttl=60)
def load_gold_table(hdfs_path: str):
    spark = get_spark()
    return spark.read.format("delta").load(hdfs_path).toPandas()


def list_gold_tables() -> list[str]:
    """Liste les tables Delta disponibles dans /lakehouse/gold/ sur HDFS."""
    try:
        spark = get_spark()
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
        gold_root = jvm.org.apache.hadoop.fs.Path("/lakehouse/gold")
        if not fs.exists(gold_root):
            return []
        tables = []
        for status in fs.listStatus(gold_root):
            if status.isDirectory():
                delta_log = jvm.org.apache.hadoop.fs.Path(status.getPath(), "_delta_log")
                if fs.exists(delta_log):
                    tables.append(status.getPath().getName())
        return sorted(tables)
    except Exception:
        return []


col_info, col_btn = st.columns([5, 1])
with col_info:
    st.info(
        "Cluster HDFS connecté (`hdfs://namenode:9000`). "
        "Alimentez vos tables Delta dans `/lakehouse/gold/` depuis JupyterLab pour afficher vos analyses ici."
    )
with col_btn:
    if st.button("Actualiser le cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

with st.expander("Exemple d'intégration d'une requête expliquée (attendu pour le projet)"):
    st.code(
        """
SELECT category,
       COUNT(*) AS total_orders,
       PERCENTILE_APPROX(delivery_days, 0.5) AS median_delivery_days
FROM delta.`/lakehouse/silver/orders`
GROUP BY category
ORDER BY total_orders DESC;
        """,
        language="sql",
    )
    st.markdown(
        "**Justification statistique :** Utilisation de la médiane plutôt que de la moyenne "
        "pour neutraliser les retards logistiques extrêmes."
    )

gold_tables = list_gold_tables()

if not gold_tables:
    st.warning(
        "Aucune table Delta détectée dans `/lakehouse/gold/` pour le moment. "
        "Suivez les notebooks des **Jours 1 à 4** sur JupyterLab (`http://localhost:8888`, token `bigdata`) "
        "pour construire les couches Bronze, Silver et Gold."
    )
else:
    st.success(f"Tables Gold détectées sur HDFS ({len(gold_tables)}) : {', '.join(f'`{t}`' for t in gold_tables)}")

    tabs = st.tabs(["Vue Synthèse Olist", "Explorateur de tables Gold"])

    with tabs[0]:
        if "kpi_categories" in gold_tables:
            df_cat = load_gold_table("/lakehouse/gold/kpi_categories")
            st.subheader("Performance logistique et commerciale par catégorie (Top 15)")
            c1, c2 = st.columns(2)
            top_cat = df_cat.sort_values("total_orders", ascending=False).head(15)
            with c1:
                fig_rev = px.bar(
                    top_cat,
                    x="total_revenue",
                    y="category",
                    orientation="h",
                    color="late_rate_pct",
                    color_continuous_scale="RdYlGn_r",
                    title="Chiffre d'affaires et taux de retard (%) par catégorie",
                    labels={
                        "total_revenue": "CA Total (BRL)",
                        "category": "Catégorie",
                        "late_rate_pct": "Retard (%)",
                    },
                )
                fig_rev.update_layout(yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig_rev, use_container_width=True)
            with c2:
                fig_deliv = px.scatter(
                    top_cat,
                    x="median_delivery_days",
                    y="mean_delivery_days",
                    size="total_orders",
                    hover_name="category",
                    color="iqr_delivery_days",
                    title="Moyenne vs Médiane des délais de livraison (asymétrie logistique)",
                    labels={
                        "median_delivery_days": "Délai médian (jours)",
                        "mean_delivery_days": "Délai moyen (jours)",
                        "iqr_delivery_days": "IQR (jours)",
                    },
                )
                st.plotly_chart(fig_deliv, use_container_width=True)

        if "kpi_monthly" in gold_tables:
            df_month = load_gold_table("/lakehouse/gold/kpi_monthly").sort_values("order_month")
            st.subheader("Dynamique mensuelle des commandes et délais")
            fig_m = px.line(
                df_month,
                x="order_month",
                y=["total_orders", "late_orders"],
                markers=True,
                title="Évolution mensuelle du volume de commandes et des livraisons en retard",
            )
            st.plotly_chart(fig_m, use_container_width=True)

        if "customer_rfm" in gold_tables:
            df_rfm = load_gold_table("/lakehouse/gold/customer_rfm")
            st.subheader("Segmentation Clients RFM")
            seg_summary = (
                df_rfm.groupby("segment", as_index=False)
                .agg(
                    nb_clients=("customer_unique_id", "count"),
                    panier_median=("monetary", "median"),
                    recence_mediane=("recency_days", "median"),
                )
                .sort_values("nb_clients", ascending=False)
            )
            c3, c4 = st.columns([1, 1])
            with c3:
                fig_seg = px.pie(
                    seg_summary,
                    names="segment",
                    values="nb_clients",
                    hole=0.4,
                    title="Répartition des segments clients RFM",
                )
                st.plotly_chart(fig_seg, use_container_width=True)
            with c4:
                st.dataframe(seg_summary, use_container_width=True, hide_index=True)

    with tabs[1]:
        selected = st.selectbox("Choisir une table Delta dans `/lakehouse/gold/` :", gold_tables)
        if selected:
            df_sel = load_gold_table(f"/lakehouse/gold/{selected}")
            st.caption(f"Chemin HDFS : `hdfs://namenode:9000/lakehouse/gold/{selected}` — {len(df_sel):,} lignes")
            st.dataframe(df_sel, use_container_width=True)
