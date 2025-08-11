import streamlit as st
import pandas as pd
import glob
import time

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")

st.title("🛡️ Real-Time Fraud Detection Dashboard")

# Dictionnaire pour les couleurs par type de fraude
FRAUD_EMOJIS = {
    "HIGH_VALUE": "🔴",
    "HIGH_FREQUENCY": "🟠",
    "GEO_SWITCH": "🟣",
    "CURRENCY_SWITCH": "🔵",
    "CAROUSEL_FRAUD": "🟢"
}

def load_latest_alerts(parquet_dir="/app/output/parquet_alerts"):
    files = glob.glob(f"{parquet_dir}/*.parquet")
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.sort_values("timestamp", ascending=False)
    return df

# Rafraîchissement automatique toutes les 5 secondes
REFRESH_INTERVAL = 5

placeholder = st.empty()

while True:
    df = load_latest_alerts()
    with placeholder.container():
        st.markdown("## Statistiques par type de fraude")
        cols = st.columns(len(FRAUD_EMOJIS))
        for i, (fraud_type, emoji) in enumerate(FRAUD_EMOJIS.items()):
            count = df[df["fraud_type"] == fraud_type].shape[0] if not df.empty else 0
            cols[i].metric(
                label=f"{emoji} {fraud_type.replace('_', ' ').title()}",
                value=count
            )

        st.markdown("---")
        with st.expander("ℹ️ À quoi correspond le flag ?"):
            st.markdown("""
            Le **flag** coloré à côté de chaque alerte indique le type de fraude détecté :

            - 🔴 **HIGH_VALUE** : Une transaction supérieure à 49 000 € a été détectée pour cet utilisateur.
            - 🟠 **HIGH_FREQUENCY** : Au moins 3 transactions ont été réalisées par le même utilisateur en moins de 5 minutes.
            - 🟣 **GEO_SWITCH** : L’utilisateur a effectué des transactions depuis plusieurs pays différents en moins de 5 minutes.
            - 🔵 **CURRENCY_SWITCH** : L’utilisateur a utilisé au moins deux devises différentes en moins de 5 minutes.
            - 🟢 **CAROUSEL_FRAUD** : Plusieurs petits montants (<30 €) ont été dépensés chez au moins 3 marchands différents en moins de 3 minutes.
            """)

        with st.expander("Voir le nombre total de transactions traitées"):
            st.metric("Transactions totales", len(df))

        st.markdown("## Dernières alertes de fraude")
        parquet_files = glob.glob("/app/output/parquet_alerts/*.parquet")
        st.caption(f"Fichiers Parquet trouvés : {len(parquet_files)}")
        if df.empty:
            st.info("Aucune alerte détectée pour le moment.")
        else:
            df["Flag"] = df["fraud_type"].apply(
                lambda x: f"{FRAUD_EMOJIS.get(x, '⚪')} {x}"
            )
            st.dataframe(
                df[["timestamp", "user_id", "transaction_id", "Flag"]],
                use_container_width=True,
                hide_index=True
            )
    time.sleep(REFRESH_INTERVAL)