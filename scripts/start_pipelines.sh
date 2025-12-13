#!/bin/bash

# Plus besoin d'attendre 30s que le Master démarre, on attend juste un peu pour ClickHouse/MinIO
echo "⏳ Attente de 10 secondes pour les services..."
sleep 10

# Définition des paquets
PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"

# 1. Préchauffage (Toujours utile pour télécharger les JARs proprement une fois)
echo "⬇️  Préchauffage du cache..."
spark-submit \
  --master "local[*]" \
  --packages "$PACKAGES" \
  --class org.apache.spark.examples.SparkPi \
  /opt/bitnami/spark/examples/jars/spark-examples_*.jar 10

# 2. Lancement des pipelines en mode LOCAL
# Note : on utilise local[*] pour utiliser tous les coeurs disponibles
# Spark va se débrouiller pour partager le CPU entre les 3 scripts

echo "🚀 Lancement du pipeline EVENTS..."
spark-submit \
  --master "local[*]" \
  --packages "$PACKAGES" \
  /app/scripts/ingest_events.py &

echo "🚀 Lancement du pipeline MENTIONS..."
spark-submit \
  --master "local[*]" \
  --packages "$PACKAGES" \
  /app/scripts/ingest_mentions.py &

echo "🚀 Lancement du pipeline GKG..."
spark-submit \
  --master "local[*]" \
  --packages "$PACKAGES" \
  /app/scripts/ingest_gkg.py &

echo "📡 Tous les pipelines tournent en local. En attente..."
wait
