from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. INITIALISATION ---
spark = SparkSession.builder \
    .appName("GDELT-Mentions-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. DÉFINITION DU SCHÉMA (RAW 16 colonnes) ---
# On lit tout en String d'abord pour éviter les erreurs de parsing
gdelt_mentions_schema = StructType([StructField(f"_c{i}", StringType(), True) for i in range(16)])

# --- 3. LECTURE S3 ---
raw_stream = spark.readStream \
    .format("csv") \
    .option("sep", "\t") \
    .schema(gdelt_mentions_schema) \
    .load("s3a://gdelt-raw/mentions/")

# --- 4. SELECTION & NETTOYAGE (On garde uniquement ce que tu as demandé) ---
# Mapping basé sur GDELT 2.0 Mentions Codebook
df_clean = raw_stream.select(
    col("_c0").cast("long").alias("GLOBALEVENTID"),
    
    # Conversion des dates (Format GDELT: YYYYMMDDHHMMSS)
    to_timestamp(col("_c1"), "yyyyMMddHHmmss").alias("EventTimeDate"),
    to_timestamp(col("_c2"), "yyyyMMddHHmmss").alias("MentionTimeDate"),
    
    col("_c3").cast("int").alias("MentionType"),
    
    when(col("_c4") == "", "UNK").otherwise(col("_c4")).alias("MentionSourceName"),
    when(col("_c5") == "", "UNK").otherwise(col("_c5")).alias("MentionIdentifier"),
    
    # --- COLONNES SAUTÉES ---
    # _c6 = SentenceID
    # _c7 = Actor1CharOffset
    # _c8 = Actor2CharOffset
    # _c9 = ActionCharOffset
    
    col("_c10").cast("int").alias("InRawText"),
    col("_c11").cast("int").alias("Confidence"),
    col("_c12").cast("int").alias("MentionDocLen"),
    col("_c13").cast("float").alias("MentionDocTone")
    
    # --- COLONNES SAUTÉES ---
    # _c14 = MentionDocTranslationInfo
    # _c15 = Extras
) \
.where(col("GLOBALEVENTID").isNotNull()) \
.na.fill(0) \
.na.fill("UNK") \
.na.fill(0.00)

# --- 5. ECRITURE VERS CLICKHOUSE ---
def write_to_clickhouse(batch_df, batch_id):
    print(f"📦 Mentions Batch {batch_id} : Insertion de {batch_df.count()} lignes...")
    
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", "mentions") \
        .option("user", "default") \
        .option("password", "1234") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .mode("append") \
        .save()
    print(f"✅ BatchMention {batch_id} terminé.")

# On utilise un checkpoint différent pour ne pas mélanger avec events
query = df_clean.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "s3a://gdelt-raw/checkpoints/mentions_v1") \
    .start()

print("🚀 Pipeline MENTIONS lancé !")
query.awaitTermination()
