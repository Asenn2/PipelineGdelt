from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. INITIALISATION ---
spark = SparkSession.builder \
    .appName("GDELT-GKG-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. DÉFINITION DU SCHÉMA (RAW 27 colonnes) ---
# Le GKG est très large, on définit tout en String pour la lecture
gdelt_gkg_schema = StructType([StructField(f"_c{i}", StringType(), True) for i in range(27)])

# --- 3. LECTURE S3 ---
# Attention : Assure-toi que NiFi envoie bien les fichiers GKG dans ce dossier
raw_stream = spark.readStream \
    .format("csv") \
    .option("sep", "\t") \
    .schema(gdelt_gkg_schema) \
    .load("s3a://gdelt-raw/gkg/")

# --- 4. SELECTION & FILTRAGE (Selon tes exclusions) ---
df_clean = raw_stream.select(
    col("_c0").alias("GKGRECORDID"),
    
    # Conversion de la date (Format GKG: YYYYMMDDHHMMSS)
    to_timestamp(col("_c1"), "yyyyMMddHHmmss").alias("DATE"),
    
    col("_c2").cast("int").alias("SourceCollectionIdentifier"),
    col("_c3").alias("SourceCommonName"),
    col("_c4").alias("DocumentIdentifier"),
    col("_c5").alias("Counts"),
    
    # _c6 = V2Counts (EXCLU)
    # _c7 = Themes (EXCLU)
    
    col("_c8").alias("V2Themes"), # Tu n'as pas exclu V2Themes, je le garde
    
    # _c9 = Locations (EXCLU)
    # _c10 = V2Locations (EXCLU)
    
    col("_c11").alias("Persons"), # Tu as exclu V2Persons, on garde Persons
    
    # _c12 = V2Persons (EXCLU)
    
    col("_c13").alias("Organizations"), # Tu as exclu V2Orgs, on garde Orgs
    
    # _c14 = V2Organizations (EXCLU)
    
    col("_c15").alias("V2Tone"), # L'analyse de sentiment (Tone, Positive, Negative...)
    
    # _c16 = Dates (EXCLU)
    
    col("_c17").alias("GCAM") # Global Content Analysis Measures
    
    # TOUT LE RESTE EST EXCLU (Index 18 à 26) :
    # SharingImage, RelatedImages, SocialImageEmbeds, SocialVideoEmbeds, 
    # Quotations, AllNames, Amounts, TranslationInfo, Extras
) \
.where(col("GKGRECORDID").isNotNull()) \
.na.fill("") # On remplit les NULLs par vide pour les chaînes de caractères

# --- 5. ECRITURE VERS CLICKHOUSE ---
def write_to_clickhouse(batch_df, batch_id):
    print(f"🧠 GKG Batch {batch_id} : Insertion de {batch_df.count()} lignes...")
    
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", "gkg") \
        .option("user", "default") \
        .option("password", "1234") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .mode("append") \
        .save()
    print(f"✅ Batch {batch_id} terminé.")

query = df_clean.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "s3a://gdelt-raw/checkpoints/gkg_v1") \
    .start()

print("🚀 Pipeline GKG lancé !")
query.awaitTermination()
