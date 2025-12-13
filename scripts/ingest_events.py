from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. INITIALISATION ---
spark = SparkSession.builder \
    .appName("GDELT-Events-Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. DÉFINITION DU SCHÉMA (C'est ce qui manquait !) ---
# GDELT 2.0 Events a 61 colonnes. On crée un schéma générique (_c0 à _c60) en String.
# On typera les colonnes plus tard lors de la sélection.
gdelt_schema = StructType([StructField(f"_c{i}", StringType(), True) for i in range(61)])

# --- 3. LECTURE S3 AVEC SCHÉMA ---
raw_stream = spark.readStream \
    .format("csv") \
    .option("sep", "\t") \
    .schema(gdelt_schema) \
    .load("s3a://gdelt-raw/events/")

# --- 4. SELECTION & NETTOYAGE ---
df_clean = raw_stream.select(
    col("_c0").cast("long").alias("GLOBALEVENTID"),
    to_date(col("_c1"), "yyyyMMdd").alias("SQLDATE"),
    
    # Acteur 1
    when(col("_c6") == "", "UNK").otherwise(col("_c6")).alias("Actor1Name"),
    when(col("_c7") == "", "UNK").otherwise(col("_c7")).alias("Actor1CountryCode"),
    when(col("_c12") == "", "UNK").otherwise(col("_c12")).alias("Actor1Type1Code"),
    when(col("_c13") == "", "UNK").otherwise(col("_c13")).alias("Actor1Type2Code"),
    when(col("_c14") == "", "UNK").otherwise(col("_c14")).alias("Actor1Type3Code"),
    
    # Acteur 2
    when(col("_c16") == "", "UNK").otherwise(col("_c16")).alias("Actor2Name"),
    when(col("_c17") == "", "UNK").otherwise(col("_c17")).alias("Actor2CountryCode"),
    when(col("_c22") == "", "UNK").otherwise(col("_c22")).alias("Actor2Type1Code"),
    when(col("_c23") == "", "UNK").otherwise(col("_c23")).alias("Actor2Type2Code"),
    when(col("_c24") == "", "UNK").otherwise(col("_c24")).alias("Actor2Type3Code"),
    
    # Codes Evenement
    col("_c25").cast("int").alias("IsRootEvent"),
    col("_c26").alias("EventCode"),
    col("_c27").alias("EventBaseCode"),
    col("_c28").alias("EventRootCode"),
    col("_c29").cast("int").alias("QuadClass"),
    col("_c30").cast("float").alias("GoldsteinScale"),
    
    # Impact
    col("_c31").cast("int").alias("NumMentions"),
    col("_c32").cast("int").alias("NumSources"),
    col("_c33").cast("int").alias("NumArticles"),
    col("_c34").cast("float").alias("AvgTone"),
    
    # Geo Acteur 1
    col("_c35").cast("int").alias("Actor1Geo_Type"),
    when(col("_c37") == "", "UNK").otherwise(col("_c37")).alias("Actor1Geo_CountryCode"),
    col("_c40").cast("float").alias("Actor1Geo_Lat"),
    col("_c41").cast("float").alias("Actor1Geo_Long"),
    
    # Geo Acteur 2
    col("_c43").cast("int").alias("Actor2Geo_Type"),
    when(col("_c45") == "", "UNK").otherwise(col("_c45")).alias("Actor2Geo_CountryCode"),
    col("_c48").cast("float").alias("Actor2Geo_Lat"),
    col("_c49").cast("float").alias("Actor2Geo_Long"),
    
    # Geo Action
    col("_c51").cast("int").alias("ActionGeo_Type"),
    when(col("_c53") == "", "UNK").otherwise(col("_c53")).alias("ActionGeo_CountryCode"),
    col("_c56").cast("float").alias("ActionGeo_Lat"),
    col("_c57").cast("float").alias("ActionGeo_Long"),
    
    # Source
    col("_c60").alias("SOURCEURL")
) \
.where(col("GLOBALEVENTID").isNotNull()) \
.where(col("SQLDATE").isNotNull()) \
.where(col("ActionGeo_Lat").isNotNull()) \
.na.fill(0.0) \
.na.fill(0) \
.na.fill("UNK")

# --- 5. ECRITURE ---
def write_to_clickhouse(batch_df, batch_id):
    print(f"📦 Events Batch {batch_id} : Insertion de {batch_df.count()} lignes...")
    
   # batch_df.write \
   #     .format("jdbc") \
   #     .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
   #     .option("dbtable", "events") \
   #     .option("user", "default") \
   #     .option("password", "1234") \
   #     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
   #     .mode("append") \
   #     .save()
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default?ssl=false") \
        .option("dbtable", "events") \
        .option("user", "default") \
        .option("password", "1234") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .option("numPartitions", "1") \
        .option("batchsize", "1000") \
        .option("isolationLevel", "NONE") \
        .mode("append") \
        .save()
    print(f"✅ Batch {batch_id} terminé.")

query = df_clean.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "s3a://gdelt-raw/checkpoints/events_v2") \
    .start()

print("🚀 Pipeline Events V2 lancé !")
query.awaitTermination()
