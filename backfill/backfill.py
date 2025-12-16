import requests
import boto3
import io
import zipfile
import gzip
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "gdelt-raw"

# Période à vérifier (15 jours en arrière)
DAYS_BACK = int(os.getenv("DAYS_BACK", 10)) 
START_DATE = datetime.utcnow() - timedelta(days=DAYS_BACK)
END_DATE = datetime.utcnow()

# Les 3 types de fichiers GDELT
FILE_TYPES = {
    "events": ("export",".CSV.zip"),   # GDELT appelle ça "export", tu l'appelles "events"
    "mentions": ("mentions",".CSV.zip"),
    "gkg": ("gkg",".csv.zip")
}

# Connexion MinIO
s3 = boto3.client('s3',
                  endpoint_url=MINIO_ENDPOINT,
                  aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)

def upload_to_minio(file_content, object_name):
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=file_content)
        print(f"✅ Uploadé : {object_name}")
    except Exception as e:
        print(f"❌ Erreur Upload {object_name}: {e}")

def file_exists(object_name):
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=object_name)
        return True
    except ClientError:
        return False

def process_date(date_obj):
    # Format GDELT : YYYYMMDDHHMMSS
    timestamp = date_obj.strftime("%Y%m%d%H%M00")
    
    for local_name, (gdelt_name,extension) in FILE_TYPES.items():
        # 1. Définir les noms de fichiers
        # URL GDELT (toujours en .zip)
        gdelt_url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.{gdelt_name}{extension}"
        
        # Ton format MinIO (d'après ton image : dossier/nom_date.csv.gz)
        minio_path = f"{local_name}/{local_name}_{timestamp}.csv.gz"

        # 2. Vérifier si on l'a déjà (Pour ne pas écraser l'existant !)
        if file_exists(minio_path):
            print(f"⏩ Déjà présent (Skipped) : {minio_path}")
            continue

        # 3. Télécharger si manquant
        print(f"⬇️  Téléchargement manquant : {gdelt_url}...")
        res = requests.get(gdelt_url)
        
        if res.status_code == 200:
            try:
                # 4. Conversion magique : ZIP -> CSV -> GZIP
                # GDELT donne un ZIP, Spark/NiFi a l'air de préférer GZIP chez toi
                with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                    # On prend le premier fichier du zip (le CSV)
                    csv_filename = z.namelist()[0]
                    with z.open(csv_filename) as csv_file:
                        csv_content = csv_file.read()
                        
                        # On re-compresse en GZIP
                        gzip_buffer = io.BytesIO()
                        with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
                            gz.write(csv_content)
                        
                        # 5. Envoyer à MinIO
                        upload_to_minio(gzip_buffer.getvalue(), minio_path)
            except Exception as e:
                print(f"⚠️ Erreur de traitement ZIP pour {gdelt_url}: {e}")
        elif res.status_code == 404:
            print(f"⚠️ Fichier introuvable chez GDELT (Peut-être pas encore généré ?) : {gdelt_url}")
        else:
            print(f"❌ Erreur HTTP {res.status_code}")

# --- MAIN LOOP ---
current_check = START_DATE
# On arrondit au quart d'heure précédent pour commencer propre
current_check = current_check - timedelta(minutes=current_check.minute % 15, seconds=current_check.second, microseconds=current_check.microsecond)

print(f"🚀 Démarrage du Backfill de {START_DATE} à {END_DATE}")

while current_check < END_DATE:
    process_date(current_check)
    current_check += timedelta(minutes=15)

print("🏁 Backfill terminé !")
