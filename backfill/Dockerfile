# On part d'une image Python légère
FROM python:3.9-slim

WORKDIR /app

# Installation des dépendances
# On le fait directement ici pour éviter de créer un requirements.txt juste pour 3 libs
RUN pip install requests boto3

# Copie du script
COPY backfill.py .

# Commande de lancement
CMD ["python", "backfill.py"]
