# Image légère, compatible Python 3.13
FROM python:3.13-slim

ARG STREAMLIT_PORT=8501
# Répertoire de travail
WORKDIR /app

# Installer les outils nécessaires pour compiler numpy/pandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# Copier le fichier des dépendances
COPY docker/streamlit/requirements.txt .

# Installer les libs Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l’application
COPY src/app ./src/app
COPY data ./data

# Exposer le port
EXPOSE ${STREAMLIT_PORT}

# Variables d'environnement pour Streamlit
ENV STREAMLIT_SERVER_PORT=${STREAMLIT_PORT}
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV API_URL=http://backend:8000

# Lancer Streamlit avec Home.py comme point d'entrée
CMD ["streamlit", "run", "src/app/Home.py", "--server.port=8501", "--server.address=0.0.0.0"]
