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

# Lancer Streamlit
CMD ["streamlit", "run", "src/app/main.py", "--server.port=${STREAMLIT_PORT}", "--server.address=0.0.0.0"]
