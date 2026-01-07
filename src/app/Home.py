"""
ATAY UFO Analytics - Streamlit Application
Main entry point for the Streamlit dashboard
"""
import streamlit as st

# Page configuration
st.set_page_config(
    page_title="ATAY UFO Analytics",
    page_icon="👽",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "ATAY - UFO Sightings & Weather Correlation Analytics"
    }
)

# Custom CSS for dark theme styling
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0a0a0f 0%, #1a1a2e 100%);
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: rgba(20, 20, 30, 0.95);
        border-right: 1px solid rgba(124, 58, 237, 0.3);
    }
    
    /* Cards and containers */
    .stMetric {
        background: rgba(30, 30, 45, 0.8);
        border-radius: 12px;
        padding: 16px;
        border: 1px solid rgba(124, 58, 237, 0.2);
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #7C3AED 0%, #5B21B6 100%);
        color: white;
        border: none;
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #8B5CF6 0%, #7C3AED 100%);
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(124, 58, 237, 0.4);
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(30, 30, 45, 0.5);
        border-radius: 8px;
        padding: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        color: #888;
        border-radius: 6px;
    }
    
    .stTabs [aria-selected="true"] {
        background: rgba(124, 58, 237, 0.3);
        color: #00F5A0;
    }
    
    /* DataFrames */
    .dataframe {
        background: rgba(30, 30, 45, 0.8) !important;
        border-radius: 8px;
    }
    
    /* Expanders */
    .streamlit-expanderHeader {
        background: rgba(30, 30, 45, 0.8);
        border-radius: 8px;
    }
    
    /* Metric values */
    [data-testid="stMetricValue"] {
        color: #00F5A0;
    }
    
    /* Links */
    a {
        color: #7C3AED !important;
    }
    
    /* Dividers */
    hr {
        border-color: rgba(124, 58, 237, 0.2);
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ============ SIDEBAR ============
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 20px 0;">
        <h1 style="font-size: 48px; margin: 0;">👽</h1>
        <h2 style="color: #00F5A0; margin: 8px 0;">ATAY</h2>
        <p style="color: #888; font-size: 12px;">UFO Analytics Platform</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    st.markdown("""
    <div style="padding: 12px; background: rgba(124,58,237,0.1); border-radius: 8px; margin-bottom: 16px;">
        <p style="color: #7C3AED; font-weight: 600; margin-bottom: 8px;">📊 Data Sources</p>
        <p style="color: #888; font-size: 12px; margin: 0;">
            • NUFORC UFO Reports<br/>
            • NOAA GSOD Weather Data
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="padding: 12px; background: rgba(0,245,160,0.1); border-radius: 8px;">
        <p style="color: #00F5A0; font-weight: 600; margin-bottom: 8px;">🔗 Navigation</p>
        <p style="color: #888; font-size: 12px; margin: 0;">
            Use the pages in the sidebar to explore different analytics views.
        </p>
    </div>
    """, unsafe_allow_html=True)


# ============ MAIN PAGE (HOME) ============
st.title("👽 ATAY - UFO Analytics Platform")
st.caption("Exploring correlations between UFO sightings and weather conditions")

st.divider()

# Welcome section
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("""
    ## Welcome to ATAY
    
    **ATAY** (Analyse et Traitement pour l'Analyse des Yétis... ou plutôt des OVNI!) est une plateforme 
    d'analyse de données croisant les observations d'OVNI avec les conditions météorologiques.
    
    ### 🎯 Objectifs du projet
    - Analyser les patterns temporels et géographiques des observations d'OVNI
    - Corréler les conditions météorologiques avec les signalements
    - Visualiser les données de manière interactive
    - Identifier des tendances significatives
    
    ### 📊 Données utilisées
    - **NUFORC** : National UFO Reporting Center - Base de données d'observations
    - **NOAA GSOD** : Global Summary of the Day - Données météorologiques mondiales
    """)

with col2:
    st.markdown("""
    <div style="background: rgba(30,30,35,0.8); border-radius: 12px; padding: 20px; text-align: center;">
        <h3 style="color: #00F5A0; margin-bottom: 16px;">Quick Stats</h3>
        <div style="margin-bottom: 12px;">
            <p style="font-size: 32px; color: #7C3AED; margin: 0;">80K+</p>
            <p style="color: #888; font-size: 12px; margin: 0;">UFO Observations</p>
        </div>
        <div style="margin-bottom: 12px;">
            <p style="font-size: 32px; color: #00F5A0; margin: 0;">30+</p>
            <p style="color: #888; font-size: 12px; margin: 0;">UFO Shapes</p>
        </div>
        <div>
            <p style="font-size: 32px; color: #F59E0B; margin: 0;">20+</p>
            <p style="color: #888; font-size: 12px; margin: 0;">Years of Data</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Features grid
st.subheader("🚀 Explore the Platform")

feature_cols = st.columns(4)

features = [
    {
        "icon": "📊",
        "title": "Dashboard",
        "desc": "Vue d'ensemble avec KPIs et graphiques de tendances",
        "color": "#7C3AED"
    },
    {
        "icon": "🗺️",
        "title": "Map Explorer",
        "desc": "Carte interactive des observations mondiales",
        "color": "#00F5A0"
    },
    {
        "icon": "👁️",
        "title": "Observations",
        "desc": "Parcourir et filtrer les rapports d'observation",
        "color": "#F59E0B"
    },
    {
        "icon": "🌡️",
        "title": "Climate Analysis",
        "desc": "Corrélations météo et analyse climatique",
        "color": "#EF4444"
    }
]

for col, feature in zip(feature_cols, features):
    with col:
        st.markdown(f"""
        <div style="background: rgba(30,30,35,0.8); border-radius: 12px; padding: 20px; text-align: center; height: 180px; border: 1px solid {feature['color']}20;">
            <p style="font-size: 36px; margin: 0 0 12px 0;">{feature['icon']}</p>
            <p style="color: {feature['color']}; font-weight: 600; margin: 0 0 8px 0;">{feature['title']}</p>
            <p style="color: #888; font-size: 12px; margin: 0;">{feature['desc']}</p>
        </div>
        """, unsafe_allow_html=True)

st.divider()

# Architecture section
st.subheader("🏗️ Architecture Technique")

arch_col1, arch_col2 = st.columns(2)

with arch_col1:
    st.markdown("""
    #### Pipeline ETL
    ```
    📥 Ingestion → 🔄 Transformation → 📊 Analyse → 🎨 Visualisation
    ```
    
    - **Apache Airflow** : Orchestration des workflows
    - **PostgreSQL** : Data Warehouse avec modèle dimensionnel
    - **FastAPI** : API REST pour l'accès aux données
    - **Streamlit** : Dashboard interactif
    """)

with arch_col2:
    st.markdown("""
    #### Modèle de données
    - **Fact Table** : `fact_ufo_observation`
    - **Dimensions** :
        - `dim_date` - Calendrier
        - `dim_location` - Géographie
        - `dim_shape` - Formes d'OVNI
        - `dim_weather_station` - Stations météo
        - `dim_frshtt` - Conditions météo
    """)

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; padding: 20px; color: #888;">
    <p style="font-size: 12px;">
        ATAY UFO Analytics Platform • Built with ❤️ using Python, FastAPI & Streamlit<br/>
        Data sources: NUFORC & NOAA GSOD
    </p>
</div>
""", unsafe_allow_html=True)
