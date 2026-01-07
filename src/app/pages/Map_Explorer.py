"""
ATAY UFO Analytics - Streamlit Map Explorer
Equivalent to React frontend MapExplorer page
"""
import streamlit as st
import pandas as pd
import pydeck as pdk
from datetime import datetime, date
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api_client import map_api, dimensions


def render_map_explorer():
    """Render the interactive map explorer page"""
    
    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("🗺️ UFO Map Explorer")
        st.caption("Interactive visualization of UFO sightings worldwide")
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Map mode selection
    map_mode = st.sidebar.radio(
        "Display Mode",
        options=["Markers", "Heatmap"],
        horizontal=True
    )
    
    # Country filter
    country_filter = st.sidebar.text_input(
        "Country Code",
        placeholder="e.g., us, gb, ca",
        help="Enter a 2-letter country code to filter sightings"
    )
    
    # Date range filters
    st.sidebar.subheader("Date Range")
    
    date_from = st.sidebar.date_input(
        "From",
        value=None,
        min_value=date(1900, 1, 1),
        max_value=date.today()
    )
    
    date_to = st.sidebar.date_input(
        "To",
        value=None,
        min_value=date(1900, 1, 1),
        max_value=date.today()
    )
    
    # Max points slider
    max_points = st.sidebar.slider(
        "Max Points",
        min_value=100,
        max_value=10000,
        value=2000,
        step=100,
        help="Limit the number of points displayed for performance"
    )
    
    # Clear filters button
    if st.sidebar.button("🗑️ Clear Filters", use_container_width=True):
        st.rerun()
    
    # Build filter parameters
    params = {"max_points": max_points}
    if country_filter:
        params["country"] = country_filter.lower()
    if date_from:
        params["date_from"] = date_from.strftime("%Y-%m-%d")
    if date_to:
        params["date_to"] = date_to.strftime("%Y-%m-%d")
    
    # Load map data
    with st.spinner("Loading map data..."):
        if map_mode == "Heatmap":
            points_data = map_api.heatmap(**params)
        else:
            points_data = map_api.points(**params)
    
    points = points_data.get("data", [])
    
    # Stats display
    with col2:
        st.metric("Total Sightings", f"{len(points):,}")
    
    if not points:
        st.warning("No sightings found with the current filters. Try adjusting your criteria.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(points)
    
    # Ensure we have lat/lng columns
    if 'latitude' in df.columns:
        df['lat'] = df['latitude']
    if 'longitude' in df.columns:
        df['lng'] = df['longitude']
    
    # Filter out invalid coordinates
    df = df.dropna(subset=['lat', 'lng'])
    df = df[(df['lat'].between(-90, 90)) & (df['lng'].between(-180, 180))]
    
    if df.empty:
        st.warning("No valid coordinates found in the data.")
        return
    
    # Calculate map center
    center_lat = df['lat'].mean()
    center_lng = df['lng'].mean()
    
    # Create the map based on mode
    if map_mode == "Heatmap":
        # Heatmap layer
        layer = pdk.Layer(
            "HeatmapLayer",
            data=df,
            get_position=['lng', 'lat'],
            get_weight=1,
            opacity=0.8,
            threshold=0.05,
            radiusPixels=30,
            colorRange=[
                [0, 245, 160, 50],
                [0, 245, 160, 100],
                [124, 58, 237, 150],
                [124, 58, 237, 200],
                [239, 68, 68, 255]
            ]
        )
        
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lng,
            zoom=2,
            pitch=0
        )
    else:
        # Scatter plot layer (markers)
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=['lng', 'lat'],
            get_fill_color=[0, 245, 160, 180],
            get_line_color=[124, 58, 237, 255],
            line_width_min_pixels=2,
            get_radius=30000,
            radius_min_pixels=4,
            radius_max_pixels=15,
            pickable=True,
            auto_highlight=True
        )
        
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lng,
            zoom=2,
            pitch=0
        )
    
    # Create the deck
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/dark-v11",
        tooltip={
            "html": """
            <div style="padding: 8px; background: #1a1a1f; border-radius: 8px; border: 1px solid #333;">
                <b style="color: #00F5A0;">UFO Sighting</b><br/>
                <span style="color: #888;">Shape: {shape}</span><br/>
                <span style="color: #888;">Location: {city}, {country}</span>
            </div>
            """,
            "style": {
                "backgroundColor": "transparent",
                "color": "white"
            }
        }
    )
    
    # Render the map
    st.pydeck_chart(deck, use_container_width=True, height=600)
    
    # Map legend
    st.divider()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 12px;">
            <p style="color: #00F5A0; font-weight: 600; margin: 0;">🗺️ {map_mode} View</p>
            <p style="color: #888; font-size: 12px; margin: 4px 0 0 0;">{len(df):,} observations displayed</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        if country_filter:
            st.markdown(f"""
            <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 12px;">
                <p style="color: #7C3AED; font-weight: 600; margin: 0;">🌍 Country Filter</p>
                <p style="color: #888; font-size: 12px; margin: 4px 0 0 0;">{country_filter.upper()}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 12px;">
                <p style="color: #7C3AED; font-weight: 600; margin: 0;">🌍 Worldwide</p>
                <p style="color: #888; font-size: 12px; margin: 4px 0 0 0;">All countries</p>
            </div>
            """, unsafe_allow_html=True)
    
    with col3:
        if date_from or date_to:
            date_range_str = f"{date_from or 'Any'} to {date_to or 'Any'}"
            st.markdown(f"""
            <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 12px;">
                <p style="color: #F59E0B; font-weight: 600; margin: 0;">📅 Date Range</p>
                <p style="color: #888; font-size: 12px; margin: 4px 0 0 0;">{date_range_str}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 12px;">
                <p style="color: #F59E0B; font-weight: 600; margin: 0;">📅 All Time</p>
                <p style="color: #888; font-size: 12px; margin: 4px 0 0 0;">No date filter</p>
            </div>
            """, unsafe_allow_html=True)


# Run when page is loaded
render_map_explorer()
