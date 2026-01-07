"""
ATAY UFO Analytics - Streamlit Dimensions Explorer
Equivalent to React frontend Dimensions page
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api_client import dimensions, meta

# Color palette
COLORS = ['#7C3AED', '#00F5A0', '#F59E0B', '#EF4444', '#06B6D4', '#8B5CF6', '#EC4899', '#10B981']


def render_dimensions():
    """Render the dimensions explorer page"""
    
    # Header
    st.title("📦 Data Dimensions")
    st.caption("Explore the dimensional data powering the UFO analytics")
    
    st.divider()
    
    # ============ TABS FOR DIMENSIONS ============
    tab_shapes, tab_weather, tab_locations, tab_stations = st.tabs([
        "🔷 Shapes",
        "⛅ Weather (FRSHTT)",
        "📍 Locations",
        "🌤️ Weather Stations"
    ])
    
    # ============ SHAPES TAB ============
    with tab_shapes:
        st.subheader("🔷 UFO Shapes")
        st.caption("Different shapes reported in UFO sightings")
        
        with st.spinner("Loading shapes..."):
            shapes_data = dimensions.shapes()
        
        shapes = shapes_data.get("data", [])
        
        if shapes:
            # KPIs
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Shapes", f"{len(shapes):,}")
            with col2:
                total_sightings = sum(s.get('sightings', s.get('count', 0)) for s in shapes)
                st.metric("Total Sightings", f"{total_sightings:,}")
            
            st.divider()
            
            # Chart and Table side by side
            chart_col, table_col = st.columns([1, 1])
            
            with chart_col:
                st.markdown("##### Shape Distribution")
                shapes_df = pd.DataFrame(shapes[:12])
                shapes_df['name'] = shapes_df.get('shape', shapes_df.get('shape_key', 'Unknown'))
                shapes_df['value'] = shapes_df.get('sightings', shapes_df.get('count', 0))
                
                fig = px.bar(
                    shapes_df,
                    x='name',
                    y='value',
                    color='name',
                    color_discrete_sequence=COLORS
                )
                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    xaxis_title="",
                    yaxis_title="Sightings",
                    margin=dict(l=0, r=0, t=10, b=0),
                    height=400,
                    showlegend=False,
                    xaxis_tickangle=-45
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with table_col:
                st.markdown("##### All Shapes")
                display_df = pd.DataFrame([{
                    'Shape': s.get('shape', s.get('shape_key', 'Unknown')),
                    'Sightings': s.get('sightings', s.get('count', 0)),
                    'Description': s.get('description', '-')
                } for s in shapes])
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=400,
                    hide_index=True
                )
        else:
            st.info("No shapes data available")
    
    # ============ WEATHER (FRSHTT) TAB ============
    with tab_weather:
        st.subheader("⛅ Weather Conditions (FRSHTT)")
        st.caption("Weather classification system used in GSOD data")
        
        # FRSHTT explanation
        with st.expander("📖 What is FRSHTT?", expanded=False):
            st.markdown("""
            **FRSHTT** is a weather indicator system from NOAA's Global Surface Summary of the Day (GSOD):
            
            | Code | Meaning |
            |------|---------|
            | **F** | Fog |
            | **R** | Rain/Drizzle |
            | **S** | Snow/Ice Pellets |
            | **H** | Hail |
            | **T** | Thunder |
            | **T** | Tornado/Funnel Cloud |
            
            Each observation can have multiple weather conditions present simultaneously.
            """)
        
        with st.spinner("Loading weather conditions..."):
            frshtt_data = dimensions.frshtt()
        
        frshtt = frshtt_data.get("data", [])
        
        if frshtt:
            # KPIs
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Weather Categories", f"{len(frshtt):,}")
            with col2:
                total_obs = sum(w.get('observations', w.get('count', 0)) for w in frshtt)
                st.metric("Linked Observations", f"{total_obs:,}")
            
            st.divider()
            
            # Chart
            chart_col, table_col = st.columns([1, 1])
            
            with chart_col:
                st.markdown("##### Weather Distribution")
                weather_df = pd.DataFrame(frshtt[:15])
                weather_df['label'] = weather_df.get('weather_label', weather_df.get('frshtt_key', 'Unknown'))
                weather_df['value'] = weather_df.get('observations', weather_df.get('count', 0))
                
                fig = px.pie(
                    weather_df,
                    values='value',
                    names='label',
                    hole=0.4,
                    color_discrete_sequence=COLORS
                )
                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    margin=dict(l=0, r=0, t=10, b=0),
                    height=400,
                    showlegend=True
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with table_col:
                st.markdown("##### All Weather Conditions")
                display_df = pd.DataFrame([{
                    'Code': w.get('frshtt_key', 'Unknown'),
                    'Label': w.get('weather_label', '-'),
                    'Observations': w.get('observations', w.get('count', 0))
                } for w in frshtt])
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=400,
                    hide_index=True
                )
        else:
            st.info("No weather data available")
    
    # ============ LOCATIONS TAB ============
    with tab_locations:
        st.subheader("📍 Locations")
        st.caption("Geographic locations with UFO sightings")
        
        # Filters
        filter_col1, filter_col2, filter_col3 = st.columns([2, 2, 1])
        
        with filter_col1:
            country_filter = st.text_input(
                "Filter by Country",
                placeholder="e.g., USA, UK, CA",
                key="loc_country"
            )
        
        with filter_col2:
            city_filter = st.text_input(
                "Filter by City",
                placeholder="e.g., Phoenix, London",
                key="loc_city"
            )
        
        with filter_col3:
            limit = st.selectbox(
                "Limit",
                options=[50, 100, 200, 500],
                index=1,
                key="loc_limit"
            )
        
        # Pagination state
        if 'loc_offset' not in st.session_state:
            st.session_state.loc_offset = 0
        
        with st.spinner("Loading locations..."):
            locations_data = dimensions.locations(
                country=country_filter if country_filter else None,
                city=city_filter if city_filter else None,
                limit=limit,
                offset=st.session_state.loc_offset
            )
        
        locations = locations_data.get("data", [])
        
        if locations:
            # Stats
            st.metric("Locations Loaded", f"{len(locations):,}")
            
            st.divider()
            
            # Table
            display_df = pd.DataFrame([{
                'City': loc.get('city', 'Unknown'),
                'State': loc.get('state', '-'),
                'Country': loc.get('country', 'Unknown'),
                'Latitude': round(loc.get('latitude', 0), 4) if loc.get('latitude') else '-',
                'Longitude': round(loc.get('longitude', 0), 4) if loc.get('longitude') else '-',
                'Sightings': loc.get('sightings', loc.get('count', 0))
            } for loc in locations])
            
            st.dataframe(
                display_df,
                use_container_width=True,
                height=500,
                hide_index=True
            )
            
            # Pagination
            st.divider()
            pag_col1, pag_col2, pag_col3 = st.columns([1, 2, 1])
            
            with pag_col1:
                if st.button("⬅️ Previous", disabled=st.session_state.loc_offset == 0, key="loc_prev"):
                    st.session_state.loc_offset = max(0, st.session_state.loc_offset - limit)
                    st.rerun()
            
            with pag_col2:
                page_num = (st.session_state.loc_offset // limit) + 1
                st.markdown(f"<p style='text-align: center; color: #888;'>Page {page_num}</p>", unsafe_allow_html=True)
            
            with pag_col3:
                if st.button("Next ➡️", disabled=len(locations) < limit, key="loc_next"):
                    st.session_state.loc_offset += limit
                    st.rerun()
        else:
            st.info("No locations found with the current filters")
    
    # ============ WEATHER STATIONS TAB ============
    with tab_stations:
        st.subheader("🌤️ Weather Stations")
        st.caption("NOAA GSOD weather stations linked to UFO sightings")
        
        # Pagination state
        if 'station_offset' not in st.session_state:
            st.session_state.station_offset = 0
        
        station_limit = st.selectbox(
            "Limit",
            options=[50, 100, 200],
            index=1,
            key="station_limit"
        )
        
        with st.spinner("Loading weather stations..."):
            stations_data = dimensions.weather_stations(
                limit=station_limit,
                offset=st.session_state.station_offset
            )
        
        stations = stations_data.get("data", [])
        
        if stations:
            # Stats
            st.metric("Stations Loaded", f"{len(stations):,}")
            
            st.divider()
            
            # Map visualization of stations
            stations_with_coords = [s for s in stations if s.get('latitude') and s.get('longitude')]
            
            if stations_with_coords:
                st.markdown("##### Station Locations")
                
                import pydeck as pdk
                
                stations_df = pd.DataFrame(stations_with_coords)
                stations_df['lat'] = stations_df['latitude']
                stations_df['lng'] = stations_df['longitude']
                
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=stations_df,
                    get_position=['lng', 'lat'],
                    get_fill_color=[0, 245, 160, 180],
                    get_radius=50000,
                    radius_min_pixels=5,
                    radius_max_pixels=15,
                    pickable=True
                )
                
                view_state = pdk.ViewState(
                    latitude=stations_df['lat'].mean(),
                    longitude=stations_df['lng'].mean(),
                    zoom=2,
                    pitch=0
                )
                
                deck = pdk.Deck(
                    layers=[layer],
                    initial_view_state=view_state,
                    map_style="mapbox://styles/mapbox/dark-v11",
                    tooltip={"html": "<b>Station:</b> {station_name}<br/><b>ID:</b> {station_id}"}
                )
                
                st.pydeck_chart(deck, use_container_width=True, height=300)
            
            st.divider()
            
            # Table
            st.markdown("##### All Stations")
            display_df = pd.DataFrame([{
                'Station ID': s.get('station_id', 'Unknown'),
                'Name': s.get('station_name', s.get('name', '-')),
                'Country': s.get('country', '-'),
                'Latitude': round(s.get('latitude', 0), 4) if s.get('latitude') else '-',
                'Longitude': round(s.get('longitude', 0), 4) if s.get('longitude') else '-',
                'Elevation (m)': s.get('elevation', '-')
            } for s in stations])
            
            st.dataframe(
                display_df,
                use_container_width=True,
                height=400,
                hide_index=True
            )
            
            # Pagination
            st.divider()
            pag_col1, pag_col2, pag_col3 = st.columns([1, 2, 1])
            
            with pag_col1:
                if st.button("⬅️ Previous", disabled=st.session_state.station_offset == 0, key="station_prev"):
                    st.session_state.station_offset = max(0, st.session_state.station_offset - station_limit)
                    st.rerun()
            
            with pag_col2:
                page_num = (st.session_state.station_offset // station_limit) + 1
                st.markdown(f"<p style='text-align: center; color: #888;'>Page {page_num}</p>", unsafe_allow_html=True)
            
            with pag_col3:
                if st.button("Next ➡️", disabled=len(stations) < station_limit, key="station_next"):
                    st.session_state.station_offset += station_limit
                    st.rerun()
        else:
            st.info("No weather stations data available")
    
    # ============ DATABASE INFO ============
    st.divider()
    st.subheader("📊 Database Summary")
    
    with st.spinner("Loading database info..."):
        row_counts_data = meta.row_counts()
    
    row_counts = row_counts_data.get("data", {})
    
    if row_counts:
        # Display as metric cards
        cols = st.columns(4)
        tables = list(row_counts.items())[:8]
        
        for i, (table, count) in enumerate(tables):
            col_idx = i % 4
            with cols[col_idx]:
                table_name = table.replace('_', ' ').title()
                st.markdown(f"""
                <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 12px; margin-bottom: 8px; text-align: center;">
                    <p style="color: #888; font-size: 12px; margin: 0;">{table_name}</p>
                    <p style="color: #00F5A0; font-weight: 600; font-size: 20px; margin: 4px 0 0 0;">{count:,}</p>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No database info available")


# Run when page is loaded
render_dimensions()
