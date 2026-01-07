"""
ATAY UFO Analytics - Streamlit Observations Browser
Equivalent to React frontend Observations page
"""
import streamlit as st
import pandas as pd
from datetime import date
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api_client import ufo, dimensions


def render_observations():
    """Render the observations browser page"""
    
    # Header
    st.title("👁️ UFO Observations")
    st.caption("Browse and explore UFO sighting reports")
    
    st.divider()
    
    # Load shapes for filter dropdown
    shapes_data = dimensions.shapes()
    shapes = shapes_data.get("data", [])
    
    # ============ FILTERS ============
    with st.expander("🔍 Filters", expanded=True):
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            # City search
            city_search = st.text_input(
                "Search City",
                placeholder="Enter city name...",
                key="city_search"
            )
            
            # Country filter
            country_filter = st.text_input(
                "Country",
                placeholder="e.g., USA",
                key="country_filter"
            )
        
        with filter_col2:
            # Shape filter
            shape_options = ["All shapes"] + [s.get('shape', str(s.get('shape_key', ''))) for s in shapes]
            selected_shape = st.selectbox(
                "Shape",
                options=shape_options,
                key="shape_filter"
            )
            
            # Get shape_key if a specific shape is selected
            shape_key = None
            if selected_shape != "All shapes":
                for s in shapes:
                    if s.get('shape') == selected_shape or str(s.get('shape_key')) == selected_shape:
                        shape_key = s.get('shape_key')
                        break
        
        with filter_col3:
            # Date range
            date_from = st.date_input(
                "From Date",
                value=None,
                key="date_from"
            )
            
            date_to = st.date_input(
                "To Date",
                value=None,
                key="date_to"
            )
        
        # Clear filters
        if st.button("🗑️ Clear Filters", use_container_width=True):
            st.rerun()
    
    # ============ PAGINATION STATE ============
    if 'page' not in st.session_state:
        st.session_state.page = 1
    
    page_size = 20
    offset = (st.session_state.page - 1) * page_size
    
    # ============ FETCH DATA ============
    with st.spinner("Loading observations..."):
        params = {
            "limit": page_size,
            "offset": offset
        }
        
        if city_search:
            params["city"] = city_search
        if country_filter:
            params["country"] = country_filter
        if shape_key:
            params["shape_key"] = shape_key
        if date_from:
            params["date_from"] = date_from.strftime("%Y-%m-%d")
        if date_to:
            params["date_to"] = date_to.strftime("%Y-%m-%d")
        
        observations_data = ufo.observations(**params)
    
    observations = observations_data.get("data", [])
    total_items = len(observations)
    
    # Stats
    st.markdown(f"""
    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 16px;">
        <span style="color: #7C3AED;">👁️</span>
        <span>{total_items:,} records loaded</span>
    </div>
    """, unsafe_allow_html=True)
    
    # ============ DATA TABLE ============
    if observations:
        # Convert to DataFrame
        df = pd.DataFrame(observations)
        
        # Prepare display columns
        display_df = pd.DataFrame()
        display_df['📅 Date'] = df.get('date_time', df.get('date', 'Unknown'))
        display_df['📍 Location'] = df.apply(
            lambda x: f"{x.get('city', 'Unknown')}, {x.get('country', 'Unknown')}", axis=1
        )
        display_df['🔷 Shape'] = df.get('shape', 'Unknown')
        
        # Duration formatting
        if 'duration_seconds' in df.columns:
            display_df['⏱️ Duration'] = df['duration_seconds'].apply(
                lambda x: f"{int(x/60)} min" if pd.notna(x) and x > 0 else "Unknown"
            )
        else:
            display_df['⏱️ Duration'] = df.get('duration', 'Unknown')
        
        # Store fact_id for detail view
        if 'fact_id' in df.columns:
            display_df['_fact_id'] = df['fact_id']
        
        # Display the table
        st.dataframe(
            display_df.drop(columns=['_fact_id'] if '_fact_id' in display_df.columns else []),
            use_container_width=True,
            height=500,
            hide_index=True
        )
        
        # ============ PAGINATION ============
        st.divider()
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.button("⬅️ Previous", disabled=st.session_state.page <= 1, use_container_width=True):
                st.session_state.page -= 1
                st.rerun()
        
        with col2:
            st.markdown(
                f"<p style='text-align: center; color: #888;'>Page {st.session_state.page}</p>",
                unsafe_allow_html=True
            )
        
        with col3:
            if st.button("Next ➡️", disabled=total_items < page_size, use_container_width=True):
                st.session_state.page += 1
                st.rerun()
        
        # ============ DETAIL VIEW ============
        st.divider()
        st.subheader("📋 Observation Details")
        st.caption("Select an observation to view details")
        
        if 'fact_id' in df.columns:
            # Create selection dropdown
            observation_options = [
                f"{row.get('date_time', row.get('date', 'Unknown'))} - {row.get('city', 'Unknown')}, {row.get('country', 'Unknown')} ({row.get('shape', 'Unknown')})"
                for _, row in df.iterrows()
            ]
            
            selected_idx = st.selectbox(
                "Select an observation",
                range(len(observation_options)),
                format_func=lambda x: observation_options[x],
                key="selected_observation"
            )
            
            if selected_idx is not None and selected_idx < len(df):
                selected_obs = df.iloc[selected_idx]
                fact_id = selected_obs.get('fact_id')
                
                if fact_id:
                    # Fetch detail
                    with st.spinner("Loading details..."):
                        detail_data = ufo.observation_detail(str(fact_id))
                    
                    detail = detail_data.get("data", selected_obs.to_dict())
                    
                    # Display detail card
                    detail_col1, detail_col2 = st.columns(2)
                    
                    with detail_col1:
                        st.markdown(f"""
                        <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 16px; margin-bottom: 12px;">
                            <p style="color: #888; font-size: 12px; margin-bottom: 4px;">Date & Time</p>
                            <p style="font-weight: 500; margin: 0;">{detail.get('date_time', selected_obs.get('date_time', 'Unknown'))}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        st.markdown(f"""
                        <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 16px; margin-bottom: 12px;">
                            <p style="color: #888; font-size: 12px; margin-bottom: 4px;">Location</p>
                            <p style="font-weight: 500; margin: 0;">{detail.get('city', selected_obs.get('city', 'Unknown'))}, {detail.get('country', selected_obs.get('country', 'Unknown'))}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with detail_col2:
                        shape_val = detail.get('shape', selected_obs.get('shape', 'Unknown'))
                        st.markdown(f"""
                        <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 16px; margin-bottom: 12px;">
                            <p style="color: #888; font-size: 12px; margin-bottom: 4px;">Shape</p>
                            <span style="background: rgba(124,58,237,0.2); color: #7C3AED; padding: 4px 12px; border-radius: 16px; font-size: 14px;">{shape_val}</span>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        duration_val = detail.get('duration_seconds', selected_obs.get('duration_seconds', 0))
                        if duration_val and duration_val > 0:
                            duration_str = f"{int(duration_val / 60)} minutes"
                        else:
                            duration_str = detail.get('duration', selected_obs.get('duration', 'Unknown'))
                        
                        st.markdown(f"""
                        <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 16px; margin-bottom: 12px;">
                            <p style="color: #888; font-size: 12px; margin-bottom: 4px;">Duration</p>
                            <p style="font-weight: 500; margin: 0;">{duration_str}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Comment section
                    comment = detail.get('comment', selected_obs.get('comment', ''))
                    if comment:
                        st.markdown(f"""
                        <div style="background: rgba(124,58,237,0.1); border: 1px solid rgba(124,58,237,0.3); border-radius: 8px; padding: 16px; margin-top: 12px;">
                            <p style="color: #7C3AED; font-weight: 600; margin-bottom: 8px;">💬 Witness Report</p>
                            <p style="color: #aaa; font-size: 14px; line-height: 1.6; margin: 0;">{comment}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Coordinates
                    lat = detail.get('latitude', selected_obs.get('latitude'))
                    lng = detail.get('longitude', selected_obs.get('longitude'))
                    if lat and lng:
                        st.markdown(f"""
                        <p style="color: #888; font-size: 12px; margin-top: 16px;">
                            📍 Coordinates: Lat {lat}, Lng {lng}
                        </p>
                        """, unsafe_allow_html=True)
    else:
        st.info("No observations found with the current filters. Try adjusting your search criteria.")


# Run when page is loaded
render_observations()
