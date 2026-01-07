"""
ATAY UFO Analytics - Streamlit Dashboard
Equivalent to React frontend Dashboard page
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from api_client import stats, dimensions

# Color palette matching React frontend
COLORS = ['#7C3AED', '#00F5A0', '#F59E0B', '#EF4444', '#06B6D4', '#8B5CF6', '#EC4899', '#10B981']


def render_dashboard():
    """Render the main dashboard page"""
    
    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📊 Dashboard")
        st.caption("UFO sightings analytics overview")
    with col2:
        st.markdown("""
        <div style="background: rgba(0,245,160,0.1); border-radius: 8px; padding: 8px 16px; text-align: center;">
            <span style="color: #00F5A0;">●</span> <span style="color: #888;">Live Data</span>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Load data
    with st.spinner("Loading analytics data..."):
        overview_data = stats.overview()
        shapes_data = stats.by_shape()
        monthly_data = stats.time_series_monthly()
        season_data = stats.by_season()
        duration_data = stats.duration_distribution()
        countries_data = stats.top_countries(limit=10)
    
    overview = overview_data.get("data", {})
    shapes = shapes_data.get("data", [])
    monthly = monthly_data.get("data", [])
    seasons = season_data.get("data", [])
    durations = duration_data.get("data", [])
    countries = countries_data.get("data", [])
    
    # ============ KPI CARDS ============
    st.subheader("Key Metrics")
    
    kpi_cols = st.columns(4)
    
    with kpi_cols[0]:
        st.metric(
            label="👁️ Total Observations",
            value=f"{overview.get('total_observations', 0):,}"
        )
    
    with kpi_cols[1]:
        st.metric(
            label="🔷 Unique Shapes",
            value=f"{overview.get('total_shapes', len(shapes)):,}"
        )
    
    with kpi_cols[2]:
        st.metric(
            label="📍 Locations Covered",
            value=f"{overview.get('total_locations', 0):,}"
        )
    
    with kpi_cols[3]:
        st.metric(
            label="🌤️ Weather Stations",
            value=f"{overview.get('total_stations', 0):,}"
        )
    
    st.divider()
    
    # ============ CHARTS ROW 1 ============
    chart_col1, chart_col2 = st.columns(2)
    
    # Monthly Observations Line Chart
    with chart_col1:
        st.subheader("📈 Observations Over Time")
        st.caption("Monthly trend analysis")
        
        if monthly:
            # Process monthly data for chart (last 24 months)
            monthly_df = pd.DataFrame(monthly[-24:])
            monthly_df['month_str'] = monthly_df.apply(
                lambda x: f"{x['year']}-{str(x['month']).zfill(2)}", axis=1
            )
            
            fig = px.line(
                monthly_df,
                x='month_str',
                y='observations',
                markers=False,
                color_discrete_sequence=[COLORS[0]]
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="Month",
                yaxis_title="Observations",
                margin=dict(l=0, r=0, t=10, b=0),
                height=350
            )
            fig.update_traces(fill='tozeroy', fillcolor='rgba(124,58,237,0.2)')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No monthly data available")
    
    # Shape Distribution Pie Chart
    with chart_col2:
        st.subheader("🔷 Shape Distribution")
        st.caption("Most reported UFO shapes")
        
        if shapes:
            # Process shapes for pie chart (top 8)
            shapes_df = pd.DataFrame(shapes[:8])
            shapes_df['name'] = shapes_df.get('shape', shapes_df.get('shape_key', 'Unknown'))
            shapes_df['value'] = shapes_df.get('sightings', shapes_df.get('count', 0))
            
            fig = px.pie(
                shapes_df,
                values='value',
                names='name',
                hole=0.5,
                color_discrete_sequence=COLORS
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=0, r=0, t=10, b=0),
                height=350,
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="right", x=1.3)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No shape data available")
    
    # ============ CHARTS ROW 2 ============
    st.divider()
    
    chart_col3, chart_col4, chart_col5 = st.columns(3)
    
    # Season Distribution
    with chart_col3:
        st.subheader("🌸 By Season")
        st.caption("Seasonal patterns")
        
        if seasons:
            seasons_df = pd.DataFrame(seasons)
            seasons_df['name'] = seasons_df.get('season', 'Unknown')
            seasons_df['value'] = seasons_df.get('sightings', 0)
            
            fig = px.pie(
                seasons_df,
                values='value',
                names='name',
                color_discrete_sequence=COLORS
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=0, r=0, t=10, b=0),
                height=300,
                showlegend=True
            )
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No season data available")
    
    # Duration Distribution
    with chart_col4:
        st.subheader("⏱️ Duration Distribution")
        st.caption("Sighting duration ranges")
        
        if durations:
            duration_df = pd.DataFrame(durations[:10])
            duration_df['range'] = duration_df.get('duration_bucket', 'Unknown')
            duration_df['count'] = duration_df.get('count', 0)
            
            fig = px.bar(
                duration_df,
                y='range',
                x='count',
                orientation='h',
                color_discrete_sequence=['#00F5A0']
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="Count",
                yaxis_title="",
                margin=dict(l=0, r=0, t=10, b=0),
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No duration data available")
    
    # Top Countries
    with chart_col5:
        st.subheader("🌍 Top Countries")
        st.caption("Most sightings by country")
        
        if countries:
            countries_df = pd.DataFrame(countries[:8])
            countries_df['country'] = countries_df.get('country', 'Unknown')
            countries_df['observations'] = countries_df.get('sightings', 0)
            
            fig = px.bar(
                countries_df,
                x='country',
                y='observations',
                color_discrete_sequence=[COLORS[0]]
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="",
                yaxis_title="Observations",
                margin=dict(l=0, r=0, t=10, b=0),
                height=300,
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No country data available")
    
    # ============ KEY INSIGHTS ============
    st.divider()
    st.subheader("💡 Key Insights")
    st.caption("Highlights from the data")
    
    insight_cols = st.columns(3)
    
    with insight_cols[0]:
        st.markdown("""
        <div style="background: rgba(124,58,237,0.1); border-left: 4px solid #7C3AED; border-radius: 8px; padding: 16px;">
            <p style="color: #7C3AED; font-weight: 600; margin-bottom: 8px;">📈 Peak Activity</p>
            <p style="color: #888; font-size: 14px; margin: 0;">
                Most UFO sightings occur during summer months, with July and August showing the highest activity.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with insight_cols[1]:
        st.markdown("""
        <div style="background: rgba(0,245,160,0.1); border-left: 4px solid #00F5A0; border-radius: 8px; padding: 16px;">
            <p style="color: #00F5A0; font-weight: 600; margin-bottom: 8px;">🔷 Common Shapes</p>
            <p style="color: #888; font-size: 14px; margin: 0;">
                "Light" and "Circle" are the most frequently reported UFO shapes across all observations.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with insight_cols[2]:
        st.markdown("""
        <div style="background: rgba(124,58,237,0.1); border-left: 4px solid #7C3AED; border-radius: 8px; padding: 16px;">
            <p style="color: #7C3AED; font-weight: 600; margin-bottom: 8px;">📅 Duration Pattern</p>
            <p style="color: #888; font-size: 14px; margin: 0;">
                Most sightings last between 1-5 minutes, with very few exceeding 30 minutes duration.
            </p>
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    render_dashboard()
