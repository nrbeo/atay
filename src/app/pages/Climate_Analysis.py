"""
ATAY UFO Analytics - Streamlit Climate Analysis
Equivalent to React frontend ClimateAnalysis page
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api_client import stats

# Color palettes
COLORS = ['#7C3AED', '#00F5A0', '#F59E0B', '#EF4444', '#06B6D4', '#8B5CF6', '#EC4899', '#10B981']
SEASON_COLORS = {
    'winter': '#06B6D4',
    'spring': '#10B981', 
    'summer': '#F59E0B',
    'autumn': '#EF4444',
    'fall': '#EF4444'
}


def render_climate_analysis():
    """Render the climate analysis page"""
    
    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("🌡️ Climate × UFO Analysis")
        st.caption("Exploring correlations between weather conditions and UFO sightings")
    with col2:
        st.markdown("""
        <div style="background: rgba(124,58,237,0.1); border-radius: 8px; padding: 8px 16px; text-align: center;">
            <span style="color: #7C3AED;">☁️</span> <span style="color: #888;">Weather Correlation</span>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Load all climate data
    with st.spinner("Loading climate analysis data..."):
        temp_data = stats.by_temperature()
        vis_data = stats.by_visibility()
        duration_weather_data = stats.duration_by_weather()
        shape_season_data = stats.shape_by_season()
        top_shapes_weather_data = stats.top_shapes_by_weather()
        season_weather_data = stats.season_weather_matrix()
    
    temperature = temp_data.get("data", [])
    visibility = vis_data.get("data", [])
    duration_by_weather = duration_weather_data.get("data", [])
    shape_by_season = shape_season_data.get("data", [])
    top_shapes_weather = top_shapes_weather_data.get("data", [])
    season_weather = season_weather_data.get("data", [])
    
    # ============ KEY INSIGHTS CARDS ============
    st.subheader("💡 Key Insights")
    
    insight_cols = st.columns(3)
    
    with insight_cols[0]:
        st.markdown("""
        <div style="background: rgba(30,30,35,0.8); border-left: 4px solid #7C3AED; border-radius: 8px; padding: 16px;">
            <p style="color: #7C3AED; font-weight: 600; margin-bottom: 8px;">🌡️ Temperature Impact</p>
            <p style="color: #888; font-size: 14px; margin: 0;">
                Most UFO sightings occur in mild temperatures (10-20°C), suggesting observers 
                are more likely outdoors in comfortable weather.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with insight_cols[1]:
        st.markdown("""
        <div style="background: rgba(30,30,35,0.8); border-left: 4px solid #00F5A0; border-radius: 8px; padding: 16px;">
            <p style="color: #00F5A0; font-weight: 600; margin-bottom: 8px;">👁️ Visibility Correlation</p>
            <p style="color: #888; font-size: 14px; margin: 0;">
                Higher visibility conditions correlate with more sightings, likely due to 
                better observation conditions rather than UFO behavior.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with insight_cols[2]:
        st.markdown("""
        <div style="background: rgba(30,30,35,0.8); border-left: 4px solid #F59E0B; border-radius: 8px; padding: 16px;">
            <p style="color: #F59E0B; font-weight: 600; margin-bottom: 8px;">☀️ Clear Sky Dominance</p>
            <p style="color: #888; font-size: 14px; margin: 0;">
                Clear/blue sky conditions account for the majority of sightings, with 
                "light" shapes being most commonly reported.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # ============ ROW 1: Temperature & Visibility ============
    chart_col1, chart_col2 = st.columns(2)
    
    # Temperature chart
    with chart_col1:
        st.subheader("🌡️ Sightings by Temperature")
        st.caption("Distribution across temperature ranges")
        
        if temperature:
            temp_df = pd.DataFrame(temperature)
            temp_df['range'] = temp_df.get('temp_bucket', 'Unknown')
            temp_df['sightings'] = temp_df.get('sightings', 0)
            
            fig = px.bar(
                temp_df,
                y='range',
                x='sightings',
                orientation='h',
                color_discrete_sequence=[COLORS[0]]
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="Sightings",
                yaxis_title="",
                margin=dict(l=0, r=0, t=10, b=0),
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No temperature data available")
    
    # Visibility chart
    with chart_col2:
        st.subheader("👁️ Sightings by Visibility")
        st.caption("Distribution across visibility ranges")
        
        if visibility:
            vis_df = pd.DataFrame(visibility)
            vis_df['range'] = vis_df.get('visibility_bucket', 'Unknown')
            vis_df['sightings'] = vis_df.get('sightings', 0)
            
            fig = px.bar(
                vis_df,
                y='range',
                x='sightings',
                orientation='h',
                color_discrete_sequence=['#00F5A0']
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="Sightings",
                yaxis_title="",
                margin=dict(l=0, r=0, t=10, b=0),
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No visibility data available")
    
    st.divider()
    
    # ============ ROW 2: Duration by Weather & Shape by Season Radar ============
    chart_col3, chart_col4 = st.columns(2)
    
    # Duration by weather
    with chart_col3:
        st.subheader("⏱️ Average Duration by Weather")
        st.caption("Do sightings last longer in certain conditions?")
        
        if duration_by_weather:
            dur_df = pd.DataFrame(duration_by_weather)
            dur_df['weather'] = dur_df.get('weather_label', 'Unknown')
            dur_df['avgMinutes'] = dur_df.get('avg_duration_minutes', 0)
            
            fig = px.bar(
                dur_df,
                x='weather',
                y='avgMinutes',
                color_discrete_sequence=['#F59E0B']
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="",
                yaxis_title="Minutes",
                margin=dict(l=0, r=0, t=10, b=0),
                height=350,
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No duration by weather data available")
    
    # Shape by season radar
    with chart_col4:
        st.subheader("📊 Top Shapes by Season")
        st.caption("Radar view of shape distribution across seasons")
        
        if shape_by_season:
            # Process shape by season data
            shape_season_df = pd.DataFrame(shape_by_season)
            
            # Aggregate data by shape and season
            seasons = ['winter', 'spring', 'summer', 'autumn']
            shape_agg = {}
            
            for _, row in shape_season_df.iterrows():
                shape = row.get('shape', 'unknown')
                season = str(row.get('season', '')).lower()
                sightings = row.get('sightings', 0)
                
                if shape not in shape_agg:
                    shape_agg[shape] = {'winter': 0, 'spring': 0, 'summer': 0, 'autumn': 0, 'total': 0}
                
                if season in seasons:
                    shape_agg[shape][season] = sightings
                    shape_agg[shape]['total'] += sightings
            
            # Get top 5 shapes
            top_shapes = sorted(shape_agg.items(), key=lambda x: x[1]['total'], reverse=True)[:5]
            
            if top_shapes:
                # Create radar chart
                fig = go.Figure()
                
                for i, (shape, data) in enumerate(top_shapes):
                    values = [data[s] for s in seasons]
                    values.append(values[0])  # Close the shape
                    
                    fig.add_trace(go.Scatterpolar(
                        r=values,
                        theta=['Winter', 'Spring', 'Summer', 'Autumn', 'Winter'],
                        fill='toself',
                        name=shape,
                        line_color=COLORS[i],
                        fillcolor=f'rgba({int(COLORS[i][1:3], 16)}, {int(COLORS[i][3:5], 16)}, {int(COLORS[i][5:7], 16)}, 0.2)'
                    ))
                
                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    polar=dict(
                        radialaxis=dict(visible=True, gridcolor='rgba(255,255,255,0.1)'),
                        angularaxis=dict(gridcolor='rgba(255,255,255,0.1)')
                    ),
                    margin=dict(l=40, r=40, t=40, b=40),
                    height=350,
                    showlegend=True
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough data for radar chart")
        else:
            st.info("No shape by season data available")
    
    st.divider()
    
    # ============ ROW 3: Shape Distribution by Season (Grouped Bar) ============
    st.subheader("📊 Shape Distribution by Season")
    st.caption("Top 8 shapes compared across all seasons")
    
    if shape_by_season:
        shape_season_df = pd.DataFrame(shape_by_season)
        
        # Aggregate data
        seasons = ['winter', 'spring', 'summer', 'autumn']
        shape_agg = {}
        
        for _, row in shape_season_df.iterrows():
            shape = row.get('shape', 'unknown')
            season = str(row.get('season', '')).lower()
            sightings = row.get('sightings', 0)
            
            if shape not in shape_agg:
                shape_agg[shape] = {'shape': shape, 'winter': 0, 'spring': 0, 'summer': 0, 'autumn': 0, 'total': 0}
            
            if season in seasons:
                shape_agg[shape][season] = sightings
                shape_agg[shape]['total'] += sightings
        
        # Get top 8 shapes
        top_shapes_list = sorted(shape_agg.values(), key=lambda x: x['total'], reverse=True)[:8]
        
        if top_shapes_list:
            bar_df = pd.DataFrame(top_shapes_list)
            
            fig = go.Figure()
            
            for i, season in enumerate(seasons):
                fig.add_trace(go.Bar(
                    name=season.capitalize(),
                    x=bar_df['shape'],
                    y=bar_df[season],
                    marker_color=SEASON_COLORS.get(season, COLORS[i])
                ))
            
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                barmode='group',
                xaxis_title="",
                yaxis_title="Sightings",
                margin=dict(l=0, r=0, t=10, b=0),
                height=400,
                xaxis_tickangle=-30,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough data for grouped bar chart")
    else:
        st.info("No shape by season data available")
    
    st.divider()
    
    # ============ ROW 4: Top Shapes per Weather Condition ============
    st.subheader("🔷 Top Shapes per Weather Condition")
    st.caption("Which shapes are most reported under each weather pattern?")
    
    if top_shapes_weather:
        # Group by weather
        weather_groups = {}
        for item in top_shapes_weather:
            weather = item.get('weather_label', 'Unknown')
            if weather not in weather_groups:
                weather_groups[weather] = []
            weather_groups[weather].append({
                'shape': item.get('shape', 'Unknown'),
                'sightings': item.get('sightings', 0)
            })
        
        # Display in grid
        weather_list = list(weather_groups.items())[:6]  # Show top 6 weather conditions
        
        for row_idx in range(0, len(weather_list), 3):
            cols = st.columns(3)
            for col_idx, col in enumerate(cols):
                if row_idx + col_idx < len(weather_list):
                    weather, shapes_list = weather_list[row_idx + col_idx]
                    
                    with col:
                        st.markdown(f"""
                        <div style="background: rgba(30,30,35,0.8); border-radius: 8px; padding: 16px; margin-bottom: 16px;">
                            <p style="color: #00F5A0; font-weight: 600; margin-bottom: 12px;">☁️ {weather}</p>
                        """, unsafe_allow_html=True)
                        
                        for i, shape_item in enumerate(shapes_list[:5]):
                            shape = shape_item['shape']
                            sightings = shape_item['sightings']
                            st.markdown(f"""
                            <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
                                <span style="color: #aaa;">{i+1}. {shape}</span>
                                <span style="color: #7C3AED;">{sightings:,}</span>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("No shapes by weather data available")


# Run when page is loaded
render_climate_analysis()
