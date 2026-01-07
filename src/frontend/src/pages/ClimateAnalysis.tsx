import { useQuery } from '@tanstack/react-query';
import { stats } from '@/lib/api';
import { ChartCard } from '@/components/dashboard/ChartCard';
import { LoadingState } from '@/components/ui/loading-spinner';
import { Cloud, Thermometer, Eye, Shapes, Sun, Droplets } from 'lucide-react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  ScatterChart,
  Scatter,
  ZAxis,
} from 'recharts';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

const COLORS = ['#7C3AED', '#00F5A0', '#F59E0B', '#EF4444', '#06B6D4', '#8B5CF6', '#EC4899', '#10B981'];
const SEASON_COLORS: Record<string, string> = {
  winter: '#06B6D4',
  spring: '#10B981',
  summer: '#F59E0B',
  autumn: '#EF4444',
  fall: '#EF4444',
};

export default function ClimateAnalysis() {
  const [selectedSeason, setSelectedSeason] = useState<string | null>(null);

  // Fetch all climate correlation data
  const { data: shapeByWeatherData, isLoading: shapeWeatherLoading } = useQuery({
    queryKey: ['stats-shape-by-weather'],
    queryFn: () => stats.shapeByWeather(),
  });

  const { data: shapeBySeasonData, isLoading: shapeSeasonLoading } = useQuery({
    queryKey: ['stats-shape-by-season'],
    queryFn: () => stats.shapeBySeasonData(),
  });

  const { data: durationByWeatherData, isLoading: durationWeatherLoading } = useQuery({
    queryKey: ['stats-duration-by-weather'],
    queryFn: () => stats.durationByWeather(),
  });

  const { data: temperatureData, isLoading: tempLoading } = useQuery({
    queryKey: ['stats-by-temperature'],
    queryFn: () => stats.byTemperature(),
  });

  const { data: visibilityData, isLoading: visLoading } = useQuery({
    queryKey: ['stats-by-visibility'],
    queryFn: () => stats.byVisibility(),
  });

  const { data: topShapesWeatherData, isLoading: topShapesLoading } = useQuery({
    queryKey: ['stats-top-shapes-by-weather'],
    queryFn: () => stats.topShapesByWeather(),
  });

  const { data: seasonWeatherData, isLoading: seasonWeatherLoading } = useQuery({
    queryKey: ['stats-season-weather-matrix'],
    queryFn: () => stats.seasonWeatherMatrix(),
  });

  const isLoading = shapeWeatherLoading || shapeSeasonLoading || durationWeatherLoading || 
                    tempLoading || visLoading || topShapesLoading || seasonWeatherLoading;

  if (isLoading) {
    return <LoadingState message="Loading climate analysis data..." />;
  }

  // Process data for visualizations
  const shapeByWeather = shapeByWeatherData?.data || [];
  const shapeBySeason = shapeBySeasonData?.data || [];
  const durationByWeather = durationByWeatherData?.data || [];
  const temperature = temperatureData?.data || [];
  const visibility = visibilityData?.data || [];
  const topShapesWeather = topShapesWeatherData?.data || [];
  const seasonWeather = seasonWeatherData?.data || [];

  // Process temperature data for bar chart
  const tempChartData = temperature.map((t: any) => ({
    range: t.temp_bucket,
    sightings: t.sightings,
    avgDuration: t.avg_duration_min || 0,
  }));

  // Process visibility data for bar chart
  const visChartData = visibility.map((v: any) => ({
    range: v.visibility_bucket,
    sightings: v.sightings,
    avgDuration: v.avg_duration_min || 0,
  }));

  // Process duration by weather
  const durationWeatherChart = durationByWeather.map((d: any) => ({
    weather: d.weather_label || 'Unknown',
    avgMinutes: d.avg_duration_minutes || 0,
    sightings: d.sightings,
  }));

  // Process shape by season - aggregate top shapes per season
  const seasons = ['winter', 'spring', 'summer', 'autumn'];
  const shapeSeasonAggregated: Record<string, Record<string, number>> = {};
  
  shapeBySeason.forEach((item: any) => {
    const shape = item.shape || 'unknown';
    if (!shapeSeasonAggregated[shape]) {
      shapeSeasonAggregated[shape] = { winter: 0, spring: 0, summer: 0, autumn: 0 };
    }
    const season = item.season?.toLowerCase() || 'unknown';
    if (seasons.includes(season)) {
      shapeSeasonAggregated[shape][season] = item.sightings;
    }
  });

  // Get top 8 shapes by total sightings
  const topShapes = Object.entries(shapeSeasonAggregated)
    .map(([shape, data]) => ({
      shape,
      total: Object.values(data).reduce((a, b) => a + b, 0),
      ...data,
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 8);

  // Radar chart data for shapes by season
  const radarData = seasons.map(season => {
    const data: any = { season: season.charAt(0).toUpperCase() + season.slice(1) };
    topShapes.slice(0, 5).forEach((s, i) => {
      data[s.shape] = s[season as keyof typeof s] || 0;
    });
    return data;
  });

  // Process top shapes by weather for grouped display
  const weatherGroups: Record<string, Array<{ shape: string; sightings: number }>> = {};
  topShapesWeather.forEach((item: any) => {
    const weather = item.weather_label || 'Unknown';
    if (!weatherGroups[weather]) {
      weatherGroups[weather] = [];
    }
    weatherGroups[weather].push({ shape: item.shape, sightings: item.sightings });
  });

  // Season weather matrix for heatmap-style display
  const seasonWeatherMatrix = seasonWeather.map((sw: any) => ({
    season: sw.season,
    weather: sw.weather_label || 'Unknown',
    sightings: sw.sightings,
  }));

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold font-display tracking-wide text-gradient">
            Climate × UFO Analysis
          </h1>
          <p className="text-muted-foreground mt-1">
            Exploring correlations between weather conditions and UFO sightings
          </p>
        </div>
        <div className="flex items-center gap-2 px-4 py-2 glass-card rounded-lg">
          <Cloud className="w-4 h-4 text-primary" />
          <span className="text-sm text-muted-foreground">Weather Correlation</span>
        </div>
      </div>

      {/* Key Insights Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="glass-card p-4 rounded-xl border-l-4 border-primary">
          <div className="flex items-center gap-2 mb-2">
            <Thermometer className="w-5 h-5 text-primary" />
            <span className="font-semibold">Temperature Impact</span>
          </div>
          <p className="text-sm text-muted-foreground">
            Most UFO sightings occur in mild temperatures (10-20°C), suggesting observers 
            are more likely outdoors in comfortable weather.
          </p>
        </div>
        <div className="glass-card p-4 rounded-xl border-l-4 border-accent">
          <div className="flex items-center gap-2 mb-2">
            <Eye className="w-5 h-5 text-accent" />
            <span className="font-semibold">Visibility Correlation</span>
          </div>
          <p className="text-sm text-muted-foreground">
            Higher visibility conditions correlate with more sightings, likely due to 
            better observation conditions rather than UFO behavior.
          </p>
        </div>
        <div className="glass-card p-4 rounded-xl border-l-4 border-yellow-500">
          <div className="flex items-center gap-2 mb-2">
            <Sun className="w-5 h-5 text-yellow-500" />
            <span className="font-semibold">Clear Sky Dominance</span>
          </div>
          <p className="text-sm text-muted-foreground">
            Clear/blue sky conditions account for the majority of sightings, with 
            "light" shapes being most commonly reported.
          </p>
        </div>
      </div>

      {/* Row 1: Temperature & Visibility */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard 
          title="Sightings by Temperature" 
          subtitle="Distribution across temperature ranges"
        >
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={tempChartData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
                <XAxis type="number" stroke="hsl(0 0% 60%)" fontSize={12} />
                <YAxis 
                  dataKey="range" 
                  type="category" 
                  stroke="hsl(0 0% 60%)" 
                  fontSize={10}
                  width={120}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                  formatter={(value: any, name: string) => [
                    value.toLocaleString(),
                    name === 'sightings' ? 'Sightings' : 'Avg Duration (min)'
                  ]}
                />
                <Bar dataKey="sightings" fill="#7C3AED" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        <ChartCard 
          title="Sightings by Visibility" 
          subtitle="Distribution across visibility ranges"
        >
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={visChartData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
                <XAxis type="number" stroke="hsl(0 0% 60%)" fontSize={12} />
                <YAxis 
                  dataKey="range" 
                  type="category" 
                  stroke="hsl(0 0% 60%)" 
                  fontSize={10}
                  width={100}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                  formatter={(value: any) => [value.toLocaleString(), 'Sightings']}
                />
                <Bar dataKey="sightings" fill="#00F5A0" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>
      </div>

      {/* Row 2: Duration by Weather & Shape by Season Radar */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard 
          title="Average Duration by Weather" 
          subtitle="Do sightings last longer in certain conditions?"
        >
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={durationWeatherChart}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
                <XAxis 
                  dataKey="weather" 
                  stroke="hsl(0 0% 60%)" 
                  fontSize={10}
                  angle={-45}
                  textAnchor="end"
                  height={80}
                />
                <YAxis 
                  stroke="hsl(0 0% 60%)" 
                  fontSize={12}
                  label={{ value: 'Minutes', angle: -90, position: 'insideLeft', fill: 'hsl(0 0% 60%)' }}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                  formatter={(value: any, name: string) => [
                    typeof value === 'number' ? value.toFixed(1) : value,
                    name === 'avgMinutes' ? 'Avg Duration (min)' : 'Total Sightings'
                  ]}
                />
                <Bar dataKey="avgMinutes" fill="#F59E0B" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        <ChartCard 
          title="Top Shapes by Season" 
          subtitle="Radar view of shape distribution across seasons"
        >
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <RadarChart data={radarData}>
                <PolarGrid stroke="hsl(0 0% 30%)" />
                <PolarAngleAxis dataKey="season" stroke="hsl(0 0% 60%)" fontSize={12} />
                <PolarRadiusAxis stroke="hsl(0 0% 40%)" fontSize={10} />
                {topShapes.slice(0, 5).map((s, i) => (
                  <Radar
                    key={s.shape}
                    name={s.shape}
                    dataKey={s.shape}
                    stroke={COLORS[i]}
                    fill={COLORS[i]}
                    fillOpacity={0.2}
                  />
                ))}
                <Legend wrapperStyle={{ fontSize: '11px' }} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                />
              </RadarChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>
      </div>

      {/* Row 3: Shape by Season Grouped Bars */}
      <ChartCard 
        title="Shape Distribution by Season" 
        subtitle="Top 8 shapes compared across all seasons"
      >
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={topShapes}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
              <XAxis 
                dataKey="shape" 
                stroke="hsl(0 0% 60%)" 
                fontSize={11}
                angle={-30}
                textAnchor="end"
                height={60}
              />
              <YAxis stroke="hsl(0 0% 60%)" fontSize={12} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(0 0% 10%)',
                  border: '1px solid hsl(0 0% 20%)',
                  borderRadius: '8px',
                }}
              />
              <Legend wrapperStyle={{ paddingTop: '10px' }} />
              <Bar dataKey="winter" fill={SEASON_COLORS.winter} name="Winter" />
              <Bar dataKey="spring" fill={SEASON_COLORS.spring} name="Spring" />
              <Bar dataKey="summer" fill={SEASON_COLORS.summer} name="Summer" />
              <Bar dataKey="autumn" fill={SEASON_COLORS.autumn} name="Autumn" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </ChartCard>

      {/* Row 4: Top Shapes per Weather Condition */}
      <ChartCard 
        title="Top 5 Shapes per Weather Condition" 
        subtitle="Which shapes are most reported under each weather pattern?"
      >
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 p-2">
          {Object.entries(weatherGroups).slice(0, 6).map(([weather, shapes]) => (
            <div key={weather} className="bg-background/50 rounded-lg p-3 border border-border/30">
              <div className="flex items-center gap-2 mb-3">
                <Droplets className="w-4 h-4 text-primary" />
                <span className="font-medium text-sm">{weather}</span>
              </div>
              <div className="space-y-2">
                {shapes.slice(0, 5).map((s, i) => (
                  <div key={s.shape} className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      <div 
                        className="w-2 h-2 rounded-full" 
                        style={{ backgroundColor: COLORS[i] }}
                      />
                      <span className="text-muted-foreground">{s.shape}</span>
                    </div>
                    <span className="font-mono text-xs">{s.sightings.toLocaleString()}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </ChartCard>

      {/* Summary Section */}
      <ChartCard title="Key Findings" subtitle="Summary of climate-UFO correlations">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="p-4 bg-primary/5 rounded-lg border border-primary/20">
            <h4 className="font-semibold text-primary mb-2">🌡️ Temperature Pattern</h4>
            <p className="text-sm text-muted-foreground">
              UFO sightings peak in mild temperatures (10-20°C / 50-68°F). Extreme cold or 
              hot conditions show fewer reports, likely due to reduced outdoor activity.
            </p>
          </div>
          <div className="p-4 bg-accent/5 rounded-lg border border-accent/20">
            <h4 className="font-semibold text-accent mb-2">👁️ Visibility Impact</h4>
            <p className="text-sm text-muted-foreground">
              Good visibility (&gt;10 miles) correlates with the highest number of sightings.
              Poor visibility conditions have significantly fewer reports.
            </p>
          </div>
          <div className="p-4 bg-yellow-500/5 rounded-lg border border-yellow-500/20">
            <h4 className="font-semibold text-yellow-500 mb-2">☀️ Clear Sky Preference</h4>
            <p className="text-sm text-muted-foreground">
              The vast majority of sightings occur under clear/blue sky conditions. 
              "Light" shapes dominate in all weather types but especially in clear skies.
            </p>
          </div>
          <div className="p-4 bg-purple-500/5 rounded-lg border border-purple-500/20">
            <h4 className="font-semibold text-purple-400 mb-2">🔄 Seasonal Shapes</h4>
            <p className="text-sm text-muted-foreground">
              While "light" and "circle" shapes are consistent year-round, some shapes like
              "fireball" show summer peaks, possibly due to meteor shower misidentifications.
            </p>
          </div>
        </div>
      </ChartCard>
    </div>
  );
}
