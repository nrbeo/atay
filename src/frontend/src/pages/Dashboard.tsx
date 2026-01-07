import { useQuery } from '@tanstack/react-query';
import { stats, dimensions, ufo } from '@/lib/api';
import { StatCard } from '@/components/dashboard/StatCard';
import { ChartCard } from '@/components/dashboard/ChartCard';
import { LoadingState } from '@/components/ui/loading-spinner';
import { Eye, Shapes, MapPin, Cloud, TrendingUp, Calendar } from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
  Legend,
} from 'recharts';

const COLORS = ['#7C3AED', '#00F5A0', '#F59E0B', '#EF4444', '#06B6D4', '#8B5CF6', '#EC4899', '#10B981'];

export default function Dashboard() {
  const { data: overview, isLoading: overviewLoading } = useQuery({
    queryKey: ['stats-overview'],
    queryFn: () => stats.overview(),
  });

  const { data: shapesData, isLoading: shapesLoading } = useQuery({
    queryKey: ['stats-by-shape'],
    queryFn: () => stats.byShape(),
  });

  const { data: monthlyData, isLoading: monthlyLoading } = useQuery({
    queryKey: ['stats-monthly'],
    queryFn: () => stats.timeSeriesMonthly(),
  });

  const { data: seasonData, isLoading: seasonLoading } = useQuery({
    queryKey: ['stats-by-season'],
    queryFn: () => stats.bySeason(),
  });

  const { data: durationData, isLoading: durationLoading } = useQuery({
    queryKey: ['stats-duration'],
    queryFn: () => stats.durationDistribution(),
  });

  const { data: topCountries, isLoading: countriesLoading } = useQuery({
    queryKey: ['stats-top-countries'],
    queryFn: () => stats.topCountries({ limit: 10 }),
  });

  const isLoading = overviewLoading || shapesLoading || monthlyLoading || seasonLoading || durationLoading;

  if (isLoading) {
    return <LoadingState message="Loading analytics data..." />;
  }

  const overviewData = overview?.data;
  const shapes = shapesData?.data || [];
  const monthly = monthlyData?.data || [];
  const seasons = seasonData?.data || [];
  const durations = durationData?.data || [];
  const countries = topCountries?.data || [];

  // Process shapes for pie chart (top 8)
  // Backend returns: { shape, shape_category, sightings }
  const topShapes = shapes.slice(0, 8).map((s: any) => ({
    name: s.shape || 'Unknown',
    value: s.sightings,
  }));

  // Process seasons for pie chart
  // Backend returns: { season, sightings }
  const seasonChartData = seasons.map((s: any) => ({
    name: s.season,
    value: s.sightings,
  }));

  // Process monthly data for line chart
  // Backend returns: { year, month, observations }
  const monthlyChartData = monthly.slice(-24).map((m: any) => ({
    month: `${m.year}-${String(m.month).padStart(2, '0')}`,
    observations: m.observations,
  }));

  // Process duration data
  // Backend returns: { duration_bucket, count }
  const durationChartData = durations.slice(0, 10).map((d: any) => ({
    range: d.duration_bucket,
    count: d.count,
  }));

  // Process countries data
  // Backend returns: { country, sightings }
  const countriesChartData = countries.slice(0, 8).map((c: any) => ({
    country: c.country,
    observations: c.sightings,
  }));

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold font-display tracking-wide text-gradient">
            Dashboard
          </h1>
          <p className="text-muted-foreground mt-1">
            UFO sightings analytics overview
          </p>
        </div>
        <div className="flex items-center gap-2 px-4 py-2 glass-card rounded-lg">
          <div className="w-2 h-2 bg-accent rounded-full animate-pulse" />
          <span className="text-sm text-muted-foreground">Live Data</span>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Total Observations"
          value={overviewData?.total_observations || 0}
          icon={Eye}
          delay={0}
          accentColor="primary"
        />
        <StatCard
          title="Unique Shapes"
          value={overviewData?.total_shapes || shapes.length}
          icon={Shapes}
          delay={100}
          accentColor="accent"
        />
        <StatCard
          title="Locations Covered"
          value={overviewData?.total_locations || 0}
          icon={MapPin}
          delay={200}
          accentColor="primary"
        />
        <StatCard
          title="Weather Stations"
          value={overviewData?.total_stations || 0}
          icon={Cloud}
          delay={300}
          accentColor="accent"
        />
      </div>

      {/* Charts Row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Monthly Observations */}
        <ChartCard title="Observations Over Time" subtitle="Monthly trend analysis">
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={monthlyChartData}>
                <defs>
                  <linearGradient id="colorObs" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#7C3AED" stopOpacity={0.4} />
                    <stop offset="95%" stopColor="#7C3AED" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
                <XAxis
                  dataKey="month"
                  stroke="hsl(0 0% 60%)"
                  fontSize={12}
                  tickLine={false}
                />
                <YAxis stroke="hsl(0 0% 60%)" fontSize={12} tickLine={false} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="observations"
                  stroke="#7C3AED"
                  strokeWidth={2}
                  dot={false}
                  fill="url(#colorObs)"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        {/* Shape Distribution */}
        <ChartCard title="Shape Distribution" subtitle="Most reported UFO shapes">
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={topShapes}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={2}
                  dataKey="value"
                >
                  {topShapes.map((entry: any, index: number) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                />
                <Legend
                  layout="vertical"
                  verticalAlign="middle"
                  align="right"
                  wrapperStyle={{ fontSize: '12px' }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>
      </div>

      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Season Distribution */}
        <ChartCard title="By Season" subtitle="Seasonal patterns">
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={seasonChartData}
                  cx="50%"
                  cy="50%"
                  outerRadius={80}
                  dataKey="value"
                  label={({ name, percent }) =>
                    `${name} ${(percent * 100).toFixed(0)}%`
                  }
                  labelLine={false}
                >
                  {seasonChartData.map((entry: any, index: number) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        {/* Duration Distribution */}
        <ChartCard title="Duration Distribution" subtitle="Sighting duration ranges">
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={durationChartData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
                <XAxis type="number" stroke="hsl(0 0% 60%)" fontSize={12} />
                <YAxis
                  dataKey="range"
                  type="category"
                  stroke="hsl(0 0% 60%)"
                  fontSize={10}
                  width={80}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(0 0% 10%)',
                    border: '1px solid hsl(0 0% 20%)',
                    borderRadius: '8px',
                  }}
                />
                <Bar dataKey="count" fill="#00F5A0" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        {/* Top Countries */}
        <ChartCard title="Top Countries" subtitle="Most sightings by country">
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={countriesChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
                <XAxis
                  dataKey="country"
                  stroke="hsl(0 0% 60%)"
                  fontSize={10}
                  angle={-45}
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
                <Bar dataKey="observations" fill="#7C3AED" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>
      </div>

      {/* Insights Section */}
      <ChartCard title="Key Insights" subtitle="Highlights from the data">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="p-4 bg-primary/5 rounded-lg border border-primary/20">
            <div className="flex items-center gap-2 mb-2">
              <TrendingUp className="w-4 h-4 text-primary" />
              <span className="text-sm font-medium text-primary">Peak Activity</span>
            </div>
            <p className="text-sm text-muted-foreground">
              Most UFO sightings occur during summer months, with July and August showing the highest activity.
            </p>
          </div>
          <div className="p-4 bg-accent/5 rounded-lg border border-accent/20">
            <div className="flex items-center gap-2 mb-2">
              <Shapes className="w-4 h-4 text-accent" />
              <span className="text-sm font-medium text-accent">Common Shapes</span>
            </div>
            <p className="text-sm text-muted-foreground">
              "Light" and "Circle" are the most frequently reported UFO shapes across all observations.
            </p>
          </div>
          <div className="p-4 bg-primary/5 rounded-lg border border-primary/20">
            <div className="flex items-center gap-2 mb-2">
              <Calendar className="w-4 h-4 text-primary" />
              <span className="text-sm font-medium text-primary">Duration Pattern</span>
            </div>
            <p className="text-sm text-muted-foreground">
              Most sightings last between 1-5 minutes, with very few exceeding 30 minutes duration.
            </p>
          </div>
        </div>
      </ChartCard>
    </div>
  );
}
