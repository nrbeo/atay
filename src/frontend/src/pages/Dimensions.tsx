import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { dimensions } from '@/lib/api';
import { LoadingState } from '@/components/ui/loading-spinner';
import { ChartCard } from '@/components/dashboard/ChartCard';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Shapes, Cloud, MapPin, Radio, ChevronLeft, ChevronRight, Search } from 'lucide-react';
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
} from 'recharts';
import { cn } from '@/lib/utils';

const COLORS = ['#7C3AED', '#00F5A0', '#F59E0B', '#EF4444', '#06B6D4', '#8B5CF6', '#EC4899', '#10B981'];

export default function Dimensions() {
  const [activeTab, setActiveTab] = useState('shapes');
  const [locationPage, setLocationPage] = useState(1);
  const [stationPage, setStationPage] = useState(1);
  const [locationSearch, setLocationSearch] = useState('');
  const pageSize = 15;

  const { data: shapesData, isLoading: shapesLoading } = useQuery({
    queryKey: ['dimensions-shapes'],
    queryFn: () => dimensions.shapes(),
  });

  const { data: frshttData, isLoading: frshttLoading } = useQuery({
    queryKey: ['dimensions-frshtt'],
    queryFn: () => dimensions.frshtt(),
  });

  const { data: locationsData, isLoading: locationsLoading } = useQuery({
    queryKey: ['dimensions-locations', locationPage, locationSearch],
    queryFn: () =>
      dimensions.locations({
        limit: pageSize,
        offset: (locationPage - 1) * pageSize,
        city: locationSearch || undefined,
      }),
  });

  const { data: stationsData, isLoading: stationsLoading } = useQuery({
    queryKey: ['dimensions-stations', stationPage],
    queryFn: () => dimensions.weatherStations({ 
      limit: pageSize, 
      offset: (stationPage - 1) * pageSize 
    }),
  });

  const shapes = shapesData?.data || [];
  const frshtt = frshttData?.data || [];
  const locations = locationsData?.data || [];
  const stations = stationsData?.data || [];
  const locationTotal = locations.length;
  const stationTotal = stations.length;

  // Process shapes for chart
  const shapeChartData = shapes.slice(0, 10).map((s: any) => ({
    name: s.shape || s.shape_key || 'Unknown',
    value: s.count || 1,
  }));

  // Process FRSHTT for bar chart
  const weatherChartData = frshtt.slice(0, 8).map((f: any) => ({
    pattern: f.frshtt_key || f.pattern || 'Unknown',
    count: f.count || 1,
  }));

  const tabItems = [
    { value: 'shapes', label: 'Shapes', icon: Shapes },
    { value: 'weather', label: 'Weather Patterns', icon: Cloud },
    { value: 'locations', label: 'Locations', icon: MapPin },
    { value: 'stations', label: 'Weather Stations', icon: Radio },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold font-display tracking-wide text-gradient">
          Dimensions
        </h1>
        <p className="text-muted-foreground mt-1">
          Explore the reference data behind UFO sightings
        </p>
      </div>

      {/* Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
        <TabsList className="glass-card p-1 h-auto flex-wrap">
          {tabItems.map((tab) => (
            <TabsTrigger
              key={tab.value}
              value={tab.value}
              className={cn(
                'flex items-center gap-2 px-4 py-2 data-[state=active]:bg-primary data-[state=active]:text-primary-foreground'
              )}
            >
              <tab.icon className="w-4 h-4" />
              {tab.label}
            </TabsTrigger>
          ))}
        </TabsList>

        {/* Shapes Tab */}
        <TabsContent value="shapes" className="space-y-6">
          {shapesLoading ? (
            <LoadingState message="Loading shapes..." />
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Shapes Chart */}
              <ChartCard title="Shape Distribution" subtitle="Top 10 reported shapes">
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie
                        data={shapeChartData}
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={120}
                        paddingAngle={2}
                        dataKey="value"
                        label={({ name }) => name}
                      >
                        {shapeChartData.map((entry: any, index: number) => (
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

              {/* Shapes Table */}
              <ChartCard title="All Shapes" subtitle={`${shapes.length} unique shapes`}>
                <div className="max-h-80 overflow-y-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="hover:bg-transparent">
                        <TableHead>Shape</TableHead>
                        <TableHead className="text-right">Count</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {shapes.map((shape: any, i: number) => (
                        <TableRow key={i} className="border-border/30">
                          <TableCell className="font-medium">
                            <div className="flex items-center gap-2">
                              <div
                                className="w-3 h-3 rounded-full"
                                style={{ backgroundColor: COLORS[i % COLORS.length] }}
                              />
                              {shape.shape || shape.shape_key || 'Unknown'}
                            </div>
                          </TableCell>
                          <TableCell className="text-right">
                            {(shape.count || 0).toLocaleString()}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </ChartCard>
            </div>
          )}
        </TabsContent>

        {/* Weather Patterns Tab */}
        <TabsContent value="weather" className="space-y-6">
          {frshttLoading ? (
            <LoadingState message="Loading weather patterns..." />
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Weather Chart */}
              <ChartCard title="FRSHTT Patterns" subtitle="Weather condition codes">
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={weatherChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="hsl(0 0% 20%)" />
                      <XAxis dataKey="pattern" stroke="hsl(0 0% 60%)" fontSize={10} />
                      <YAxis stroke="hsl(0 0% 60%)" fontSize={12} />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: 'hsl(0 0% 10%)',
                          border: '1px solid hsl(0 0% 20%)',
                          borderRadius: '8px',
                        }}
                      />
                      <Bar dataKey="count" fill="#00F5A0" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </ChartCard>

              {/* Weather Info */}
              <ChartCard title="Weather Codes Legend" subtitle="FRSHTT meaning">
                <div className="grid grid-cols-2 gap-3">
                  {[
                    { code: 'F', label: 'Fog', color: 'text-blue-400' },
                    { code: 'R', label: 'Rain', color: 'text-cyan-400' },
                    { code: 'S', label: 'Snow', color: 'text-slate-200' },
                    { code: 'H', label: 'Hail', color: 'text-indigo-400' },
                    { code: 'T', label: 'Thunder', color: 'text-yellow-400' },
                    { code: 'T', label: 'Tornado', color: 'text-red-400' },
                  ].map((item, i) => (
                    <div key={i} className="p-3 bg-secondary/50 rounded-lg">
                      <span className={cn('text-xl font-bold font-display', item.color)}>
                        {item.code}
                      </span>
                      <p className="text-sm text-muted-foreground mt-1">{item.label}</p>
                    </div>
                  ))}
                </div>
              </ChartCard>
            </div>
          )}
        </TabsContent>

        {/* Locations Tab */}
        <TabsContent value="locations" className="space-y-6">
          <ChartCard
            title="Locations"
            subtitle={`${locationTotal.toLocaleString()} unique locations`}
          >
            {/* Search */}
            <div className="flex gap-2 mb-4">
              <Input
                placeholder="Search by city..."
                value={locationSearch}
                onChange={(e) => {
                  setLocationSearch(e.target.value);
                  setLocationPage(1);
                }}
                className="max-w-xs"
              />
              <Button variant="secondary" size="icon">
                <Search className="w-4 h-4" />
              </Button>
            </div>

            {locationsLoading ? (
              <LoadingState message="Loading locations..." />
            ) : (
              <>
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead>City</TableHead>
                      <TableHead>Country</TableHead>
                      <TableHead>Coordinates</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {locations.map((loc: any, i: number) => (
                      <TableRow key={i} className="border-border/30">
                        <TableCell className="font-medium">{loc.city || 'Unknown'}</TableCell>
                        <TableCell>{loc.country || 'Unknown'}</TableCell>
                        <TableCell className="text-muted-foreground text-sm">
                          {loc.latitude ? `${loc.latitude.toFixed(2)}, ${loc.longitude.toFixed(2)}` : 'N/A'}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>

                {/* Pagination */}
                <div className="flex items-center justify-between mt-4 pt-4 border-t border-border/50">
                  <p className="text-sm text-muted-foreground">
                    Page {locationPage} of {Math.ceil(locationTotal / pageSize)}
                  </p>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setLocationPage((p) => Math.max(1, p - 1))}
                      disabled={locationPage === 1}
                    >
                      <ChevronLeft className="w-4 h-4" />
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setLocationPage((p) => p + 1)}
                      disabled={locationPage >= Math.ceil(locationTotal / pageSize)}
                    >
                      <ChevronRight className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              </>
            )}
          </ChartCard>
        </TabsContent>

        {/* Weather Stations Tab */}
        <TabsContent value="stations" className="space-y-6">
          <ChartCard
            title="Weather Stations"
            subtitle={`${stationTotal.toLocaleString()} stations`}
          >
            {stationsLoading ? (
              <LoadingState message="Loading stations..." />
            ) : (
              <>
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead>Station ID</TableHead>
                      <TableHead>Name</TableHead>
                      <TableHead>Location</TableHead>
                      <TableHead>Elevation</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {stations.map((station: any, i: number) => (
                      <TableRow key={i} className="border-border/30">
                        <TableCell className="font-mono text-sm">
                          {station.station_id || station.station_key}
                        </TableCell>
                        <TableCell className="font-medium">
                          {station.name || 'Unknown'}
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {station.country || 'Unknown'}
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {station.elevation ? `${station.elevation}m` : 'N/A'}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>

                {/* Pagination */}
                <div className="flex items-center justify-between mt-4 pt-4 border-t border-border/50">
                  <p className="text-sm text-muted-foreground">
                    Page {stationPage} of {Math.ceil(stationTotal / pageSize)}
                  </p>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setStationPage((p) => Math.max(1, p - 1))}
                      disabled={stationPage === 1}
                    >
                      <ChevronLeft className="w-4 h-4" />
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setStationPage((p) => p + 1)}
                      disabled={stationPage >= Math.ceil(stationTotal / pageSize)}
                    >
                      <ChevronRight className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              </>
            )}
          </ChartCard>
        </TabsContent>
      </Tabs>
    </div>
  );
}
