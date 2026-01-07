import { useState, useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { map as mapApi, dimensions } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { MapPin, Flame, Filter, X, Eye, Radar, LayoutDashboard, Shapes, Info, Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Link } from 'react-router-dom';

export default function MapExplorer() {
  const [mapMode, setMapMode] = useState<'markers' | 'heatmap'>('markers');
  const [showFilters, setShowFilters] = useState(true);
  const [filters, setFilters] = useState({
    country: '',
    date_from: '',
    date_to: '',
  });
  const mapRef = useRef<HTMLDivElement>(null);
  const [mapLoaded, setMapLoaded] = useState(false);
  const leafletMapRef = useRef<any>(null);

  const { data: shapesData } = useQuery({
    queryKey: ['dimensions-shapes'],
    queryFn: () => dimensions.shapes(),
  });

  const { data: pointsData, isLoading: pointsLoading } = useQuery({
    queryKey: ['map-points', filters],
    queryFn: () =>
      mapApi.points({
        country: filters.country || undefined,
        date_from: filters.date_from || undefined,
        date_to: filters.date_to || undefined,
      }),
  });

  const shapes = shapesData?.data || [];
  const points = pointsData?.data || [];

  // Initialize Leaflet map
  useEffect(() => {
    if (!mapRef.current || leafletMapRef.current) return;

    const initMap = async () => {
      const L = await import('leaflet');
      await import('leaflet/dist/leaflet.css');

      const map = L.map(mapRef.current!, {
        center: [20, 0],
        zoom: 2,
        zoomControl: true,
        minZoom: 2,
        maxZoom: 18,
        worldCopyJump: true,
      });

      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 20,
      }).addTo(map);

      leafletMapRef.current = map;
      setMapLoaded(true);
    };

    initMap();

    return () => {
      if (leafletMapRef.current) {
        leafletMapRef.current.remove();
        leafletMapRef.current = null;
      }
    };
  }, []);

  // Update markers when points change
  useEffect(() => {
    if (!leafletMapRef.current || !mapLoaded) return;

    const map = leafletMapRef.current;
    
    // Import Leaflet dynamically
    import('leaflet').then((L) => {
      // Clear existing markers
      map.eachLayer((layer: any) => {
        if (layer instanceof L.CircleMarker) {
          map.removeLayer(layer);
        }
      });

      // Add new markers
      points.forEach((point: any) => {
        const lat = point.latitude || point.lat;
        const lng = point.longitude || point.lng;
        
        if (!lat || !lng || isNaN(lat) || isNaN(lng)) return;

        const marker = L.circleMarker([lat, lng], {
          radius: 6,
          fillColor: '#00F5A0',
          fillOpacity: 0.8,
          color: '#7C3AED',
          weight: 2,
          opacity: 1,
        });

        marker.bindPopup(`
          <div style="padding: 8px; min-width: 150px;">
            <p style="font-weight: 600; color: #00F5A0; margin: 0 0 4px 0;">UFO Sighting</p>
            <p style="font-size: 12px; color: #888; margin: 0;">Shape: ${point.shape || 'Unknown'}</p>
            ${point.city ? `<p style="font-size: 12px; color: #888; margin: 4px 0 0 0;">${point.city}, ${point.country}</p>` : ''}
          </div>
        `);

        marker.addTo(map);
      });
    });
  }, [points, mapLoaded]);

  const clearFilters = () => {
    setFilters({ country: '', date_from: '', date_to: '' });
  };

  return (
    <div className="h-screen w-screen fixed inset-0 overflow-hidden bg-[#0a0a0f]">
      {/* Map container */}
      <div ref={mapRef} className="absolute inset-0 w-full h-full z-0" />
      
      {/* Loading overlay */}
      {!mapLoaded && (
        <div className="absolute inset-0 flex items-center justify-center bg-background/80 z-10">
          <div className="flex flex-col items-center gap-4">
            <Loader2 className="w-12 h-12 animate-spin text-primary" />
            <p className="text-muted-foreground">Loading map...</p>
          </div>
        </div>
      )}

      {/* Header overlay */}
      <div className="absolute top-4 left-4 z-[1000] flex items-center gap-3">
        <div className="glass-card px-4 py-3 rounded-xl flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-primary to-accent flex items-center justify-center neon-glow">
            <Radar className="w-5 h-5 text-primary-foreground" />
          </div>
          <div>
            <h1 className="font-display text-lg font-bold tracking-wider text-gradient">
              ATAY UFO
            </h1>
            <p className="text-xs text-muted-foreground">Analytics Map</p>
          </div>
        </div>
      </div>

      {/* Stats overlay */}
      <div className="absolute top-4 right-4 z-[1000]">
        <div className="glass-card px-4 py-2 rounded-lg flex items-center gap-2">
          <div className="w-2 h-2 bg-accent rounded-full animate-pulse" />
          <span className="text-sm">
            {pointsLoading ? 'Loading...' : `${points.length.toLocaleString()} sightings`}
          </span>
        </div>
      </div>

      {/* Filters Panel */}
      <div
        className={cn(
          'absolute top-20 left-4 z-[1000] glass-card rounded-xl transition-all duration-300',
          showFilters ? 'w-72 p-4' : 'w-auto p-2'
        )}
      >
        <div className="flex items-center justify-between mb-4">
          {showFilters && (
            <div className="flex items-center gap-2">
              <Filter className="w-4 h-4 text-primary" />
              <h3 className="font-semibold">Filters</h3>
            </div>
          )}
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setShowFilters(!showFilters)}
          >
            {showFilters ? <X className="w-4 h-4" /> : <Filter className="w-4 h-4" />}
          </Button>
        </div>

        {showFilters && (
          <div className="space-y-4 animate-fade-in">
            {/* Map Mode */}
            <div>
              <Label className="text-sm mb-2 block">Display Mode</Label>
              <div className="flex gap-2">
                <Button
                  variant={mapMode === 'markers' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setMapMode('markers')}
                  className="flex-1"
                >
                  <MapPin className="w-4 h-4 mr-1" />
                  Markers
                </Button>
                <Button
                  variant={mapMode === 'heatmap' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setMapMode('heatmap')}
                  className="flex-1"
                >
                  <Flame className="w-4 h-4 mr-1" />
                  Heatmap
                </Button>
              </div>
            </div>

            {/* Country Filter */}
            <div>
              <Label className="text-sm mb-2 block">Country</Label>
              <Input
                placeholder="e.g., us"
                value={filters.country}
                onChange={(e) => setFilters({ ...filters, country: e.target.value })}
              />
            </div>

            {/* Date Range */}
            <div>
              <Label className="text-sm mb-2 block">Date Range</Label>
              <div className="space-y-2">
                <Input
                  type="date"
                  value={filters.date_from}
                  onChange={(e) => setFilters({ ...filters, date_from: e.target.value })}
                />
                <Input
                  type="date"
                  value={filters.date_to}
                  onChange={(e) => setFilters({ ...filters, date_to: e.target.value })}
                />
              </div>
            </div>

            <Button variant="outline" size="sm" onClick={clearFilters} className="w-full">
              Clear Filters
            </Button>

            {/* Stats */}
            <div className="pt-4 border-t border-border">
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Eye className="w-4 h-4" />
                <span>
                  {pointsLoading ? 'Loading...' : `${points.length.toLocaleString()} points`}
                </span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Navigation links */}
      <div className="absolute bottom-4 left-4 z-[1000] flex gap-2 flex-wrap">
        <Link 
          to="/dashboard" 
          className="glass-card px-4 py-2 rounded-lg text-sm hover:bg-secondary/50 transition-colors flex items-center gap-2"
        >
          <LayoutDashboard className="w-4 h-4" />
          Dashboard
        </Link>
        <Link 
          to="/observations" 
          className="glass-card px-4 py-2 rounded-lg text-sm hover:bg-secondary/50 transition-colors flex items-center gap-2"
        >
          <Eye className="w-4 h-4" />
          Observations
        </Link>
        <Link 
          to="/dimensions" 
          className="glass-card px-4 py-2 rounded-lg text-sm hover:bg-secondary/50 transition-colors flex items-center gap-2"
        >
          <Shapes className="w-4 h-4" />
          Dimensions
        </Link>
        <Link 
          to="/about" 
          className="glass-card px-4 py-2 rounded-lg text-sm hover:bg-secondary/50 transition-colors flex items-center gap-2"
        >
          <Info className="w-4 h-4" />
          About
        </Link>
      </div>

      {/* Map info overlay */}
      <div className="absolute bottom-4 right-4 z-[1000] glass-card px-4 py-2 rounded-lg">
        <p className="text-sm font-medium">
          {mapMode === 'heatmap' ? 'Heatmap View' : 'Marker View'}
        </p>
        <p className="text-xs text-muted-foreground">
          {points.length.toLocaleString()} observations
        </p>
      </div>
    </div>
  );
}
