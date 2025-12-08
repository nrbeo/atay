import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ufo, dimensions, ObservationsParams } from '@/lib/api';
import { LoadingState } from '@/components/ui/loading-spinner';
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
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  ChevronLeft,
  ChevronRight,
  Search,
  Filter,
  Eye,
  MapPin,
  Clock,
  Calendar,
  MessageSquare,
} from 'lucide-react';
import { cn } from '@/lib/utils';

export default function Observations() {
  const [page, setPage] = useState(1);
  const [pageSize] = useState(20);
  const [selectedObservation, setSelectedObservation] = useState<any>(null);
  const [filters, setFilters] = useState({
    city: '',
    country: '',
    shape_key: undefined as number | undefined,
    date_from: '',
    date_to: '',
  });
  const [searchTerm, setSearchTerm] = useState('');

  const { data: shapesData } = useQuery({
    queryKey: ['dimensions-shapes'],
    queryFn: () => dimensions.shapes(),
  });

  const { data: observationsData, isLoading } = useQuery({
    queryKey: ['observations', page, pageSize, filters],
    queryFn: () =>
      ufo.observations({
        limit: pageSize,
        offset: (page - 1) * pageSize,
        city: filters.city || undefined,
        country: filters.country || undefined,
        shape_key: filters.shape_key || undefined,
        date_from: filters.date_from || undefined,
        date_to: filters.date_to || undefined,
      }),
  });

  const { data: detailData, isLoading: detailLoading } = useQuery({
    queryKey: ['observation-detail', selectedObservation?.fact_id],
    queryFn: () => ufo.observationDetail(selectedObservation?.fact_id),
    enabled: !!selectedObservation?.fact_id,
  });

  const shapes = shapesData?.data || [];
  const observations = observationsData?.data || [];
  const totalItems = observations.length;
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const detail = detailData?.data;

  const handleSearch = () => {
    setPage(1);
    setFilters((prev) => ({ ...prev, city: searchTerm }));
  };

  const clearFilters = () => {
    setFilters({
      city: '',
      country: '',
      shape_key: undefined,
      date_from: '',
      date_to: '',
    });
    setSearchTerm('');
    setPage(1);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold font-display tracking-wide text-gradient">
            Observations
          </h1>
          <p className="text-muted-foreground mt-1">
            Browse and explore UFO sighting reports
          </p>
        </div>
        <div className="flex items-center gap-2 px-4 py-2 glass-card rounded-lg">
          <Eye className="w-4 h-4 text-primary" />
          <span className="text-sm">{totalItems.toLocaleString()} records</span>
        </div>
      </div>

      {/* Filters */}
      <div className="glass-card rounded-xl p-4">
        <div className="flex items-center gap-2 mb-4">
          <Filter className="w-4 h-4 text-primary" />
          <h3 className="font-semibold">Filters</h3>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-4">
          {/* Search */}
          <div className="lg:col-span-2">
            <Label className="text-sm mb-2 block">Search City</Label>
            <div className="flex gap-2">
              <Input
                placeholder="Enter city name..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              />
              <Button variant="secondary" size="icon" onClick={handleSearch}>
                <Search className="w-4 h-4" />
              </Button>
            </div>
          </div>

          {/* Country */}
          <div>
            <Label className="text-sm mb-2 block">Country</Label>
            <Input
              placeholder="e.g., USA"
              value={filters.country}
              onChange={(e) => setFilters({ ...filters, country: e.target.value })}
            />
          </div>

          {/* Shape */}
          <div>
            <Label className="text-sm mb-2 block">Shape</Label>
            <Select
              value={filters.shape_key?.toString() || "all"}
              onValueChange={(v) => setFilters({ ...filters, shape_key: v === "all" ? undefined : parseInt(v) })}
            >
              <SelectTrigger>
                <SelectValue placeholder="All" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All shapes</SelectItem>
                {shapes.map((s: any) => (
                  <SelectItem key={s.shape_key} value={s.shape_key.toString()}>
                    {s.shape}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Date Range */}
          <div>
            <Label className="text-sm mb-2 block">From</Label>
            <Input
              type="date"
              value={filters.date_from}
              onChange={(e) => setFilters({ ...filters, date_from: e.target.value })}
            />
          </div>
          <div>
            <Label className="text-sm mb-2 block">To</Label>
            <Input
              type="date"
              value={filters.date_to}
              onChange={(e) => setFilters({ ...filters, date_to: e.target.value })}
            />
          </div>
        </div>

        <div className="flex justify-end mt-4">
          <Button variant="outline" size="sm" onClick={clearFilters}>
            Clear Filters
          </Button>
        </div>
      </div>

      {/* Table */}
      <div className="glass-card rounded-xl overflow-hidden">
        {isLoading ? (
          <LoadingState message="Loading observations..." />
        ) : (
          <>
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent border-border/50">
                  <TableHead className="text-muted-foreground">Date</TableHead>
                  <TableHead className="text-muted-foreground">Location</TableHead>
                  <TableHead className="text-muted-foreground">Shape</TableHead>
                  <TableHead className="text-muted-foreground">Duration</TableHead>
                  <TableHead className="text-muted-foreground">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {observations.map((obs: any, index: number) => (
                  <TableRow
                    key={obs.fact_id || index}
                    className={cn(
                      'border-border/30 cursor-pointer transition-colors',
                      'hover:bg-primary/5'
                    )}
                    style={{ animationDelay: `${index * 30}ms` }}
                  >
                    <TableCell className="font-medium">
                      <div className="flex items-center gap-2">
                        <Calendar className="w-4 h-4 text-muted-foreground" />
                        {obs.date_time || obs.date || 'Unknown'}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <MapPin className="w-4 h-4 text-accent" />
                        <span>
                          {obs.city || 'Unknown'}, {obs.country || 'Unknown'}
                        </span>
                      </div>
                    </TableCell>
                    <TableCell>
                      <span className="px-2 py-1 bg-primary/10 text-primary rounded-full text-xs font-medium">
                        {obs.shape || 'Unknown'}
                      </span>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <Clock className="w-4 h-4 text-muted-foreground" />
                        {obs.duration_seconds
                          ? `${Math.round(obs.duration_seconds / 60)} min`
                          : obs.duration || 'Unknown'}
                      </div>
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setSelectedObservation(obs)}
                      >
                        <Eye className="w-4 h-4 mr-1" />
                        View
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>

            {/* Pagination */}
            <div className="flex items-center justify-between p-4 border-t border-border/50">
              <p className="text-sm text-muted-foreground">
                Page {page} of {totalPages}
              </p>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page === 1}
                >
                  <ChevronLeft className="w-4 h-4 mr-1" />
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages}
                >
                  Next
                  <ChevronRight className="w-4 h-4 ml-1" />
                </Button>
              </div>
            </div>
          </>
        )}
      </div>

      {/* Detail Dialog */}
      <Dialog open={!!selectedObservation} onOpenChange={() => setSelectedObservation(null)}>
        <DialogContent className="glass-card max-w-2xl">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-xl">
              <Eye className="w-5 h-5 text-primary" />
              Observation Details
            </DialogTitle>
          </DialogHeader>

          {detailLoading ? (
            <LoadingState message="Loading details..." />
          ) : (
            <div className="space-y-4">
              {/* Basic Info */}
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-secondary/50 rounded-lg">
                  <p className="text-xs text-muted-foreground mb-1">Date & Time</p>
                  <p className="font-medium">
                    {detail?.date_time || selectedObservation?.date_time || 'Unknown'}
                  </p>
                </div>
                <div className="p-3 bg-secondary/50 rounded-lg">
                  <p className="text-xs text-muted-foreground mb-1">Location</p>
                  <p className="font-medium">
                    {detail?.city || selectedObservation?.city}, {detail?.country || selectedObservation?.country}
                  </p>
                </div>
                <div className="p-3 bg-secondary/50 rounded-lg">
                  <p className="text-xs text-muted-foreground mb-1">Shape</p>
                  <span className="px-2 py-1 bg-primary/10 text-primary rounded-full text-sm font-medium">
                    {detail?.shape || selectedObservation?.shape || 'Unknown'}
                  </span>
                </div>
                <div className="p-3 bg-secondary/50 rounded-lg">
                  <p className="text-xs text-muted-foreground mb-1">Duration</p>
                  <p className="font-medium">
                    {detail?.duration_seconds
                      ? `${Math.round(detail.duration_seconds / 60)} minutes`
                      : selectedObservation?.duration || 'Unknown'}
                  </p>
                </div>
              </div>

              {/* Comment */}
              {(detail?.comment || selectedObservation?.comment) && (
                <div className="p-4 bg-primary/5 border border-primary/20 rounded-lg">
                  <div className="flex items-center gap-2 mb-2">
                    <MessageSquare className="w-4 h-4 text-primary" />
                    <span className="text-sm font-medium text-primary">Witness Report</span>
                  </div>
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    {detail?.comment || selectedObservation?.comment}
                  </p>
                </div>
              )}

              {/* Coordinates */}
              {(detail?.latitude || selectedObservation?.latitude) && (
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span>
                    Lat: {detail?.latitude || selectedObservation?.latitude}
                  </span>
                  <span>
                    Lng: {detail?.longitude || selectedObservation?.longitude}
                  </span>
                </div>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
