import { useQuery } from '@tanstack/react-query';
import { meta } from '@/lib/api';
import { LoadingSpinner } from '@/components/ui/loading-spinner';
import { Button } from '@/components/ui/button';
import { ChartCard } from '@/components/dashboard/ChartCard';
import {
  Radar,
  ExternalLink,
  Database,
  Server,
  FileJson,
  CheckCircle2,
  AlertCircle,
  Github,
  Mail,
} from 'lucide-react';
import { cn } from '@/lib/utils';

export default function About() {
  const { data: healthData, isLoading: healthLoading } = useQuery({
    queryKey: ['meta-health'],
    queryFn: () => meta.health(),
  });

  const { data: infoData, isLoading: infoLoading } = useQuery({
    queryKey: ['meta-info'],
    queryFn: () => meta.info(),
  });

  const { data: tablesData, isLoading: tablesLoading } = useQuery({
    queryKey: ['meta-tables'],
    queryFn: () => meta.tables(),
  });

  const { data: rowCountsData, isLoading: rowCountsLoading } = useQuery({
    queryKey: ['meta-row-counts'],
    queryFn: () => meta.rowCounts(),
  });

  const health = healthData?.data;
  const info = infoData?.data;
  const tables = tablesData?.data || [];
  const rowCounts = rowCountsData?.data || {};

  const isHealthy = health?.status === 'healthy' || health?.status === 'ok';

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="text-center max-w-2xl mx-auto">
        <div className="inline-flex items-center justify-center w-20 h-20 rounded-2xl bg-gradient-to-br from-primary to-accent mb-6 neon-glow animate-float">
          <Radar className="w-10 h-10 text-primary-foreground" />
        </div>
        <h1 className="text-4xl font-bold font-display tracking-wider text-gradient mb-4">
          ATAY UFO Analytics
        </h1>
        <p className="text-lg text-muted-foreground leading-relaxed">
          A comprehensive platform for exploring and analyzing UFO sighting data from around the
          world. Powered by advanced data engineering and modern visualization techniques.
        </p>
      </div>

      {/* Health Status */}
      <div className="flex justify-center">
        <div
          className={cn(
            'inline-flex items-center gap-3 px-6 py-3 rounded-full glass-card',
            isHealthy ? 'border-accent/30' : 'border-destructive/30'
          )}
        >
          {healthLoading ? (
            <LoadingSpinner size="sm" />
          ) : isHealthy ? (
            <CheckCircle2 className="w-5 h-5 text-accent" />
          ) : (
            <AlertCircle className="w-5 h-5 text-destructive" />
          )}
          <span className="font-medium">
            API Status: {healthLoading ? 'Checking...' : isHealthy ? 'Healthy' : 'Unavailable'}
          </span>
        </div>
      </div>

      {/* Features Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <ChartCard title="Interactive Dashboard" className="text-center">
          <div className="py-4">
            <div className="w-12 h-12 mx-auto rounded-xl bg-primary/10 flex items-center justify-center mb-3">
              <Database className="w-6 h-6 text-primary" />
            </div>
            <p className="text-sm text-muted-foreground">
              Real-time analytics with interactive charts showing trends, patterns, and insights.
            </p>
          </div>
        </ChartCard>

        <ChartCard title="Map Visualization" className="text-center">
          <div className="py-4">
            <div className="w-12 h-12 mx-auto rounded-xl bg-accent/10 flex items-center justify-center mb-3">
              <Server className="w-6 h-6 text-accent" />
            </div>
            <p className="text-sm text-muted-foreground">
              Explore sightings geographically with marker clusters and heatmap views.
            </p>
          </div>
        </ChartCard>

        <ChartCard title="RESTful API" className="text-center">
          <div className="py-4">
            <div className="w-12 h-12 mx-auto rounded-xl bg-primary/10 flex items-center justify-center mb-3">
              <FileJson className="w-6 h-6 text-primary" />
            </div>
            <p className="text-sm text-muted-foreground">
              Full API access with Swagger documentation for custom integrations.
            </p>
          </div>
        </ChartCard>
      </div>

      {/* Database Stats */}
      <ChartCard title="Database Overview" subtitle="Current data statistics">
        {rowCountsLoading ? (
          <div className="py-8 flex justify-center">
            <LoadingSpinner />
          </div>
        ) : (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {Object.entries(rowCounts).map(([table, count]: [string, any]) => (
              <div key={table} className="p-4 bg-secondary/50 rounded-lg text-center">
                <p className="text-2xl font-bold font-display text-primary">
                  {typeof count === 'number' ? count.toLocaleString() : count}
                </p>
                <p className="text-sm text-muted-foreground capitalize mt-1">
                  {table.replace(/_/g, ' ')}
                </p>
              </div>
            ))}
          </div>
        )}
      </ChartCard>

      {/* API Info */}
      <ChartCard title="API Information" subtitle="Technical details">
        {infoLoading ? (
          <div className="py-8 flex justify-center">
            <LoadingSpinner />
          </div>
        ) : info ? (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="p-3 bg-secondary/50 rounded-lg">
                <p className="text-xs text-muted-foreground">Version</p>
                <p className="font-medium">{info.version || 'N/A'}</p>
              </div>
              <div className="p-3 bg-secondary/50 rounded-lg">
                <p className="text-xs text-muted-foreground">Environment</p>
                <p className="font-medium">{info.environment || 'Production'}</p>
              </div>
            </div>
            {info.description && (
              <p className="text-sm text-muted-foreground">{info.description}</p>
            )}
          </div>
        ) : (
          <p className="text-muted-foreground">Unable to load API information</p>
        )}
      </ChartCard>

      {/* Links */}
      <div className="flex flex-wrap justify-center gap-4">
        <Button asChild variant="default" size="lg" className="gap-2">
          <a
            href="https://atay-ufo-analytics-api.onrender.com/docs"
            target="_blank"
            rel="noopener noreferrer"
          >
            <FileJson className="w-4 h-4" />
            API Documentation
            <ExternalLink className="w-4 h-4" />
          </a>
        </Button>

        <Button asChild variant="outline" size="lg" className="gap-2">
          <a href="mailto:contact@atay-analytics.com">
            <Mail className="w-4 h-4" />
            Contact
          </a>
        </Button>

        <Button asChild variant="outline" size="lg" className="gap-2">
          <a href="https://github.com" target="_blank" rel="noopener noreferrer">
            <Github className="w-4 h-4" />
            GitHub
            <ExternalLink className="w-4 h-4" />
          </a>
        </Button>
      </div>

      {/* Footer */}
      <div className="text-center pt-8 border-t border-border/50">
        <p className="text-sm text-muted-foreground">
          Built with modern data engineering practices. Data sourced from public UFO sighting
          reports.
        </p>
        <p className="text-xs text-muted-foreground mt-2">
          © 2024 ATAY UFO Analytics. All rights reserved.
        </p>
      </div>
    </div>
  );
}
