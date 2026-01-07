import { cn } from '@/lib/utils';
import { LucideIcon } from 'lucide-react';
import { useEffect, useState } from 'react';

interface StatCardProps {
  title: string;
  value: number | string;
  icon: LucideIcon;
  trend?: {
    value: number;
    isPositive: boolean;
  };
  delay?: number;
  accentColor?: 'primary' | 'accent';
}

export function StatCard({
  title,
  value,
  icon: Icon,
  trend,
  delay = 0,
  accentColor = 'primary',
}: StatCardProps) {
  const [displayValue, setDisplayValue] = useState(0);
  const [isVisible, setIsVisible] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => setIsVisible(true), delay);
    return () => clearTimeout(timer);
  }, [delay]);

  useEffect(() => {
    if (!isVisible || typeof value !== 'number') return;

    const duration = 1500;
    const steps = 60;
    const stepValue = value / steps;
    let current = 0;

    const interval = setInterval(() => {
      current += stepValue;
      if (current >= value) {
        setDisplayValue(value);
        clearInterval(interval);
      } else {
        setDisplayValue(Math.floor(current));
      }
    }, duration / steps);

    return () => clearInterval(interval);
  }, [value, isVisible]);

  return (
    <div
      className={cn(
        'relative group glass-card rounded-xl p-5 transition-all duration-300',
        'hover:scale-[1.02] hover:shadow-2xl',
        !isVisible && 'opacity-0 translate-y-4',
        isVisible && 'opacity-100 translate-y-0'
      )}
      style={{ transitionDelay: `${delay}ms` }}
    >
      {/* Gradient border on hover */}
      <div className="absolute inset-0 rounded-xl opacity-0 group-hover:opacity-100 transition-opacity duration-300 gradient-border" />

      <div className="relative z-10">
        <div className="flex items-start justify-between mb-3">
          <div
            className={cn(
              'p-2.5 rounded-lg',
              accentColor === 'primary'
                ? 'bg-primary/10 text-primary'
                : 'bg-accent/10 text-accent'
            )}
          >
            <Icon className="w-5 h-5" />
          </div>
          {trend && (
            <span
              className={cn(
                'text-xs font-medium px-2 py-1 rounded-full',
                trend.isPositive
                  ? 'bg-accent/10 text-accent'
                  : 'bg-destructive/10 text-destructive'
              )}
            >
              {trend.isPositive ? '+' : ''}
              {trend.value}%
            </span>
          )}
        </div>

        <p className="text-sm text-muted-foreground mb-1">{title}</p>
        <p className="text-3xl font-bold font-display tracking-wide">
          {typeof value === 'number'
            ? displayValue.toLocaleString()
            : value}
        </p>
      </div>

      {/* Subtle glow effect */}
      <div
        className={cn(
          'absolute -inset-px rounded-xl opacity-0 group-hover:opacity-100 blur-xl transition-opacity duration-500 -z-10',
          accentColor === 'primary' ? 'bg-primary/20' : 'bg-accent/20'
        )}
      />
    </div>
  );
}
