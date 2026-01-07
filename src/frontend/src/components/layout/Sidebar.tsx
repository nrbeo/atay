import { NavLink } from '@/components/NavLink';
import { cn } from '@/lib/utils';
import {
  LayoutDashboard,
  Map,
  Eye,
  Shapes,
  Info,
  ChevronLeft,
  ChevronRight,
  Radar,
  CloudSun,
} from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/components/ui/button';

const navItems = [
  { to: '/', icon: Map, label: 'Map Explorer' },
  { to: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/climate', icon: CloudSun, label: 'Climate Analysis' },
  { to: '/observations', icon: Eye, label: 'Observations' },
  { to: '/dimensions', icon: Shapes, label: 'Dimensions' },
  { to: '/about', icon: Info, label: 'About' },
];

export function Sidebar() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={cn(
        'fixed left-0 top-0 h-screen glass-card border-r border-border/50 transition-all duration-300 z-50 flex flex-col',
        collapsed ? 'w-16' : 'w-64'
      )}
    >
      {/* Logo */}
      <div className="p-4 border-b border-border/50">
        <div className="flex items-center gap-3">
          <div className="relative">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-primary to-accent flex items-center justify-center neon-glow">
              <Radar className="w-5 h-5 text-primary-foreground" />
            </div>
            <div className="absolute -top-1 -right-1 w-3 h-3 bg-accent rounded-full animate-pulse" />
          </div>
          {!collapsed && (
            <div className="animate-fade-in">
              <h1 className="font-display text-lg font-bold tracking-wider text-gradient">
                ATAY UFO
              </h1>
              <p className="text-xs text-muted-foreground">Analytics</p>
            </div>
          )}
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-3 space-y-1">
        {navItems.map((item, index) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === '/'}
            className={cn(
              'flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200 group',
              'text-muted-foreground hover:text-foreground hover:bg-secondary/80'
            )}
            activeClassName="bg-primary/10 text-primary border border-primary/20 neon-glow-purple"
            style={{ animationDelay: `${index * 50}ms` }}
          >
            <item.icon className="w-5 h-5 shrink-0 transition-transform group-hover:scale-110" />
            {!collapsed && (
              <span className="font-medium text-sm">{item.label}</span>
            )}
          </NavLink>
        ))}
      </nav>

      {/* Collapse button */}
      <div className="p-3 border-t border-border/50">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setCollapsed(!collapsed)}
          className="w-full justify-center hover:bg-secondary"
        >
          {collapsed ? (
            <ChevronRight className="w-4 h-4" />
          ) : (
            <>
              <ChevronLeft className="w-4 h-4 mr-2" />
              <span className="text-sm">Collapse</span>
            </>
          )}
        </Button>
      </div>

      {/* Decorative elements */}
      <div className="absolute bottom-20 left-0 right-0 h-px bg-gradient-to-r from-transparent via-accent/30 to-transparent" />
    </aside>
  );
}
