import { Link, useLocation } from 'react-router-dom';
import {
  LayoutDashboard,
  FileText,
  MessageSquare,
  Settings,
  ChevronLeft,
  ChevronRight,
  Truck,
} from 'lucide-react';
import { useState } from 'react';
import { cn } from '@/lib/utils';
import { useAuthStore } from '@/store/authStore';

const navigation = [
  { name: 'Dashboards', href: '/dashboards', icon: LayoutDashboard },
  { name: 'Reports', href: '/reports', icon: FileText },
  { name: 'AI Assistant', href: '/assistant', icon: MessageSquare },
];

const adminNavigation = [
  { name: 'Admin', href: '/admin', icon: Settings },
];

export default function Sidebar() {
  const [collapsed, setCollapsed] = useState(false);
  const location = useLocation();
  const { hasRole } = useAuthStore();

  const isActive = (href: string) => {
    if (href === '/dashboards') {
      return location.pathname === '/dashboards' || location.pathname.startsWith('/dashboards/');
    }
    if (href === '/reports') {
      return location.pathname === '/reports' || location.pathname.startsWith('/reports/');
    }
    return location.pathname.startsWith(href);
  };

  return (
    <div
      className={cn(
        'flex flex-col glass border-r border-white/10 transition-all duration-300 ease-out',
        collapsed ? 'w-20' : 'w-72'
      )}
    >
      {/* Logo */}
      <div className="flex h-20 items-center justify-between px-4 border-b border-white/10">
        {!collapsed && (
          <Link to="/" className="flex items-center space-x-3 group">
            <div className="relative">
              <div className="absolute inset-0 bg-brand-orange rounded-xl blur-lg opacity-50 group-hover:opacity-70 transition-opacity" />
              <div className="relative flex h-11 w-11 items-center justify-center rounded-xl bg-gradient-to-br from-brand-orange to-brand-amber">
                <Truck className="h-6 w-6 text-white" />
              </div>
            </div>
            <div className="flex flex-col">
              <span className="text-xl font-bold text-white tracking-tight">MyFleet</span>
              <span className="text-[10px] font-medium text-white/50 uppercase tracking-widest">Intelligence</span>
            </div>
          </Link>
        )}
        {collapsed && (
          <Link to="/" className="mx-auto group">
            <div className="relative">
              <div className="absolute inset-0 bg-brand-orange rounded-xl blur-lg opacity-50 group-hover:opacity-70 transition-opacity" />
              <div className="relative flex h-11 w-11 items-center justify-center rounded-xl bg-gradient-to-br from-brand-orange to-brand-amber">
                <Truck className="h-6 w-6 text-white" />
              </div>
            </div>
          </Link>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-2 px-3 py-6">
        <div className="animate-stagger">
          {navigation.map((item) => {
            const Icon = item.icon;
            const active = isActive(item.href);
            return (
              <Link
                key={item.name}
                to={item.href}
                className={cn(
                  'relative flex items-center px-4 py-3 rounded-xl text-sm font-medium transition-all duration-200 group',
                  active
                    ? 'bg-gradient-to-r from-brand-orange to-brand-orange-light text-white shadow-glow-orange'
                    : 'text-white/70 hover:text-white hover:bg-white/10'
                )}
              >
                {active && (
                  <div className="absolute inset-0 rounded-xl bg-gradient-to-r from-brand-orange to-brand-orange-light opacity-20 blur-xl" />
                )}
                <Icon className={cn(
                  'h-5 w-5 transition-transform duration-200',
                  collapsed ? 'mx-auto' : 'mr-3',
                  !active && 'group-hover:scale-110'
                )} />
                {!collapsed && (
                  <span className="relative">{item.name}</span>
                )}
                {!collapsed && active && (
                  <div className="ml-auto w-1.5 h-1.5 rounded-full bg-white animate-pulse-glow" />
                )}
              </Link>
            );
          })}
        </div>

        {/* Admin section */}
        {hasRole('admin') && (
          <>
            <div className="my-6 border-t border-white/10" />
            <div className="px-4 mb-3">
              {!collapsed && (
                <span className="text-[10px] font-semibold text-white/40 uppercase tracking-widest">
                  Administration
                </span>
              )}
            </div>
            {adminNavigation.map((item) => {
              const Icon = item.icon;
              const active = isActive(item.href);
              return (
                <Link
                  key={item.name}
                  to={item.href}
                  className={cn(
                    'relative flex items-center px-4 py-3 rounded-xl text-sm font-medium transition-all duration-200 group',
                    active
                      ? 'bg-gradient-to-r from-brand-cyan to-brand-cyan-light text-white shadow-glow-cyan'
                      : 'text-white/70 hover:text-white hover:bg-white/10'
                  )}
                >
                  <Icon className={cn(
                    'h-5 w-5 transition-transform duration-200',
                    collapsed ? 'mx-auto' : 'mr-3',
                    !active && 'group-hover:scale-110'
                  )} />
                  {!collapsed && <span>{item.name}</span>}
                </Link>
              );
            })}
          </>
        )}
      </nav>

      {/* Collapse button */}
      <div className="border-t border-white/10 p-3">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="flex w-full items-center justify-center px-4 py-3 rounded-xl text-sm font-medium text-white/60 hover:text-white hover:bg-white/10 transition-all duration-200 group"
        >
          {collapsed ? (
            <ChevronRight className="h-5 w-5 group-hover:translate-x-1 transition-transform" />
          ) : (
            <>
              <ChevronLeft className="h-5 w-5 mr-2 group-hover:-translate-x-1 transition-transform" />
              <span>Collapse</span>
            </>
          )}
        </button>
      </div>
    </div>
  );
}
