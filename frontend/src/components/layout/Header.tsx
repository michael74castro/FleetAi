import { useMsal } from '@azure/msal-react';
import { User, LogOut, Settings, ChevronDown } from 'lucide-react';
import { NavLink, Link, useLocation, useNavigate } from 'react-router-dom';
import { useAuthStore } from '@/store/authStore';
import { getInitials, cn } from '@/lib/utils';
import { useState } from 'react';
import { api } from '@/services/api';

interface NavItem {
  name: string;
  href: string;
  children?: { name: string; href: string }[];
}

const navigation: NavItem[] = [
  { name: 'Home', href: '/' },
  {
    name: 'Operation',
    href: '/operation',
    children: [
      { name: 'Fleet', href: '/operation' },
      { name: 'Renewals & orders', href: '/operation/renewals' },
      { name: 'Service & MOT', href: '/operation/service' },
    ]
  },
  {
    name: 'Analysis',
    href: '/analysis',
    children: [
      { name: 'Fleet', href: '/analysis/fleet' },
      { name: 'Renewals & Orders', href: '/analysis/renewals' },
      { name: 'Fines', href: '/analysis/fines' },
    ]
  },
  { name: 'Report', href: '/reports' },
  { name: 'AI Assistant', href: '/assistant' },
];

export default function Header() {
  const { instance } = useMsal();
  const { user, logout: clearAuth, hasRole } = useAuthStore();
  const [showUserDropdown, setShowUserDropdown] = useState(false);
  const [openDropdown, setOpenDropdown] = useState<string | null>(null);
  const location = useLocation();
  const navigate = useNavigate();

  const toggleDropdown = (name: string) => {
    setOpenDropdown(openDropdown === name ? null : name);
  };

  const closeDropdown = () => {
    setOpenDropdown(null);
  };

  const handleLogout = async () => {
    try {
      await api.logout();
    } catch (e) {
      // Ignore logout API errors
    }
    clearAuth();
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    navigate('/login');
  };

  const isActive = (href: string) => {
    if (href === '/') {
      return location.pathname === '/';
    }
    if (href === '/reports') {
      return location.pathname === '/reports' || location.pathname.startsWith('/reports/');
    }
    if (href === '/assistant') {
      return location.pathname === '/assistant' || location.pathname.startsWith('/assistant/');
    }
    return location.pathname.startsWith(href);
  };

  const isChildActive = (children: { name: string; href: string }[]) => {
    return children.some(child => {
      // Exact match for root paths like /operation or /analysis
      if (child.href === '/operation' || child.href === '/analysis') {
        return location.pathname === child.href;
      }
      return location.pathname.startsWith(child.href);
    });
  };

  return (
    <header className="flex h-16 items-center glass border-b border-white/10 px-6" style={{ position: 'relative', zIndex: 100 }}>
      {/* Logo Section */}
      <NavLink to="/" className="flex items-center group mr-8">
        <img
          src="/logo.png"
          alt="myLeaseAI"
          className="h-16 w-auto object-contain"
          style={{ mixBlendMode: 'lighten' }}
        />
      </NavLink>

      {/* Navigation Tabs */}
      <nav className="flex items-center space-x-1">
        {navigation.map((item) => {
          const active = item.children ? isChildActive(item.children) : isActive(item.href);

          // Render dropdown for items with children
          if (item.children) {
            const isOpen = openDropdown === item.name;
            return (
              <div key={item.name} className="relative">
                <button
                  onClick={() => toggleDropdown(item.name)}
                  className={cn(
                    'relative px-4 py-2 text-sm font-medium transition-all duration-200 rounded-lg group flex items-center space-x-1',
                    active
                      ? 'text-white'
                      : 'text-white/60 hover:text-white hover:bg-white/5'
                  )}
                >
                  <span>{item.name}</span>
                  <ChevronDown className={cn(
                    'h-3 w-3 transition-transform duration-200',
                    isOpen && 'rotate-180'
                  )} />
                  {/* Active indicator - orange underline */}
                  {active && (
                    <span className="absolute bottom-0 left-2 right-2 h-0.5 bg-brand-orange rounded-full" />
                  )}
                </button>

                {/* Dropdown Menu */}
                {isOpen && (
                  <>
                    <div
                      className="fixed inset-0"
                      style={{ zIndex: 98 }}
                      onClick={closeDropdown}
                    />
                    <div
                      className="absolute left-0 top-full mt-1 w-48 glass-panel p-2 animate-scale-in origin-top-left"
                      style={{ zIndex: 99 }}
                    >
                      {item.children.map((child) => (
                        <Link
                          key={child.name}
                          to={child.href}
                          onClick={closeDropdown}
                          className={cn(
                            'block w-full px-3 py-2.5 rounded-lg text-sm transition-colors',
                            location.pathname === child.href ||
                            (child.href !== '/' && location.pathname.startsWith(child.href))
                              ? 'text-brand-orange bg-brand-orange/10'
                              : 'text-white/70 hover:text-white hover:bg-white/10'
                          )}
                        >
                          {child.name}
                        </Link>
                      ))}
                    </div>
                  </>
                )}
              </div>
            );
          }

          // Render AI Assistant with image
          if (item.name === 'AI Assistant') {
            return (
              <NavLink
                key={item.name}
                to={item.href}
                className={cn(
                  'relative px-2 py-1 transition-all duration-200 rounded-lg group flex items-center',
                  active
                    ? 'bg-white/10'
                    : 'hover:bg-white/5'
                )}
              >
                <img
                  src="/leabot.png"
                  alt="AI Assistant"
                  className="h-14 w-auto object-contain"
                  style={{ mixBlendMode: 'lighten' }}
                />
                {/* Active indicator - orange underline */}
                {active && (
                  <span className="absolute bottom-0 left-2 right-2 h-0.5 bg-brand-orange rounded-full" />
                )}
              </NavLink>
            );
          }

          // Render regular nav link
          return (
            <NavLink
              key={item.name}
              to={item.href}
              className={cn(
                'relative px-4 py-2 text-sm font-medium transition-all duration-200 rounded-lg group',
                active
                  ? 'text-white'
                  : 'text-white/60 hover:text-white hover:bg-white/5'
              )}
            >
              {item.name}
              {/* Active indicator - orange underline */}
              {active && (
                <span className="absolute bottom-0 left-2 right-2 h-0.5 bg-brand-orange rounded-full" />
              )}
              {/* Hover underline effect */}
              {!active && (
                <span className="absolute bottom-0 left-2 right-2 h-0.5 bg-white/20 rounded-full scale-x-0 group-hover:scale-x-100 transition-transform origin-center" />
              )}
            </NavLink>
          );
        })}
      </nav>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Divider */}
      <div className="h-8 w-px bg-white/10 mr-4" />

      {/* User Section */}
      <div className="relative">
        <button
          onClick={() => setShowUserDropdown(!showUserDropdown)}
          className="flex items-center space-x-3 glass-button px-3 py-2 group"
        >
          <div className="relative">
            <div className="absolute inset-0 bg-brand-orange rounded-full blur opacity-50" />
            <div className="relative flex h-8 w-8 items-center justify-center rounded-full bg-gradient-to-br from-brand-orange to-brand-amber text-sm font-semibold text-white">
              {user ? getInitials(user.display_name) : <User className="h-4 w-4" />}
            </div>
          </div>
          <div className="hidden md:block text-left">
            <p className="text-sm font-medium text-white">{user?.display_name || 'User'}</p>
            <p className="text-xs text-white/50">{user?.role_name || 'Loading...'}</p>
          </div>
          <ChevronDown className={cn(
            'h-4 w-4 text-white/50 transition-transform duration-200',
            showUserDropdown && 'rotate-180'
          )} />
        </button>

        {/* Dropdown Menu */}
        {showUserDropdown && (
          <>
            <div
              className="fixed inset-0 z-40"
              onClick={() => setShowUserDropdown(false)}
            />
            <div className="absolute right-0 mt-2 w-64 glass-panel p-2 z-50 animate-scale-in origin-top-right">
              <div className="px-3 py-3 border-b border-white/10">
                <p className="text-sm font-semibold text-white">{user?.display_name}</p>
                <p className="text-xs text-white/50 mt-0.5">{user?.email}</p>
              </div>

              <div className="py-2">
                <button className="flex w-full items-center px-3 py-2.5 rounded-lg text-sm text-white/70 hover:text-white hover:bg-white/10 transition-colors">
                  <Settings className="mr-3 h-4 w-4" />
                  Settings
                </button>

                {/* Admin link */}
                {hasRole('admin') && (
                  <NavLink
                    to="/admin"
                    onClick={() => setShowUserDropdown(false)}
                    className="flex w-full items-center px-3 py-2.5 rounded-lg text-sm text-white/70 hover:text-white hover:bg-white/10 transition-colors"
                  >
                    <Settings className="mr-3 h-4 w-4" />
                    Admin Panel
                  </NavLink>
                )}

                <button
                  onClick={handleLogout}
                  className="flex w-full items-center px-3 py-2.5 rounded-lg text-sm text-red-400 hover:text-red-300 hover:bg-red-500/10 transition-colors"
                >
                  <LogOut className="mr-3 h-4 w-4" />
                  Sign out
                </button>
              </div>
            </div>
          </>
        )}
      </div>
    </header>
  );
}
