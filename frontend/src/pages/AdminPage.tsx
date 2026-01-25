import { useEffect, useState } from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import { Users, Database, Settings, BarChart3, Search, TrendingUp, FileText, MessageSquare } from 'lucide-react';
import { api } from '@/services/api';
import type { User } from '@/types';

function UserManagement() {
  const [users, setUsers] = useState<User[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [search, setSearch] = useState('');

  useEffect(() => {
    loadUsers();
  }, []);

  const loadUsers = async () => {
    setIsLoading(true);
    try {
      const response = await api.getUsers({ page_size: 100 });
      setUsers(response.items || []);
    } catch (error) {
      console.error('Failed to load users:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const filteredUsers = users.filter(
    (user) =>
      user.display_name.toLowerCase().includes(search.toLowerCase()) ||
      user.email.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-white">User Management</h2>
        <div className="relative">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-white/40" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search users..."
            className="glass-input pl-10 pr-4 py-2.5 w-64 text-sm text-white"
          />
        </div>
      </div>

      {isLoading ? (
        <div className="flex justify-center py-12">
          <div className="relative">
            <div className="absolute inset-0 bg-brand-orange rounded-full blur-xl opacity-50 animate-pulse-glow" />
            <div className="relative w-10 h-10 border-4 border-brand-orange/30 border-t-brand-orange rounded-full animate-spin" />
          </div>
        </div>
      ) : (
        <div className="glass-panel overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-white/5 border-b border-white/10">
              <tr>
                <th className="px-5 py-4 text-left font-semibold text-white/70 uppercase tracking-wide text-xs">Name</th>
                <th className="px-5 py-4 text-left font-semibold text-white/70 uppercase tracking-wide text-xs">Email</th>
                <th className="px-5 py-4 text-left font-semibold text-white/70 uppercase tracking-wide text-xs">Role</th>
                <th className="px-5 py-4 text-left font-semibold text-white/70 uppercase tracking-wide text-xs">Status</th>
                <th className="px-5 py-4 text-left font-semibold text-white/70 uppercase tracking-wide text-xs">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {filteredUsers.map((user) => (
                <tr key={user.user_id} className="hover:bg-white/5 transition-colors">
                  <td className="px-5 py-4 text-white">{user.display_name}</td>
                  <td className="px-5 py-4 text-white/70">{user.email}</td>
                  <td className="px-5 py-4">
                    <span className="rounded-full bg-brand-orange/20 border border-brand-orange/30 px-3 py-1 text-xs font-medium text-brand-orange">
                      {user.role_name}
                    </span>
                  </td>
                  <td className="px-5 py-4">
                    <span
                      className={`rounded-full px-3 py-1 text-xs font-medium ${
                        user.is_active
                          ? 'bg-brand-green/20 border border-brand-green/30 text-brand-green'
                          : 'bg-red-500/20 border border-red-500/30 text-red-400'
                      }`}
                    >
                      {user.is_active ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                  <td className="px-5 py-4">
                    <button className="text-brand-cyan hover:text-brand-cyan-light text-sm font-medium transition-colors">
                      Edit
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function DatasetManagement() {
  const [datasets, setDatasets] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    loadDatasets();
  }, []);

  const loadDatasets = async () => {
    setIsLoading(true);
    try {
      const response = await api.getDatasets({ page_size: 100 });
      setDatasets(response.items || []);
    } catch (error) {
      console.error('Failed to load datasets:', error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <h2 className="text-lg font-semibold text-white">Dataset Management</h2>

      {isLoading ? (
        <div className="flex justify-center py-12">
          <div className="relative">
            <div className="absolute inset-0 bg-brand-cyan rounded-full blur-xl opacity-50 animate-pulse-glow" />
            <div className="relative w-10 h-10 border-4 border-brand-cyan/30 border-t-brand-cyan rounded-full animate-spin" />
          </div>
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 animate-stagger">
          {datasets.map((dataset, index) => (
            <div
              key={dataset.name}
              className="glass-card p-5 group hover:scale-[1.02] transition-all duration-300"
              style={{ animationDelay: `${index * 50}ms` }}
            >
              <div className="flex items-center space-x-4">
                <div className="relative">
                  <div className="absolute inset-0 bg-brand-cyan rounded-xl blur opacity-0 group-hover:opacity-50 transition-opacity" />
                  <div className="relative flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br from-brand-cyan/20 to-brand-cyan/10 border border-brand-cyan/30">
                    <Database className="h-6 w-6 text-brand-cyan" />
                  </div>
                </div>
                <div className="flex-1 min-w-0">
                  <h3 className="font-semibold text-white truncate">{dataset.display_name || dataset.name}</h3>
                  <p className="text-xs text-white/50 truncate">{dataset.source_object}</p>
                </div>
              </div>
              {dataset.description && (
                <p className="mt-3 text-sm text-white/60 line-clamp-2">
                  {dataset.description}
                </p>
              )}
              <div className="mt-4 flex items-center space-x-2">
                <span
                  className={`rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide ${
                    dataset.is_active
                      ? 'bg-brand-green/20 border border-brand-green/30 text-brand-green'
                      : 'bg-red-500/20 border border-red-500/30 text-red-400'
                  }`}
                >
                  {dataset.is_active ? 'Active' : 'Inactive'}
                </span>
                {dataset.category && (
                  <span className="text-xs text-white/40">{dataset.category}</span>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function SystemStats() {
  const [stats, setStats] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    loadStats();
  }, []);

  const loadStats = async () => {
    setIsLoading(true);
    try {
      const response = await api.getSystemStats();
      setStats(response);
    } catch (error) {
      console.error('Failed to load stats:', error);
    } finally {
      setIsLoading(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex justify-center py-12">
        <div className="relative">
          <div className="absolute inset-0 bg-brand-lime rounded-full blur-xl opacity-50 animate-pulse-glow" />
          <div className="relative w-10 h-10 border-4 border-brand-lime/30 border-t-brand-lime rounded-full animate-spin" />
        </div>
      </div>
    );
  }

  const statCards = [
    { label: 'Total Users', value: stats?.users || 0, icon: Users, color: 'orange' },
    { label: 'Active Dashboards', value: stats?.dashboards || 0, icon: TrendingUp, color: 'cyan' },
    { label: 'Reports Created', value: stats?.reports || 0, icon: FileText, color: 'lime' },
    { label: 'AI Conversations', value: stats?.conversations || 0, icon: MessageSquare, color: 'amber' },
  ];

  const colorClasses: Record<string, string> = {
    orange: 'from-brand-orange/20 to-brand-orange/10 border-brand-orange/30 text-brand-orange',
    cyan: 'from-brand-cyan/20 to-brand-cyan/10 border-brand-cyan/30 text-brand-cyan',
    lime: 'from-brand-lime/20 to-brand-lime/10 border-brand-lime/30 text-brand-lime',
    amber: 'from-brand-amber/20 to-brand-amber/10 border-brand-amber/30 text-brand-amber',
  };

  return (
    <div className="space-y-6">
      <h2 className="text-lg font-semibold text-white">System Statistics</h2>

      <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-4 animate-stagger">
        {statCards.map((stat, index) => (
          <div
            key={stat.label}
            className="glass-card p-5 group"
            style={{ animationDelay: `${index * 100}ms` }}
          >
            <div className="flex items-center justify-between">
              <div className={`flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br border ${colorClasses[stat.color]}`}>
                <stat.icon className="h-6 w-6" />
              </div>
            </div>
            <p className="mt-4 text-sm text-white/50">{stat.label}</p>
            <p className="mt-1 text-3xl font-bold text-white">{stat.value.toLocaleString()}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function AdminPage() {
  const location = useLocation();

  const tabs = [
    { name: 'Users', href: '/admin', icon: Users },
    { name: 'Datasets', href: '/admin/datasets', icon: Database },
    { name: 'Statistics', href: '/admin/stats', icon: BarChart3 },
    { name: 'Settings', href: '/admin/settings', icon: Settings },
  ];

  return (
    <div className="space-y-8 animate-fade-in">
      <div>
        <h1 className="text-3xl font-bold text-white tracking-tight">Admin</h1>
        <p className="text-white/60 mt-1">Manage users, datasets, and system settings</p>
      </div>

      {/* Tabs */}
      <div className="glass-panel p-1.5 inline-flex space-x-1">
        {tabs.map((tab) => {
          const Icon = tab.icon;
          const isActive =
            tab.href === '/admin'
              ? location.pathname === '/admin'
              : location.pathname.startsWith(tab.href);

          return (
            <Link
              key={tab.name}
              to={tab.href}
              className={`flex items-center space-x-2 px-5 py-2.5 rounded-xl text-sm font-medium transition-all duration-200 ${
                isActive
                  ? 'bg-gradient-to-r from-brand-orange to-brand-orange-light text-white shadow-glow-orange'
                  : 'text-white/60 hover:text-white hover:bg-white/10'
              }`}
            >
              <Icon className="h-4 w-4" />
              <span>{tab.name}</span>
            </Link>
          );
        })}
      </div>

      {/* Content */}
      <Routes>
        <Route index element={<UserManagement />} />
        <Route path="datasets" element={<DatasetManagement />} />
        <Route path="stats" element={<SystemStats />} />
        <Route
          path="settings"
          element={
            <div className="glass-panel p-12 text-center">
              <div className="relative inline-flex mb-4">
                <div className="absolute inset-0 bg-brand-cyan rounded-full blur-xl opacity-30" />
                <div className="relative flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-cyan/20 to-brand-cyan/10 border border-brand-cyan/30">
                  <Settings className="h-8 w-8 text-brand-cyan" />
                </div>
              </div>
              <p className="text-white/50">Settings coming soon...</p>
            </div>
          }
        />
      </Routes>
    </div>
  );
}
