import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Plus, LayoutDashboard, Calendar, User, MoreVertical, Copy, Trash2, X, Sparkles } from 'lucide-react';
import { useDashboardStore } from '@/store/dashboardStore';
import { formatDateTime } from '@/lib/utils';

export default function DashboardsPage() {
  const navigate = useNavigate();
  const { dashboards, isLoading, error, loadDashboards, createDashboard, deleteDashboard, cloneDashboard } = useDashboardStore();
  const [showNewDialog, setShowNewDialog] = useState(false);
  const [newName, setNewName] = useState('');
  const [newDescription, setNewDescription] = useState('');
  const [menuOpen, setMenuOpen] = useState<number | null>(null);

  useEffect(() => {
    loadDashboards();
  }, [loadDashboards]);

  const handleCreate = async () => {
    if (!newName.trim()) return;
    const dashboard = await createDashboard(newName, newDescription);
    setShowNewDialog(false);
    setNewName('');
    setNewDescription('');
    navigate(`/dashboards/${dashboard.dashboard_id}/edit`);
  };

  const handleClone = async (id: number, name: string) => {
    const dashboard = await cloneDashboard(id, `${name} (Copy)`);
    setMenuOpen(null);
    navigate(`/dashboards/${dashboard.dashboard_id}`);
  };

  const handleDelete = async (id: number) => {
    if (confirm('Are you sure you want to delete this dashboard?')) {
      await deleteDashboard(id);
      setMenuOpen(null);
    }
  };

  if (isLoading && dashboards.length === 0) {
    return (
      <div className="flex h-64 items-center justify-center">
        <div className="relative">
          <div className="absolute inset-0 bg-brand-orange rounded-full blur-xl opacity-50 animate-pulse-glow" />
          <div className="relative w-12 h-12 border-4 border-brand-orange/30 border-t-brand-orange rounded-full animate-spin" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white tracking-tight">Dashboards</h1>
          <p className="text-white/60 mt-1">Create and manage your fleet dashboards</p>
        </div>
        <button
          onClick={() => setShowNewDialog(true)}
          className="glass-button-primary flex items-center space-x-2 px-5 py-3 rounded-xl text-white font-semibold group"
        >
          <Plus className="h-5 w-5 group-hover:rotate-90 transition-transform duration-300" />
          <span>New Dashboard</span>
        </button>
      </div>

      {/* Error */}
      {error && (
        <div className="glass-card border-red-500/30 bg-red-500/10 p-4 rounded-xl">
          <p className="text-sm text-red-300">{error}</p>
        </div>
      )}

      {/* Dashboard grid */}
      {dashboards.length === 0 ? (
        <div className="glass-panel flex flex-col items-center justify-center p-16">
          <div className="relative mb-6">
            <div className="absolute inset-0 bg-brand-cyan rounded-full blur-2xl opacity-30 animate-pulse-glow" />
            <div className="relative flex h-20 w-20 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-cyan/20 to-brand-cyan/10 border border-brand-cyan/30">
              <LayoutDashboard className="h-10 w-10 text-brand-cyan" />
            </div>
          </div>
          <h3 className="text-xl font-semibold text-white mb-2">No dashboards yet</h3>
          <p className="text-white/50 text-center max-w-sm mb-6">
            Create your first dashboard to start visualizing fleet data with powerful widgets and charts
          </p>
          <button
            onClick={() => setShowNewDialog(true)}
            className="glass-button-primary flex items-center space-x-2 px-6 py-3 rounded-xl text-white font-semibold group"
          >
            <Sparkles className="h-5 w-5 group-hover:rotate-12 transition-transform" />
            <span>Create Your First Dashboard</span>
          </button>
        </div>
      ) : (
        <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3 animate-stagger">
          {dashboards.map((dashboard, index) => (
            <div
              key={dashboard.dashboard_id}
              className="group relative glass-card p-5 hover:scale-[1.02] transition-all duration-300"
              style={{ animationDelay: `${index * 50}ms` }}
            >
              <Link to={`/dashboards/${dashboard.dashboard_id}`} className="block">
                <div className="flex items-start justify-between">
                  <div className="flex items-center space-x-4">
                    <div className="relative">
                      <div className="absolute inset-0 bg-brand-orange rounded-xl blur opacity-0 group-hover:opacity-50 transition-opacity" />
                      <div className="relative flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br from-brand-orange/20 to-brand-orange/10 border border-brand-orange/30 group-hover:border-brand-orange/50 transition-colors">
                        <LayoutDashboard className="h-6 w-6 text-brand-orange" />
                      </div>
                    </div>
                    <div className="flex-1 min-w-0">
                      <h3 className="font-semibold text-white truncate group-hover:text-brand-orange transition-colors">
                        {dashboard.name}
                      </h3>
                      <p className="text-sm text-white/50 line-clamp-1 mt-0.5">
                        {dashboard.description || 'No description'}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="mt-5 pt-4 border-t border-white/10 flex items-center justify-between text-xs text-white/40">
                  <span className="flex items-center">
                    <Calendar className="mr-1.5 h-3.5 w-3.5" />
                    {formatDateTime(dashboard.updated_at)}
                  </span>
                  <span className="flex items-center">
                    <User className="mr-1.5 h-3.5 w-3.5" />
                    {dashboard.is_shared ? 'Shared' : 'Private'}
                  </span>
                </div>

                {dashboard.is_default && (
                  <span className="absolute top-3 right-12 rounded-full bg-brand-cyan/20 border border-brand-cyan/30 px-2.5 py-1 text-[10px] font-semibold text-brand-cyan uppercase tracking-wide">
                    Default
                  </span>
                )}
              </Link>

              {/* Menu */}
              <div className="absolute top-3 right-3">
                <button
                  onClick={(e) => {
                    e.preventDefault();
                    setMenuOpen(menuOpen === dashboard.dashboard_id ? null : dashboard.dashboard_id);
                  }}
                  className="glass-button p-2 opacity-0 group-hover:opacity-100 transition-opacity"
                >
                  <MoreVertical className="h-4 w-4 text-white/70" />
                </button>

                {menuOpen === dashboard.dashboard_id && (
                  <>
                    <div
                      className="fixed inset-0 z-40"
                      onClick={() => setMenuOpen(null)}
                    />
                    <div className="absolute right-0 mt-1 w-44 glass-panel p-1.5 z-50 animate-scale-in origin-top-right">
                      <button
                        onClick={() => navigate(`/dashboards/${dashboard.dashboard_id}/edit`)}
                        className="flex w-full items-center px-3 py-2.5 rounded-lg text-sm text-white/80 hover:text-white hover:bg-white/10 transition-colors"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => handleClone(dashboard.dashboard_id, dashboard.name)}
                        className="flex w-full items-center px-3 py-2.5 rounded-lg text-sm text-white/80 hover:text-white hover:bg-white/10 transition-colors"
                      >
                        <Copy className="mr-2 h-4 w-4" />
                        Duplicate
                      </button>
                      <div className="my-1 border-t border-white/10" />
                      <button
                        onClick={() => handleDelete(dashboard.dashboard_id)}
                        className="flex w-full items-center px-3 py-2.5 rounded-lg text-sm text-red-400 hover:text-red-300 hover:bg-red-500/10 transition-colors"
                      >
                        <Trash2 className="mr-2 h-4 w-4" />
                        Delete
                      </button>
                    </div>
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* New Dashboard Dialog */}
      {showNewDialog && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
          <div
            className="absolute inset-0 bg-black/60 backdrop-blur-sm"
            onClick={() => setShowNewDialog(false)}
          />
          <div className="relative w-full max-w-md glass-panel p-6 animate-scale-in">
            <button
              onClick={() => setShowNewDialog(false)}
              className="absolute top-4 right-4 glass-button p-2"
            >
              <X className="h-4 w-4 text-white/70" />
            </button>

            <div className="mb-6">
              <h2 className="text-xl font-semibold text-white">Create New Dashboard</h2>
              <p className="text-sm text-white/50 mt-1">Add a new dashboard to visualize your fleet data</p>
            </div>

            <div className="space-y-4">
              <div>
                <label className="text-sm font-medium text-white/70 mb-2 block">Name</label>
                <input
                  type="text"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  placeholder="Dashboard name"
                  className="w-full glass-input px-4 py-3 text-white"
                  autoFocus
                />
              </div>
              <div>
                <label className="text-sm font-medium text-white/70 mb-2 block">Description (optional)</label>
                <textarea
                  value={newDescription}
                  onChange={(e) => setNewDescription(e.target.value)}
                  placeholder="Dashboard description"
                  rows={3}
                  className="w-full glass-input px-4 py-3 text-white resize-none"
                />
              </div>
            </div>

            <div className="mt-6 flex justify-end space-x-3">
              <button
                onClick={() => setShowNewDialog(false)}
                className="glass-button px-5 py-2.5 rounded-xl text-white/70 font-medium"
              >
                Cancel
              </button>
              <button
                onClick={handleCreate}
                disabled={!newName.trim()}
                className="glass-button-primary px-5 py-2.5 rounded-xl text-white font-medium disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Create Dashboard
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
