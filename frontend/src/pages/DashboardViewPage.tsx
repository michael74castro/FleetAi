import { useEffect } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { Edit, RefreshCw, Share2, Calendar, Filter, ArrowLeft, LayoutDashboard } from 'lucide-react';
import { useDashboardStore } from '@/store/dashboardStore';
import DashboardGrid from '@/components/dashboard/DashboardGrid';
import GlobalFilters from '@/components/dashboard/GlobalFilters';

export default function DashboardViewPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const {
    currentDashboard,
    isLoading,
    error,
    loadDashboard,
    refreshAllWidgets,
    globalFilters,
    dateRange,
    setDateRange,
  } = useDashboardStore();

  useEffect(() => {
    if (id) {
      loadDashboard(parseInt(id));
    }
  }, [id, loadDashboard]);

  useEffect(() => {
    if (currentDashboard) {
      refreshAllWidgets();
    }
  }, [globalFilters, dateRange]);

  if (isLoading && !currentDashboard) {
    return (
      <div className="flex h-64 items-center justify-center">
        <div className="relative">
          <div className="absolute inset-0 bg-brand-orange rounded-full blur-xl opacity-50 animate-pulse-glow" />
          <div className="relative w-12 h-12 border-4 border-brand-orange/30 border-t-brand-orange rounded-full animate-spin" />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-64 glass-panel p-12 animate-fade-in">
        <p className="text-red-400 mb-4">{error}</p>
        <Link
          to="/dashboards"
          className="flex items-center space-x-2 text-brand-cyan hover:text-brand-cyan-light transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          <span>Back to dashboards</span>
        </Link>
      </div>
    );
  }

  if (!currentDashboard) {
    return (
      <div className="flex flex-col items-center justify-center h-64 glass-panel p-12 animate-fade-in">
        <p className="text-white/50 mb-4">Dashboard not found</p>
        <Link
          to="/dashboards"
          className="flex items-center space-x-2 text-brand-cyan hover:text-brand-cyan-light transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          <span>Back to dashboards</span>
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <div className="flex items-center space-x-3">
            <h1 className="text-3xl font-bold text-white tracking-tight">{currentDashboard.name}</h1>
            {currentDashboard.is_shared && (
              <span className="flex items-center space-x-1.5 rounded-full bg-brand-cyan/20 border border-brand-cyan/30 px-3 py-1 text-xs font-medium text-brand-cyan">
                <Share2 className="h-3 w-3" />
                <span>Shared</span>
              </span>
            )}
          </div>
          {currentDashboard.description && (
            <p className="text-white/60 mt-1">{currentDashboard.description}</p>
          )}
        </div>

        <div className="flex items-center space-x-3">
          <button
            onClick={() => refreshAllWidgets()}
            disabled={isLoading}
            className="glass-button flex items-center space-x-2 px-4 py-2.5 rounded-xl text-white/80 font-medium disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            <span>Refresh</span>
          </button>
          <button
            onClick={() => navigate(`/dashboards/${id}/edit`)}
            className="glass-button-primary flex items-center space-x-2 px-5 py-2.5 rounded-xl text-white font-semibold"
          >
            <Edit className="h-4 w-4" />
            <span>Edit</span>
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="glass-panel p-4 flex items-center justify-between">
        <GlobalFilters />

        {/* Date range picker */}
        <div className="flex items-center space-x-3">
          <Calendar className="h-4 w-4 text-white/50" />
          <select
            value={dateRange ? 'custom' : 'all'}
            onChange={(e) => {
              const value = e.target.value;
              if (value === 'all') {
                setDateRange(null);
              } else if (value === 'last7') {
                const end = new Date();
                const start = new Date();
                start.setDate(start.getDate() - 7);
                setDateRange({
                  start: start.toISOString().split('T')[0],
                  end: end.toISOString().split('T')[0],
                });
              } else if (value === 'last30') {
                const end = new Date();
                const start = new Date();
                start.setDate(start.getDate() - 30);
                setDateRange({
                  start: start.toISOString().split('T')[0],
                  end: end.toISOString().split('T')[0],
                });
              } else if (value === 'last90') {
                const end = new Date();
                const start = new Date();
                start.setDate(start.getDate() - 90);
                setDateRange({
                  start: start.toISOString().split('T')[0],
                  end: end.toISOString().split('T')[0],
                });
              }
            }}
            className="glass-input px-4 py-2 text-sm text-white rounded-xl appearance-none cursor-pointer"
          >
            <option value="all">All Time</option>
            <option value="last7">Last 7 Days</option>
            <option value="last30">Last 30 Days</option>
            <option value="last90">Last 90 Days</option>
          </select>
        </div>
      </div>

      {/* Dashboard Grid */}
      {currentDashboard.widgets.length === 0 ? (
        <div className="glass-panel flex flex-col items-center justify-center p-16">
          <div className="relative mb-6">
            <div className="absolute inset-0 bg-brand-orange rounded-full blur-2xl opacity-30 animate-pulse-glow" />
            <div className="relative flex h-20 w-20 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-orange/20 to-brand-orange/10 border border-brand-orange/30">
              <LayoutDashboard className="h-10 w-10 text-brand-orange" />
            </div>
          </div>
          <h3 className="text-xl font-semibold text-white mb-2">No widgets yet</h3>
          <p className="text-white/50 text-center max-w-sm mb-6">
            Add widgets to this dashboard to visualize your fleet data with charts, KPIs, and more
          </p>
          <button
            onClick={() => navigate(`/dashboards/${id}/edit`)}
            className="glass-button-primary flex items-center space-x-2 px-6 py-3 rounded-xl text-white font-semibold group"
          >
            <Edit className="h-5 w-5" />
            <span>Edit Dashboard</span>
          </button>
        </div>
      ) : (
        <DashboardGrid
          widgets={currentDashboard.widgets}
          layoutConfig={currentDashboard.layout_config}
          isEditing={false}
        />
      )}
    </div>
  );
}
