import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Plus, FileText, Calendar, User, MoreVertical, Trash2, Clock, X, FileBarChart } from 'lucide-react';
import { useReportStore } from '@/store/reportStore';
import { formatDateTime } from '@/lib/utils';

export default function ReportsPage() {
  const navigate = useNavigate();
  const { reports, isLoading, error, loadReports, deleteReport } = useReportStore();
  const [menuOpen, setMenuOpen] = useState<number | null>(null);

  useEffect(() => {
    loadReports();
  }, [loadReports]);

  const handleDelete = async (id: number) => {
    if (confirm('Are you sure you want to delete this report?')) {
      await deleteReport(id);
      setMenuOpen(null);
    }
  };

  if (isLoading && reports.length === 0) {
    return (
      <div className="flex h-64 items-center justify-center">
        <div className="relative">
          <div className="absolute inset-0 bg-brand-cyan rounded-full blur-xl opacity-50 animate-pulse-glow" />
          <div className="relative w-12 h-12 border-4 border-brand-cyan/30 border-t-brand-cyan rounded-full animate-spin" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white tracking-tight">Reports</h1>
          <p className="text-white/60 mt-1">Create and manage your fleet reports</p>
        </div>
        <button
          onClick={() => navigate('/reports/new')}
          className="glass-button-primary flex items-center space-x-2 px-5 py-3 rounded-xl text-white font-semibold group"
        >
          <Plus className="h-5 w-5 group-hover:rotate-90 transition-transform duration-300" />
          <span>New Report</span>
        </button>
      </div>

      {/* Error */}
      {error && (
        <div className="glass-card border-red-500/30 bg-red-500/10 p-4 rounded-xl">
          <p className="text-sm text-red-300">{error}</p>
        </div>
      )}

      {/* Reports grid */}
      {reports.length === 0 ? (
        <div className="glass-panel flex flex-col items-center justify-center p-16">
          <div className="relative mb-6">
            <div className="absolute inset-0 bg-brand-cyan rounded-full blur-2xl opacity-30 animate-pulse-glow" />
            <div className="relative flex h-20 w-20 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-cyan/20 to-brand-cyan/10 border border-brand-cyan/30">
              <FileBarChart className="h-10 w-10 text-brand-cyan" />
            </div>
          </div>
          <h3 className="text-xl font-semibold text-white mb-2">No reports yet</h3>
          <p className="text-white/50 text-center max-w-sm mb-6">
            Create your first report to start analyzing fleet data and generate actionable insights
          </p>
          <button
            onClick={() => navigate('/reports/new')}
            className="glass-button-primary flex items-center space-x-2 px-6 py-3 rounded-xl text-white font-semibold group"
          >
            <Plus className="h-5 w-5 group-hover:rotate-90 transition-transform duration-300" />
            <span>Create Your First Report</span>
          </button>
        </div>
      ) : (
        <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3 animate-stagger">
          {reports.map((report, index) => (
            <div
              key={report.report_id}
              className="group relative glass-card p-5 hover:scale-[1.02] transition-all duration-300"
              style={{ animationDelay: `${index * 50}ms` }}
            >
              <Link to={`/reports/${report.report_id}`} className="block">
                <div className="flex items-start justify-between">
                  <div className="flex items-center space-x-4">
                    <div className="relative">
                      <div className="absolute inset-0 bg-brand-cyan rounded-xl blur opacity-0 group-hover:opacity-50 transition-opacity" />
                      <div className="relative flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br from-brand-cyan/20 to-brand-cyan/10 border border-brand-cyan/30 group-hover:border-brand-cyan/50 transition-colors">
                        <FileText className="h-6 w-6 text-brand-cyan" />
                      </div>
                    </div>
                    <div className="flex-1 min-w-0">
                      <h3 className="font-semibold text-white truncate group-hover:text-brand-cyan transition-colors">
                        {report.name}
                      </h3>
                      <p className="text-sm text-white/50 line-clamp-1 mt-0.5">
                        {report.description || report.dataset_name}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="mt-5 pt-4 border-t border-white/10 flex items-center justify-between text-xs text-white/40">
                  <span className="flex items-center">
                    <Calendar className="mr-1.5 h-3.5 w-3.5" />
                    {formatDateTime(report.updated_at)}
                  </span>
                  <span className="flex items-center">
                    <User className="mr-1.5 h-3.5 w-3.5" />
                    {report.is_shared ? 'Shared' : 'Private'}
                  </span>
                </div>

                {report.schedules && report.schedules.length > 0 && (
                  <div className="mt-3 flex items-center text-xs">
                    <span className="flex items-center rounded-full bg-brand-lime/20 border border-brand-lime/30 px-2.5 py-1 text-brand-lime">
                      <Clock className="mr-1.5 h-3 w-3" />
                      Scheduled
                    </span>
                  </div>
                )}
              </Link>

              {/* Menu */}
              <div className="absolute top-3 right-3">
                <button
                  onClick={(e) => {
                    e.preventDefault();
                    setMenuOpen(menuOpen === report.report_id ? null : report.report_id);
                  }}
                  className="glass-button p-2 opacity-0 group-hover:opacity-100 transition-opacity"
                >
                  <MoreVertical className="h-4 w-4 text-white/70" />
                </button>

                {menuOpen === report.report_id && (
                  <>
                    <div
                      className="fixed inset-0 z-40"
                      onClick={() => setMenuOpen(null)}
                    />
                    <div className="absolute right-0 mt-1 w-44 glass-panel p-1.5 z-50 animate-scale-in origin-top-right">
                      <button
                        onClick={() => navigate(`/reports/${report.report_id}/edit`)}
                        className="flex w-full items-center px-3 py-2.5 rounded-lg text-sm text-white/80 hover:text-white hover:bg-white/10 transition-colors"
                      >
                        Edit
                      </button>
                      <div className="my-1 border-t border-white/10" />
                      <button
                        onClick={() => handleDelete(report.report_id)}
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
    </div>
  );
}
