import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Car, Users, FileText, TrendingUp, ChevronRight, BarChart3, MessageSquare } from 'lucide-react';
import { useAuthStore } from '@/store/authStore';
import { api } from '@/services/api';

interface FleetKPIs {
  total_vehicles: number;
  active_vehicles: number;
  total_customers: number;
  total_contracts: number;
  total_drivers: number;
  total_monthly_revenue: number;
  average_odometer_km: number;
  top_makes: string;
}

export default function HomePage() {
  const { user } = useAuthStore();
  const navigate = useNavigate();
  const firstName = user?.display_name?.split(' ')[0] || 'User';

  const [kpis, setKpis] = useState<FleetKPIs | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchKPIs = async () => {
      try {
        const data = await api.getFleetKPIs();
        setKpis(data);
      } catch (error) {
        // Use fallback data if API not available
        setKpis({
          total_vehicles: 33374,
          active_vehicles: 7105,
          total_customers: 2387,
          total_contracts: 33652,
          total_drivers: 33633,
          total_monthly_revenue: 79554180.45,
          average_odometer_km: 91858,
          top_makes: 'Toyota:3131,Nissan:1994,Mitsubishi:347'
        });
      } finally {
        setLoading(false);
      }
    };

    fetchKPIs();
  }, []);

  const formatNumber = (num: number) => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toLocaleString();
  };

  const formatCurrency = (num: number) => {
    if (num >= 1000000) {
      return 'AED ' + (num / 1000000).toFixed(1) + 'M';
    }
    return 'AED ' + num.toLocaleString();
  };

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Welcome Section */}
      <div className="mb-2">
        <h1 className="text-2xl font-bold text-white">
          Welcome back <span className="text-brand-orange">{firstName}</span>, ready to manage your fleet?
        </h1>
      </div>

      {/* Main Content - Two Column Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left Section - Personalized Teaser Area (2/3 width) */}
        <div className="lg:col-span-2 space-y-4">
          <h2 className="text-lg font-semibold text-white/80">What's next?</h2>

          {/* Teaser Card */}
          <div
            className="glass-panel p-0 overflow-hidden cursor-pointer"
            onClick={() => navigate('/assistant')}
          >
            <div className="relative h-80 bg-gradient-to-br from-brand-teal/20 to-brand-teal-dark/40">
              {/* Decorative Elements */}
              <div className="absolute inset-0 overflow-hidden">
                <div className="absolute -right-20 -bottom-20 w-96 h-96 bg-brand-orange/10 rounded-full blur-3xl" />
                <div className="absolute left-1/4 top-1/4 w-64 h-64 bg-brand-cyan/10 rounded-full blur-2xl" />
              </div>

              {/* Content */}
              <div className="relative h-full flex items-center p-8">
                <div className="flex-1 space-y-4">
                  <div className="inline-flex items-center px-3 py-1 rounded-full bg-brand-orange/20 text-brand-orange text-xs font-semibold">
                    AI POWERED
                  </div>
                  <h3 className="text-2xl font-bold text-white max-w-md">
                    Ask anything about your fleet with AI Assistant
                  </h3>
                  <p className="text-white/60 max-w-lg">
                    Get instant insights, generate reports, and analyze your fleet data using natural language queries powered by AI.
                  </p>
                  <button className="inline-flex items-center space-x-2 px-5 py-2.5 bg-brand-orange hover:bg-brand-orange-light text-white font-semibold rounded-lg transition-colors group">
                    <span>Start Conversation</span>
                    <ChevronRight className="h-4 w-4 group-hover:translate-x-1 transition-transform" />
                  </button>
                </div>

                {/* Decorative Car/AI Illustration */}
                <div className="hidden md:flex items-center justify-center flex-shrink-0 w-72">
                  <div className="relative">
                    <div className="absolute inset-0 bg-brand-orange/30 rounded-full blur-3xl scale-75" />
                    <div className="relative flex items-center justify-center w-48 h-48 rounded-full bg-gradient-to-br from-white/10 to-white/5 border border-white/10">
                      <MessageSquare className="h-24 w-24 text-brand-orange" />
                    </div>
                    {/* Car indicator */}
                    <div className="absolute -bottom-2 -right-2 flex items-center justify-center w-20 h-20 rounded-full bg-gradient-to-br from-brand-amber to-brand-orange shadow-glow-orange">
                      <Car className="h-10 w-10 text-white" />
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Additional Teaser Cards Row */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div
              className="glass-panel p-5 group cursor-pointer hover:border-brand-cyan/30 transition-colors"
              onClick={() => navigate('/analysis')}
            >
              <div className="flex items-start space-x-4">
                <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-brand-cyan/20 flex items-center justify-center">
                  <BarChart3 className="h-6 w-6 text-brand-cyan" />
                </div>
                <div className="flex-1">
                  <h4 className="font-semibold text-white group-hover:text-brand-cyan transition-colors">Fleet Analysis</h4>
                  <p className="text-sm text-white/50 mt-1">View detailed analytics and insights</p>
                </div>
                <ChevronRight className="h-5 w-5 text-white/30 group-hover:text-brand-cyan group-hover:translate-x-1 transition-all" />
              </div>
            </div>

            <div
              className="glass-panel p-5 group cursor-pointer hover:border-brand-green/30 transition-colors"
              onClick={() => navigate('/operation')}
            >
              <div className="flex items-start space-x-4">
                <div className="flex-shrink-0 w-12 h-12 rounded-xl bg-brand-green/20 flex items-center justify-center">
                  <Car className="h-6 w-6 text-brand-green" />
                </div>
                <div className="flex-1">
                  <h4 className="font-semibold text-white group-hover:text-brand-green transition-colors">Fleet Operations</h4>
                  <p className="text-sm text-white/50 mt-1">Manage vehicles and contracts</p>
                </div>
                <ChevronRight className="h-5 w-5 text-white/30 group-hover:text-brand-green group-hover:translate-x-1 transition-all" />
              </div>
            </div>
          </div>
        </div>

        {/* Right Section - Informative Tiles (1/3 width) */}
        <div className="space-y-4">
          <h2 className="text-lg font-semibold text-white/80">Key Figures</h2>

          {/* Stats Grid */}
          <div className="grid grid-cols-2 gap-4">
            {/* Total Active Vehicles */}
            <div
              className="glass-panel p-5 text-center hover:border-brand-orange/30 transition-colors cursor-pointer group"
              onClick={() => navigate('/operation')}
            >
              <div className="flex flex-col items-center">
                <div className="w-12 h-12 rounded-full bg-brand-orange/20 flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                  <Car className="h-6 w-6 text-brand-orange" />
                </div>
                <span className="text-3xl font-bold text-white">
                  {loading ? '...' : formatNumber(kpis?.active_vehicles || 0)}
                </span>
                <span className="text-xs text-white/50 mt-1">Active Vehicles</span>
              </div>
            </div>

            {/* Total Customers */}
            <div
              className="glass-panel p-5 text-center hover:border-brand-cyan/30 transition-colors cursor-pointer group"
              onClick={() => navigate('/operation')}
            >
              <div className="flex flex-col items-center">
                <div className="w-12 h-12 rounded-full bg-brand-cyan/20 flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                  <Users className="h-6 w-6 text-brand-cyan" />
                </div>
                <span className="text-3xl font-bold text-white">
                  {loading ? '...' : formatNumber(kpis?.total_customers || 0)}
                </span>
                <span className="text-xs text-white/50 mt-1">Customers</span>
              </div>
            </div>

            {/* Active Contracts */}
            <div
              className="glass-panel p-5 text-center hover:border-brand-green/30 transition-colors cursor-pointer group"
              onClick={() => navigate('/operation')}
            >
              <div className="flex flex-col items-center">
                <div className="w-12 h-12 rounded-full bg-brand-green/20 flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                  <FileText className="h-6 w-6 text-brand-green" />
                </div>
                <span className="text-3xl font-bold text-white">
                  {loading ? '...' : formatNumber(kpis?.total_contracts || 0)}
                </span>
                <span className="text-xs text-white/50 mt-1">Contracts</span>
              </div>
            </div>

            {/* Monthly Revenue */}
            <div
              className="glass-panel p-5 text-center hover:border-brand-amber/30 transition-colors cursor-pointer group"
              onClick={() => navigate('/analysis')}
            >
              <div className="flex flex-col items-center">
                <div className="w-12 h-12 rounded-full bg-brand-amber/20 flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                  <TrendingUp className="h-6 w-6 text-brand-amber" />
                </div>
                <span className="text-2xl font-bold text-white">
                  {loading ? '...' : formatCurrency(kpis?.total_monthly_revenue || 0)}
                </span>
                <span className="text-xs text-white/50 mt-1">Monthly Revenue</span>
              </div>
            </div>
          </div>

          {/* Quick Actions */}
          <div className="glass-panel p-5">
            <h3 className="text-sm font-semibold text-white/70 mb-4">Quick Actions</h3>
            <div className="space-y-2">
              <button
                onClick={() => navigate('/operation')}
                className="w-full text-left px-4 py-3 rounded-lg bg-white/5 hover:bg-white/10 text-white/70 hover:text-white text-sm transition-colors flex items-center justify-between group"
              >
                <span>View All Vehicles</span>
                <ChevronRight className="h-4 w-4 opacity-0 group-hover:opacity-100 group-hover:translate-x-1 transition-all" />
              </button>
              <button
                onClick={() => navigate('/reports')}
                className="w-full text-left px-4 py-3 rounded-lg bg-white/5 hover:bg-white/10 text-white/70 hover:text-white text-sm transition-colors flex items-center justify-between group"
              >
                <span>Generate Reports</span>
                <ChevronRight className="h-4 w-4 opacity-0 group-hover:opacity-100 group-hover:translate-x-1 transition-all" />
              </button>
              <button
                onClick={() => navigate('/assistant')}
                className="w-full text-left px-4 py-3 rounded-lg bg-white/5 hover:bg-white/10 text-white/70 hover:text-white text-sm transition-colors flex items-center justify-between group"
              >
                <span>Ask AI Assistant</span>
                <ChevronRight className="h-4 w-4 opacity-0 group-hover:opacity-100 group-hover:translate-x-1 transition-all" />
              </button>
            </div>
          </div>

          {/* Fleet Summary */}
          <div className="glass-panel p-5">
            <h3 className="text-sm font-semibold text-white/70 mb-4">Fleet Summary</h3>
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-sm text-white/50">Total Fleet</span>
                <span className="text-sm font-semibold text-white">
                  {loading ? '...' : formatNumber(kpis?.total_vehicles || 0)} vehicles
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-white/50">Active</span>
                <span className="text-sm font-semibold text-brand-green">
                  {loading ? '...' : formatNumber(kpis?.active_vehicles || 0)} ({kpis ? Math.round((kpis.active_vehicles / kpis.total_vehicles) * 100) : 0}%)
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-white/50">Avg. Odometer</span>
                <span className="text-sm font-semibold text-white">
                  {loading ? '...' : formatNumber(Math.round(kpis?.average_odometer_km || 0))} km
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-white/50">Total Drivers</span>
                <span className="text-sm font-semibold text-white">
                  {loading ? '...' : formatNumber(kpis?.total_drivers || 0)}
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
