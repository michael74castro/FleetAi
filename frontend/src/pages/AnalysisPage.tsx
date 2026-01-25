import { BarChart3, TrendingUp, PieChart, Activity } from 'lucide-react';

export default function AnalysisPage() {
  return (
    <div className="space-y-6 animate-fade-in">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">Analysis</h1>
          <p className="text-white/60 mt-1">Data insights and fleet analytics</p>
        </div>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="glass-panel p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white/60 text-sm">Cost Efficiency</p>
              <p className="text-2xl font-bold text-white mt-1">92%</p>
            </div>
            <div className="h-12 w-12 rounded-xl bg-brand-green/20 flex items-center justify-center">
              <TrendingUp className="h-6 w-6 text-brand-green" />
            </div>
          </div>
        </div>

        <div className="glass-panel p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white/60 text-sm">Fuel Economy</p>
              <p className="text-2xl font-bold text-white mt-1">8.2L/100km</p>
            </div>
            <div className="h-12 w-12 rounded-xl bg-brand-cyan/20 flex items-center justify-center">
              <Activity className="h-6 w-6 text-brand-cyan" />
            </div>
          </div>
        </div>

        <div className="glass-panel p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white/60 text-sm">Maintenance Score</p>
              <p className="text-2xl font-bold text-white mt-1">A+</p>
            </div>
            <div className="h-12 w-12 rounded-xl bg-brand-orange/20 flex items-center justify-center">
              <PieChart className="h-6 w-6 text-brand-orange" />
            </div>
          </div>
        </div>

        <div className="glass-panel p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white/60 text-sm">Data Points</p>
              <p className="text-2xl font-bold text-white mt-1">1.2M</p>
            </div>
            <div className="h-12 w-12 rounded-xl bg-brand-amber/20 flex items-center justify-center">
              <BarChart3 className="h-6 w-6 text-brand-amber" />
            </div>
          </div>
        </div>
      </div>

      {/* Main Content Area */}
      <div className="glass-panel p-8 text-center">
        <BarChart3 className="h-16 w-16 text-brand-orange mx-auto mb-4" />
        <h2 className="text-xl font-semibold text-white mb-2">Fleet Analytics</h2>
        <p className="text-white/60 max-w-md mx-auto">
          Dive deep into your fleet data with advanced analytics, trends, and predictive insights.
          Charts, graphs, and detailed analysis tools will be available here.
        </p>
      </div>
    </div>
  );
}
