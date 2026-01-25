import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ChevronRight, ChevronDown, Info, Plus } from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

// Generate mock data for fine escalations per month
const generateFineEscalationsData = () => {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return months.map((month) => ({
    month,
    '2020': Math.floor(Math.random() * 50) + 10,
    '2021': Math.floor(Math.random() * 60) + 15,
    '2022': Math.floor(Math.random() * 70) + 20,
    '2023': Math.floor(Math.random() * 80) + 25,
    '2024': Math.floor(Math.random() * 90) + 30,
    '2025': Math.floor(Math.random() * 100) + 35,
    '2026': Math.floor(Math.random() * 50) + 20,
  }));
};

// Generate mock data for fines & penalties per month
const generateFinesPenaltiesData = () => {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return months.map((month) => ({
    month,
    '2020': Math.floor(Math.random() * 5000) + 1000,
    '2021': Math.floor(Math.random() * 6000) + 1500,
    '2022': Math.floor(Math.random() * 7000) + 2000,
    '2023': Math.floor(Math.random() * 8000) + 2500,
    '2024': Math.floor(Math.random() * 9000) + 3000,
    '2025': Math.floor(Math.random() * 10000) + 3500,
    '2026': Math.floor(Math.random() * 5000) + 2000,
  }));
};

const YEAR_COLORS: Record<string, string> = {
  '2020': '#3B82F6', // blue
  '2021': '#10B981', // green
  '2022': '#F59E0B', // amber
  '2023': '#EF4444', // red
  '2024': '#8B5CF6', // purple
  '2025': '#06B6D4', // cyan
  '2026': '#FF6B35', // orange (brand)
};

export default function AnalysisFinesPage() {
  const [loading, setLoading] = useState(true);
  const [customerNoFilter, setCustomerNoFilter] = useState('All');
  const [costCentreFilter, setCostCentreFilter] = useState('All');
  const [yearFilter, setYearFilter] = useState('All');
  const [lastDataUpdate] = useState('2026-01-23T04:33:32Z');
  const [lastMonthlyClosure] = useState('December 2025');

  const [fineEscalationsData] = useState(generateFineEscalationsData);
  const [finesPenaltiesData] = useState(generateFinesPenaltiesData);

  // Years to display in charts
  const displayYears = ['2020', '2021', '2022', '2023', '2024', '2025', '2026'];

  useEffect(() => {
    // Simulate loading
    const timer = setTimeout(() => setLoading(false), 500);
    return () => clearTimeout(timer);
  }, []);

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'long',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      timeZone: 'UTC'
    }) + ' UTC';
  };

  const CustomTooltip = ({ active, payload, label }: { active?: boolean; payload?: Array<{ name: string; value: number; color: string }>; label?: string }) => {
    if (active && payload && payload.length) {
      return (
        <div className="glass-panel p-3 text-sm">
          <p className="text-white font-medium mb-2">{label}</p>
          {payload.map((entry, index) => (
            <p key={index} style={{ color: entry.color }} className="flex justify-between gap-4">
              <span>{entry.name}:</span>
              <span className="font-medium">{entry.value.toLocaleString()}</span>
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-brand-orange"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Breadcrumb */}
      <nav className="flex items-center space-x-2 text-sm">
        <Link to="/" className="text-brand-orange hover:text-brand-orange-light transition-colors">
          Home
        </Link>
        <ChevronRight className="h-4 w-4 text-white/40" />
        <Link to="/analysis" className="text-brand-orange hover:text-brand-orange-light transition-colors">
          Analysis
        </Link>
        <ChevronRight className="h-4 w-4 text-white/40" />
        <span className="text-white/60">Fines</span>
      </nav>

      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <h1 className="text-2xl font-bold text-white">Analysis - Fines</h1>
          <button className="text-white/40 hover:text-white/60 transition-colors">
            <Info className="h-5 w-5" />
          </button>
        </div>
        <p className="text-sm text-white/50">
          Last data update: {formatDate(lastDataUpdate)} | Last monthly closure: {lastMonthlyClosure}
        </p>
      </div>

      {/* Filter Row - 3 filters */}
      <div className="glass-panel p-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* CustomerNo */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">CustomerNo</label>
            <div className="relative">
              <select
                value={customerNoFilter}
                onChange={(e) => setCustomerNoFilter(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="All">(All)</option>
                <option value="C001">C001</option>
                <option value="C002">C002</option>
                <option value="C003">C003</option>
                <option value="C004">C004</option>
                <option value="C005">C005</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>

          {/* Cost Centre */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Cost centre</label>
            <div className="relative">
              <select
                value={costCentreFilter}
                onChange={(e) => setCostCentreFilter(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="All">(All)</option>
                <option value="sales">Sales</option>
                <option value="operations">Operations</option>
                <option value="admin">Administration</option>
                <option value="logistics">Logistics</option>
                <option value="hr">Human Resources</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>

          {/* Year */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Year</label>
            <div className="relative">
              <select
                value={yearFilter}
                onChange={(e) => setYearFilter(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="All">(All)</option>
                <option value="2026">2026</option>
                <option value="2025">2025</option>
                <option value="2024">2024</option>
                <option value="2023">2023</option>
                <option value="2022">2022</option>
                <option value="2021">2021</option>
                <option value="2020">2020</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>
        </div>
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Chart 1: Fine escalations per month */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-brand-orange mb-4">
            Fine escalations per month
          </h3>
          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={fineEscalationsData}
                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                <XAxis dataKey="month" stroke="rgba(255,255,255,0.5)" fontSize={12} />
                <YAxis stroke="rgba(255,255,255,0.5)" fontSize={12} />
                <Tooltip content={<CustomTooltip />} />
                <Legend
                  wrapperStyle={{ paddingTop: '10px' }}
                  formatter={(value) => <span className="text-white/70 text-xs">{value}</span>}
                />
                {displayYears.map((year) => (
                  <Line
                    key={year}
                    type="monotone"
                    dataKey={year}
                    name={year}
                    stroke={YEAR_COLORS[year]}
                    strokeWidth={2}
                    dot={{ r: 3 }}
                    activeDot={{ r: 5 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Chart 2: Fines & penalties per month */}
        <div className="glass-panel p-5 relative">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-brand-orange">
              Fines & penalties per month
            </h3>
            <button className="p-1.5 rounded-lg hover:bg-white/10 transition-colors text-white/50 hover:text-white">
              <Plus className="h-4 w-4" />
            </button>
          </div>
          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={finesPenaltiesData}
                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                <XAxis dataKey="month" stroke="rgba(255,255,255,0.5)" fontSize={12} />
                <YAxis stroke="rgba(255,255,255,0.5)" fontSize={12} tickFormatter={(value) => `${(value / 1000).toFixed(0)}k`} />
                <Tooltip content={<CustomTooltip />} />
                <Legend
                  wrapperStyle={{ paddingTop: '10px' }}
                  formatter={(value) => <span className="text-white/70 text-xs">{value}</span>}
                />
                {displayYears.map((year) => (
                  <Line
                    key={year}
                    type="monotone"
                    dataKey={year}
                    name={year}
                    stroke={YEAR_COLORS[year]}
                    strokeWidth={2}
                    dot={{ r: 3 }}
                    activeDot={{ r: 5 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
