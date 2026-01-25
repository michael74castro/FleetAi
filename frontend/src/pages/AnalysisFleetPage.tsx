import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ChevronRight, ChevronDown, Info } from 'lucide-react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  AreaChart,
  Area,
  PieChart,
  Pie,
  Cell
} from 'recharts';

// Mock data for charts
const additionsTerminationsData = [
  { manufacturer: 'Alfa Romeo', terminated: 2, added: 1 },
  { manufacturer: 'Audi', terminated: 15, added: 20 },
  { manufacturer: 'BMW', terminated: 25, added: 30 },
  { manufacturer: 'Chevrolet', terminated: 5, added: 3 },
  { manufacturer: 'Citroën', terminated: 8, added: 12 },
  { manufacturer: 'Dacia', terminated: 3, added: 5 },
  { manufacturer: 'Fiat', terminated: 10, added: 8 },
  { manufacturer: 'Ford', terminated: 45, added: 50 },
  { manufacturer: 'Honda', terminated: 12, added: 15 },
  { manufacturer: 'Hyundai', terminated: 18, added: 22 },
  { manufacturer: 'Jaguar', terminated: 4, added: 6 },
  { manufacturer: 'Jeep', terminated: 7, added: 9 },
  { manufacturer: 'Kia', terminated: 14, added: 18 },
  { manufacturer: 'Land Rover', terminated: 8, added: 10 },
  { manufacturer: 'Lexus', terminated: 3, added: 4 },
];

const fleetSizePerMonthData = [
  { month: 'Jan', sedan: 400, suv: 300, pickup: 200, van: 150, other: 100 },
  { month: 'Feb', sedan: 420, suv: 310, pickup: 210, van: 155, other: 105 },
  { month: 'Mar', sedan: 450, suv: 320, pickup: 220, van: 160, other: 110 },
  { month: 'Apr', sedan: 470, suv: 340, pickup: 230, van: 165, other: 115 },
  { month: 'May', sedan: 490, suv: 350, pickup: 240, van: 170, other: 120 },
  { month: 'Jun', sedan: 510, suv: 360, pickup: 250, van: 175, other: 125 },
  { month: 'Jul', sedan: 530, suv: 380, pickup: 260, van: 180, other: 130 },
  { month: 'Aug', sedan: 520, suv: 370, pickup: 255, van: 178, other: 128 },
  { month: 'Sep', sedan: 540, suv: 390, pickup: 265, van: 182, other: 132 },
  { month: 'Oct', sedan: 560, suv: 400, pickup: 275, van: 188, other: 138 },
  { month: 'Nov', sedan: 580, suv: 420, pickup: 285, van: 192, other: 142 },
  { month: 'Dec', sedan: 600, suv: 440, pickup: 295, van: 198, other: 148 },
];

const fleetPerManufacturerData = [
  { name: 'Toyota', value: 450, color: '#FF6B35' },
  { name: 'Nissan', value: 380, color: '#00D4FF' },
  { name: 'Ford', value: 320, color: '#7C3AED' },
  { name: 'BMW', value: 280, color: '#10B981' },
  { name: 'Mercedes', value: 250, color: '#F59E0B' },
  { name: 'Audi', value: 220, color: '#EF4444' },
  { name: 'Honda', value: 180, color: '#8B5CF6' },
  { name: 'Hyundai', value: 160, color: '#06B6D4' },
  { name: 'Kia', value: 140, color: '#84CC16' },
  { name: 'Volkswagen', value: 120, color: '#F97316' },
  { name: 'Other', value: 300, color: '#6B7280' },
];

const fleetPerCostCentreData = [
  { month: 'Jan', sales: 200, operations: 150, admin: 100, logistics: 180, hr: 50 },
  { month: 'Feb', sales: 210, operations: 155, admin: 105, logistics: 185, hr: 52 },
  { month: 'Mar', sales: 220, operations: 160, admin: 110, logistics: 190, hr: 55 },
  { month: 'Apr', sales: 230, operations: 170, admin: 115, logistics: 200, hr: 58 },
  { month: 'May', sales: 240, operations: 175, admin: 120, logistics: 210, hr: 60 },
  { month: 'Jun', sales: 250, operations: 180, admin: 125, logistics: 220, hr: 62 },
  { month: 'Jul', sales: 260, operations: 190, admin: 130, logistics: 230, hr: 65 },
  { month: 'Aug', sales: 255, operations: 185, admin: 128, logistics: 225, hr: 63 },
  { month: 'Sep', sales: 265, operations: 195, admin: 132, logistics: 235, hr: 67 },
  { month: 'Oct', sales: 275, operations: 200, admin: 138, logistics: 245, hr: 70 },
  { month: 'Nov', sales: 285, operations: 210, admin: 142, logistics: 255, hr: 72 },
  { month: 'Dec', sales: 295, operations: 220, admin: 148, logistics: 265, hr: 75 },
];

const COLORS = {
  primary: '#FF6B35',
  secondary: '#00D4FF',
  tertiary: '#7C3AED',
  success: '#10B981',
  warning: '#F59E0B',
  danger: '#EF4444',
  purple: '#8B5CF6',
  cyan: '#06B6D4',
  lime: '#84CC16',
  orange: '#F97316',
};

export default function AnalysisFleetPage() {
  const [loading, setLoading] = useState(true);
  const [yearFilter, setYearFilter] = useState('All');
  const [costCentreFilter, setCostCentreFilter] = useState('All');
  const [statusFilter, setStatusFilter] = useState('All');
  const [lastDataUpdate] = useState('2026-01-24T04:33:32Z');
  const [lastMonthlyClosure] = useState('December 2025');

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
        <span className="text-white/60">Fleet</span>
      </nav>

      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <h1 className="text-2xl font-bold text-white">Analysis - Fleet</h1>
          <button className="text-white/40 hover:text-white/60 transition-colors">
            <Info className="h-5 w-5" />
          </button>
        </div>
        <p className="text-sm text-white/50">
          Last data update: {formatDate(lastDataUpdate)} | Last monthly closure: {lastMonthlyClosure}
        </p>
      </div>

      {/* Filter Row */}
      <div className="glass-panel p-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Year */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Year</label>
            <div className="relative">
              <select
                value={yearFilter}
                onChange={(e) => setYearFilter(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="All">All</option>
                <option value="2026">2026</option>
                <option value="2025">2025</option>
                <option value="2024">2024</option>
                <option value="2023">2023</option>
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
                <option value="All">All</option>
                <option value="sales">Sales</option>
                <option value="operations">Operations</option>
                <option value="admin">Administration</option>
                <option value="logistics">Logistics</option>
                <option value="hr">Human Resources</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>

          {/* Status */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Status</label>
            <div className="relative">
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="All">All</option>
                <option value="active">Active</option>
                <option value="inactive">Inactive</option>
                <option value="terminated">Terminated</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>
        </div>
      </div>

      {/* Charts Grid - 2x2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Chart 1: Additions and terminations per manufacturer */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Additions and terminations per manufacturer in last 12 months
          </h3>
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={additionsTerminationsData}
                layout="vertical"
                margin={{ top: 5, right: 30, left: 80, bottom: 5 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                <XAxis type="number" stroke="rgba(255,255,255,0.5)" fontSize={12} />
                <YAxis dataKey="manufacturer" type="category" stroke="rgba(255,255,255,0.5)" fontSize={11} width={75} />
                <Tooltip content={<CustomTooltip />} />
                <Legend
                  wrapperStyle={{ paddingTop: '10px' }}
                  formatter={(value) => <span className="text-white/70 text-xs">{value}</span>}
                />
                <Bar dataKey="terminated" name="Terminated (last 12 months)" fill={COLORS.danger} radius={[0, 4, 4, 0]} />
                <Bar dataKey="added" name="Added (last 12 months)" fill={COLORS.success} radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Chart 2: Live fleet size per month */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Live fleet size per month
          </h3>
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart
                data={fleetSizePerMonthData}
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
                <Area type="monotone" dataKey="sedan" stackId="1" stroke={COLORS.primary} fill={COLORS.primary} fillOpacity={0.8} name="Sedan" />
                <Area type="monotone" dataKey="suv" stackId="1" stroke={COLORS.secondary} fill={COLORS.secondary} fillOpacity={0.8} name="SUV" />
                <Area type="monotone" dataKey="pickup" stackId="1" stroke={COLORS.tertiary} fill={COLORS.tertiary} fillOpacity={0.8} name="Pickup" />
                <Area type="monotone" dataKey="van" stackId="1" stroke={COLORS.warning} fill={COLORS.warning} fillOpacity={0.8} name="Van" />
                <Area type="monotone" dataKey="other" stackId="1" stroke={COLORS.cyan} fill={COLORS.cyan} fillOpacity={0.8} name="Other" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Chart 3: Current live fleet per manufacturer */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Current live fleet per manufacturer
          </h3>
          <div className="h-[400px] flex">
            {/* Pie Chart */}
            <div className="w-1/2">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={fleetPerManufacturerData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={120}
                    paddingAngle={2}
                    dataKey="value"
                  >
                    {fleetPerManufacturerData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip
                    formatter={(value: number) => [value.toLocaleString(), 'Vehicles']}
                    contentStyle={{
                      backgroundColor: 'rgba(17, 24, 39, 0.9)',
                      border: '1px solid rgba(255,255,255,0.1)',
                      borderRadius: '8px',
                    }}
                    itemStyle={{ color: '#fff' }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            {/* Legend */}
            <div className="w-1/2 flex flex-col justify-center space-y-2 pl-4">
              {fleetPerManufacturerData.map((entry, index) => (
                <div key={index} className="flex items-center space-x-2">
                  <div
                    className="w-3 h-3 rounded-full flex-shrink-0"
                    style={{ backgroundColor: entry.color }}
                  />
                  <span className="text-xs text-white/70 truncate">{entry.name}</span>
                  <span className="text-xs text-white/50 ml-auto">{entry.value.toLocaleString()}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Chart 4: Current live fleet per cost centre */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Current live fleet per cost centre
          </h3>
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart
                data={fleetPerCostCentreData}
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
                <Area type="monotone" dataKey="sales" stackId="1" stroke={COLORS.primary} fill={COLORS.primary} fillOpacity={0.8} name="Sales" />
                <Area type="monotone" dataKey="operations" stackId="1" stroke={COLORS.secondary} fill={COLORS.secondary} fillOpacity={0.8} name="Operations" />
                <Area type="monotone" dataKey="admin" stackId="1" stroke={COLORS.tertiary} fill={COLORS.tertiary} fillOpacity={0.8} name="Administration" />
                <Area type="monotone" dataKey="logistics" stackId="1" stroke={COLORS.warning} fill={COLORS.warning} fillOpacity={0.8} name="Logistics" />
                <Area type="monotone" dataKey="hr" stackId="1" stroke={COLORS.success} fill={COLORS.success} fillOpacity={0.8} name="Human Resources" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
