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
  ResponsiveContainer
} from 'recharts';

// Mock data for charts
const additionsPerMonthData = [
  { month: 'Jan', sedan: 15, suv: 12, pickup: 8, van: 5, other: 3 },
  { month: 'Feb', sedan: 18, suv: 14, pickup: 10, van: 6, other: 4 },
  { month: 'Mar', sedan: 22, suv: 16, pickup: 12, van: 7, other: 5 },
  { month: 'Apr', sedan: 20, suv: 18, pickup: 14, van: 8, other: 4 },
  { month: 'May', sedan: 25, suv: 20, pickup: 15, van: 9, other: 6 },
  { month: 'Jun', sedan: 28, suv: 22, pickup: 16, van: 10, other: 5 },
  { month: 'Jul', sedan: 30, suv: 25, pickup: 18, van: 12, other: 7 },
  { month: 'Aug', sedan: 26, suv: 21, pickup: 15, van: 10, other: 5 },
  { month: 'Sep', sedan: 24, suv: 19, pickup: 14, van: 9, other: 4 },
  { month: 'Oct', sedan: 22, suv: 17, pickup: 12, van: 8, other: 4 },
  { month: 'Nov', sedan: 20, suv: 15, pickup: 10, van: 7, other: 3 },
  { month: 'Dec', sedan: 18, suv: 13, pickup: 9, van: 6, other: 3 },
];

const terminationsPerMonthData = [
  { month: 'Jan', sedan: 10, suv: 8, pickup: 5, van: 3, other: 2 },
  { month: 'Feb', sedan: 12, suv: 9, pickup: 6, van: 4, other: 2 },
  { month: 'Mar', sedan: 14, suv: 11, pickup: 7, van: 4, other: 3 },
  { month: 'Apr', sedan: 13, suv: 10, pickup: 8, van: 5, other: 3 },
  { month: 'May', sedan: 16, suv: 12, pickup: 9, van: 5, other: 4 },
  { month: 'Jun', sedan: 18, suv: 14, pickup: 10, van: 6, other: 4 },
  { month: 'Jul', sedan: 20, suv: 16, pickup: 11, van: 7, other: 5 },
  { month: 'Aug', sedan: 17, suv: 13, pickup: 9, van: 6, other: 4 },
  { month: 'Sep', sedan: 15, suv: 12, pickup: 8, van: 5, other: 3 },
  { month: 'Oct', sedan: 14, suv: 10, pickup: 7, van: 5, other: 3 },
  { month: 'Nov', sedan: 12, suv: 9, pickup: 6, van: 4, other: 2 },
  { month: 'Dec', sedan: 11, suv: 8, pickup: 5, van: 3, other: 2 },
];

const ordersByManufacturerData = [
  { manufacturer: 'Toyota', newOrders: 45, renewalOrders: 32 },
  { manufacturer: 'Nissan', newOrders: 38, renewalOrders: 28 },
  { manufacturer: 'Ford', newOrders: 35, renewalOrders: 25 },
  { manufacturer: 'BMW', newOrders: 28, renewalOrders: 20 },
  { manufacturer: 'Mercedes', newOrders: 25, renewalOrders: 18 },
  { manufacturer: 'Audi', newOrders: 22, renewalOrders: 16 },
  { manufacturer: 'Honda', newOrders: 20, renewalOrders: 14 },
  { manufacturer: 'Hyundai', newOrders: 18, renewalOrders: 12 },
  { manufacturer: 'Kia', newOrders: 15, renewalOrders: 10 },
  { manufacturer: 'Volkswagen', newOrders: 12, renewalOrders: 8 },
];

const renewalsDueOverdueData = [
  { name: 'Due for renewal', value: 926 },
  { name: 'Overdue for renewal with order', value: 780 },
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
  teal: '#14B8A6',
};

export default function AnalysisRenewalsPage() {
  const [loading, setLoading] = useState(true);
  const [yearFilter, setYearFilter] = useState('All');
  const [costCentreFilter, setCostCentreFilter] = useState('All');
  const [vehicleTypeFilter, setVehicleTypeFilter] = useState('All');
  const [manufacturerFilter, setManufacturerFilter] = useState('All');
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
        <span className="text-white/60">Renewals & Orders</span>
      </nav>

      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <h1 className="text-2xl font-bold text-white">Analysis - Renewals & Orders</h1>
          <button className="text-white/40 hover:text-white/60 transition-colors">
            <Info className="h-5 w-5" />
          </button>
        </div>
        <p className="text-sm text-white/50">
          Last data update: {formatDate(lastDataUpdate)} | Last monthly closure: {lastMonthlyClosure}
        </p>
      </div>

      {/* Filter Row - 4 filters */}
      <div className="glass-panel p-4">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
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

          {/* Vehicle Type */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Vehicle type</label>
            <div className="relative">
              <select
                value={vehicleTypeFilter}
                onChange={(e) => setVehicleTypeFilter(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="All">All</option>
                <option value="sedan">Sedan</option>
                <option value="suv">SUV</option>
                <option value="pickup">Pickup</option>
                <option value="van">Van</option>
                <option value="other">Other</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>

          {/* Manufacturer */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Manufacturer</label>
            <div className="relative">
              <select
                value={manufacturerFilter}
                onChange={(e) => setManufacturerFilter(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="All">All</option>
                <option value="Toyota">Toyota</option>
                <option value="Nissan">Nissan</option>
                <option value="Ford">Ford</option>
                <option value="BMW">BMW</option>
                <option value="Mercedes">Mercedes</option>
                <option value="Audi">Audi</option>
                <option value="Honda">Honda</option>
                <option value="Hyundai">Hyundai</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>
        </div>
      </div>

      {/* Charts Grid - 2x2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Chart 1: Additions per month */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Additions per month
          </h3>
          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={additionsPerMonthData}
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
                <Bar dataKey="sedan" name="Sedan" fill={COLORS.primary} radius={[2, 2, 0, 0]} />
                <Bar dataKey="suv" name="SUV" fill={COLORS.secondary} radius={[2, 2, 0, 0]} />
                <Bar dataKey="pickup" name="Pickup" fill={COLORS.tertiary} radius={[2, 2, 0, 0]} />
                <Bar dataKey="van" name="Van" fill={COLORS.warning} radius={[2, 2, 0, 0]} />
                <Bar dataKey="other" name="Other" fill={COLORS.cyan} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Chart 2: Terminations per month */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Terminations per month
          </h3>
          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={terminationsPerMonthData}
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
                <Bar dataKey="sedan" name="Sedan" fill={COLORS.primary} radius={[2, 2, 0, 0]} />
                <Bar dataKey="suv" name="SUV" fill={COLORS.secondary} radius={[2, 2, 0, 0]} />
                <Bar dataKey="pickup" name="Pickup" fill={COLORS.tertiary} radius={[2, 2, 0, 0]} />
                <Bar dataKey="van" name="Van" fill={COLORS.warning} radius={[2, 2, 0, 0]} />
                <Bar dataKey="other" name="Other" fill={COLORS.cyan} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Chart 3: Current orders by manufacturer */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Current orders by manufacturer
          </h3>
          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={ordersByManufacturerData}
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
                <Bar dataKey="newOrders" name="New Orders" fill={COLORS.primary} radius={[0, 4, 4, 0]} />
                <Bar dataKey="renewalOrders" name="Renewal Orders" fill={COLORS.teal} radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Chart 4: Current renewals due and overdue */}
        <div className="glass-panel p-5">
          <h3 className="text-sm font-semibold text-white mb-4">
            Current renewals due and overdue
          </h3>
          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={renewalsDueOverdueData}
                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
                <XAxis dataKey="name" stroke="rgba(255,255,255,0.5)" fontSize={11} />
                <YAxis stroke="rgba(255,255,255,0.5)" fontSize={12} />
                <Tooltip
                  formatter={(value: number) => [value.toLocaleString(), 'Vehicles']}
                  contentStyle={{
                    backgroundColor: 'rgba(17, 24, 39, 0.9)',
                    border: '1px solid rgba(255,255,255,0.1)',
                    borderRadius: '8px',
                  }}
                  itemStyle={{ color: '#fff' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Bar dataKey="value" name="No. of Vehicles" fill={COLORS.teal} radius={[4, 4, 0, 0]} barSize={80} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
