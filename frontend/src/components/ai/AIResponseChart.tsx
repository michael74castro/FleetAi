import { useMemo } from 'react';
import {
  BarChart, Bar,
  LineChart, Line,
  AreaChart, Area,
  PieChart, Pie, Cell,
  XAxis, YAxis, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts';
import { BarChart3, TrendingUp, PieChart as PieIcon } from 'lucide-react';
import type { ChartConfig } from '@/types';

// Glowing color palette with corresponding glow colors
const CHART_COLORS = [
  { fill: '#F06400', glow: 'rgba(240, 100, 0, 0.6)' },    // Brand Orange
  { fill: '#00B1AF', glow: 'rgba(0, 177, 175, 0.6)' },    // Teal
  { fill: '#ED8B00', glow: 'rgba(237, 139, 0, 0.6)' },    // Amber
  { fill: '#FF7A1A', glow: 'rgba(255, 122, 26, 0.6)' },   // Light Orange
  { fill: '#00D4D1', glow: 'rgba(0, 212, 209, 0.6)' },    // Cyan
  { fill: '#009A17', glow: 'rgba(0, 154, 23, 0.6)' },     // Green
  { fill: '#A9C90E', glow: 'rgba(169, 201, 14, 0.6)' },   // Lime
  { fill: '#FF9500', glow: 'rgba(255, 149, 0, 0.6)' },    // Golden Orange
];

// Get color by index
const getColor = (index: number) => CHART_COLORS[index % CHART_COLORS.length].fill;
const getGlow = (index: number) => CHART_COLORS[index % CHART_COLORS.length].glow;

// SVG filter for intense glow effect
const GlowFilters = () => (
  <defs>
    {CHART_COLORS.map((color, i) => (
      <filter key={i} id={`glow-${i}`} x="-100%" y="-100%" width="300%" height="300%">
        <feGaussianBlur stdDeviation="8" result="blur1" />
        <feGaussianBlur stdDeviation="4" result="blur2" />
        <feFlood floodColor={color.fill} floodOpacity="1" result="glowColor" />
        <feComposite in="glowColor" in2="blur1" operator="in" result="softGlow1" />
        <feComposite in="glowColor" in2="blur2" operator="in" result="softGlow2" />
        <feMerge>
          <feMergeNode in="softGlow1" />
          <feMergeNode in="softGlow1" />
          <feMergeNode in="softGlow2" />
          <feMergeNode in="SourceGraphic" />
        </feMerge>
      </filter>
    ))}
    <filter id="glow-intense" x="-100%" y="-100%" width="300%" height="300%">
      <feGaussianBlur stdDeviation="12" result="blur1" />
      <feGaussianBlur stdDeviation="6" result="blur2" />
      <feFlood floodColor="#F06400" floodOpacity="1" result="glowColor" />
      <feComposite in="glowColor" in2="blur1" operator="in" result="softGlow1" />
      <feComposite in="glowColor" in2="blur2" operator="in" result="softGlow2" />
      <feMerge>
        <feMergeNode in="softGlow1" />
        <feMergeNode in="softGlow1" />
        <feMergeNode in="softGlow2" />
        <feMergeNode in="softGlow2" />
        <feMergeNode in="SourceGraphic" />
      </feMerge>
    </filter>
  </defs>
);

// Glassmorphism tooltip style with glow
const TOOLTIP_STYLE: React.CSSProperties = {
  backgroundColor: 'rgba(15, 23, 42, 0.85)',
  backdropFilter: 'blur(16px)',
  WebkitBackdropFilter: 'blur(16px)',
  border: '1px solid rgba(240, 100, 0, 0.3)',
  borderRadius: '12px',
  boxShadow: '0 0 20px rgba(240, 100, 0, 0.3), 0 8px 32px rgba(0, 0, 0, 0.4)',
  padding: '12px 16px',
};

const TOOLTIP_LABEL_STYLE: React.CSSProperties = {
  color: '#F06400',
  fontWeight: 600,
  fontSize: '13px',
  marginBottom: '8px',
  textShadow: '0 0 10px rgba(240, 100, 0, 0.5)',
};

const TOOLTIP_ITEM_STYLE: React.CSSProperties = {
  color: '#FFFFFF',
  fontSize: '12px',
  padding: '4px 0',
  textShadow: '0 1px 2px rgba(0, 0, 0, 0.2)',
};

const AXIS_STYLE = {
  fontSize: 11,
  fill: 'rgba(255, 255, 255, 0.4)',
};

interface AIResponseChartProps {
  data: Record<string, unknown>[];
  chartConfig: ChartConfig;
}

export default function AIResponseChart({ data, chartConfig }: AIResponseChartProps) {
  const { chart_type, x_axis_key, y_axis_keys } = chartConfig;

  const xKey = useMemo(() => {
    if (x_axis_key) return x_axis_key;
    const keys = Object.keys(data[0] || {});
    return keys[0] || '';
  }, [x_axis_key, data]);

  const yKeys = useMemo(() => {
    const idPatterns = ['_id', 'period', 'year', 'month', 'week', 'code', 'number'];
    const isIdKey = (k: string) => idPatterns.some(p => k.toLowerCase().includes(p));
    if (y_axis_keys && y_axis_keys.length > 0) {
      return y_axis_keys.filter(k => k !== xKey && !isIdKey(k));
    }
    const keys = Object.keys(data[0] || {}).filter(k => k !== xKey && !isIdKey(k));
    return keys.filter(k => typeof data[0]?.[k] === 'number');
  }, [y_axis_keys, data, xKey]);

  const ChartIcon = chart_type === 'pie' || chart_type === 'donut'
    ? PieIcon
    : chart_type === 'line' || chart_type === 'area'
      ? TrendingUp
      : BarChart3;

  if (!data || data.length === 0 || yKeys.length === 0) {
    return null;
  }

  const renderChart = () => {
    switch (chart_type) {
      case 'pie':
      case 'donut': {
        const pieData = data.map(row => ({
          name: String(row[xKey] ?? ''),
          value: Number(row[yKeys[0]] ?? 0),
        }));
        return (
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <GlowFilters />
              <Pie
                data={pieData}
                cx="50%"
                cy="50%"
                innerRadius={chart_type === 'donut' ? '40%' : 0}
                outerRadius="70%"
                paddingAngle={4}
                dataKey="value"
                nameKey="name"
                label={({ name, percent }) =>
                  `${name}: ${(percent * 100).toFixed(0)}%`
                }
                labelLine={{ stroke: 'rgba(240,100,0,0.5)', strokeWidth: 1 }}
              >
                {pieData.map((_, i) => (
                  <Cell
                    key={i}
                    fill={getColor(i)}
                    filter={`url(#glow-${i})`}
                    style={{ cursor: 'pointer' }}
                  />
                ))}
              </Pie>
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={TOOLTIP_LABEL_STYLE}
                itemStyle={TOOLTIP_ITEM_STYLE}
              />
              <Legend
                wrapperStyle={{ fontSize: '12px', color: 'rgba(255,255,255,0.8)', paddingTop: '10px' }}
              />
            </PieChart>
          </ResponsiveContainer>
        );
      }

      case 'line':
        return (
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
              <GlowFilters />
              <XAxis dataKey={xKey} tick={AXIS_STYLE} stroke="transparent" axisLine={false} tickLine={false} />
              <YAxis
                tick={AXIS_STYLE}
                stroke="transparent"
                axisLine={false}
                tickLine={false}
                domain={['dataMin * 0.95', 'dataMax * 1.02']}
                tickFormatter={(value) => value >= 1000 ? `${(value / 1000).toFixed(0)}k` : value}
              />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={TOOLTIP_LABEL_STYLE}
                itemStyle={TOOLTIP_ITEM_STYLE}
                formatter={(value: number) => value.toLocaleString()}
              />
              <Legend wrapperStyle={{ fontSize: '12px', color: 'rgba(255,255,255,0.8)' }} />
              {yKeys.map((key, i) => (
                <Line
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={getColor(i)}
                  strokeWidth={4}
                  dot={{
                    r: 6,
                    fill: getColor(i),
                    stroke: getColor(i),
                    strokeWidth: 2,
                    filter: `url(#glow-${i})`
                  }}
                  activeDot={{
                    r: 10,
                    fill: getColor(i),
                    stroke: getColor(i),
                    strokeWidth: 3,
                    filter: `url(#glow-intense)`
                  }}
                  filter={`url(#glow-${i})`}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        );

      case 'area':
        return (
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
              <GlowFilters />
              <XAxis dataKey={xKey} tick={AXIS_STYLE} stroke="transparent" axisLine={false} tickLine={false} />
              <YAxis
                tick={AXIS_STYLE}
                stroke="transparent"
                axisLine={false}
                tickLine={false}
                domain={['dataMin * 0.95', 'dataMax * 1.02']}
                tickFormatter={(value) => value >= 1000 ? `${(value / 1000).toFixed(0)}k` : value}
              />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={TOOLTIP_LABEL_STYLE}
                itemStyle={TOOLTIP_ITEM_STYLE}
                formatter={(value: number) => value.toLocaleString()}
              />
              <Legend wrapperStyle={{ fontSize: '12px', color: 'rgba(255,255,255,0.8)' }} />
              {yKeys.map((key, i) => (
                <Area
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={getColor(i)}
                  fill={getColor(i)}
                  fillOpacity={0.25}
                  strokeWidth={4}
                  filter={`url(#glow-${i})`}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        );

      case 'bar':
      default:
        return (
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
              <GlowFilters />
              <XAxis dataKey={xKey} tick={AXIS_STYLE} stroke="transparent" axisLine={false} tickLine={false} />
              <YAxis tick={AXIS_STYLE} stroke="transparent" axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={TOOLTIP_LABEL_STYLE}
                itemStyle={TOOLTIP_ITEM_STYLE}
                cursor={{ fill: 'rgba(240, 100, 0, 0.15)' }}
              />
              <Legend wrapperStyle={{ fontSize: '12px', color: 'rgba(255,255,255,0.8)' }} />
              {yKeys.map((key, i) => (
                <Bar
                  key={key}
                  dataKey={key}
                  fill={getColor(i)}
                  radius={[8, 8, 0, 0]}
                  filter={`url(#glow-${i})`}
                  style={{ cursor: 'pointer' }}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        );
    }
  };

  return (
    <div className="mt-3 rounded-2xl border border-brand-orange/30 overflow-hidden bg-[#0a0f1a] shadow-[0_0_40px_rgba(240,100,0,0.2),inset_0_1px_0_rgba(255,255,255,0.05)]">
      <div className="flex items-center space-x-2 px-4 py-3 border-b border-brand-orange/20 bg-gradient-to-r from-brand-orange/15 to-transparent">
        <div className="p-1.5 rounded-lg bg-brand-orange/25 shadow-[0_0_15px_rgba(240,100,0,0.5)]">
          <ChartIcon className="h-4 w-4 text-brand-orange" style={{ filter: 'drop-shadow(0 0 6px rgba(240, 100, 0, 0.8))' }} />
        </div>
        <span className="text-sm font-semibold text-brand-orange/90 uppercase tracking-wide" style={{ textShadow: '0 0 10px rgba(240, 100, 0, 0.5)' }}>
          {chartConfig.title || 'Visualization'}
        </span>
      </div>
      <div className="px-3 py-4 bg-[#050810]">
        {renderChart()}
      </div>
    </div>
  );
}
