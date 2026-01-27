import { useMemo } from 'react';
import {
  BarChart, Bar,
  LineChart, Line,
  AreaChart, Area,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts';
import { BarChart3, TrendingUp, PieChart as PieIcon } from 'lucide-react';
import type { ChartConfig } from '@/types';

const CHART_COLORS = [
  '#F06400',
  '#00B1AF',
  '#ED8B00',
  '#FF7A1A',
  '#00D4D1',
  '#009A17',
  '#A9C90E',
  '#F06400CC',
];

const TOOLTIP_STYLE: React.CSSProperties = {
  backgroundColor: 'rgba(0, 51, 68, 0.95)',
  border: '1px solid rgba(255, 255, 255, 0.15)',
  borderRadius: '12px',
  color: 'rgba(255, 255, 255, 0.9)',
  fontSize: '12px',
  padding: '8px 12px',
};

const AXIS_STYLE = {
  fontSize: 11,
  fill: 'rgba(255, 255, 255, 0.5)',
};

const GRID_STROKE = 'rgba(255, 255, 255, 0.08)';

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
    if (y_axis_keys && y_axis_keys.length > 0) return y_axis_keys;
    const keys = Object.keys(data[0] || {}).filter(k => k !== xKey);
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
          <ResponsiveContainer width="100%" height={260}>
            <PieChart>
              <Pie
                data={pieData}
                cx="50%"
                cy="50%"
                innerRadius={chart_type === 'donut' ? '45%' : 0}
                outerRadius="78%"
                paddingAngle={2}
                dataKey="value"
                nameKey="name"
                label={({ name, percent }) =>
                  `${name}: ${(percent * 100).toFixed(0)}%`
                }
                labelLine={false}
              >
                {pieData.map((_, i) => (
                  <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Legend
                wrapperStyle={{ fontSize: '11px', color: 'rgba(255,255,255,0.7)' }}
              />
            </PieChart>
          </ResponsiveContainer>
        );
      }

      case 'line':
        return (
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
              <XAxis dataKey={xKey} tick={AXIS_STYLE} stroke="transparent" />
              <YAxis tick={AXIS_STYLE} stroke="transparent" />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Legend wrapperStyle={{ fontSize: '11px', color: 'rgba(255,255,255,0.7)' }} />
              {yKeys.map((key, i) => (
                <Line
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={CHART_COLORS[i % CHART_COLORS.length]}
                  strokeWidth={2}
                  dot={{ r: 3, fill: CHART_COLORS[i % CHART_COLORS.length] }}
                  activeDot={{ r: 5 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        );

      case 'area':
        return (
          <ResponsiveContainer width="100%" height={260}>
            <AreaChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
              <XAxis dataKey={xKey} tick={AXIS_STYLE} stroke="transparent" />
              <YAxis tick={AXIS_STYLE} stroke="transparent" />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Legend wrapperStyle={{ fontSize: '11px', color: 'rgba(255,255,255,0.7)' }} />
              {yKeys.map((key, i) => (
                <Area
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={CHART_COLORS[i % CHART_COLORS.length]}
                  fill={CHART_COLORS[i % CHART_COLORS.length]}
                  fillOpacity={0.15}
                  strokeWidth={2}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        );

      case 'bar':
      default:
        return (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
              <XAxis dataKey={xKey} tick={AXIS_STYLE} stroke="transparent" />
              <YAxis tick={AXIS_STYLE} stroke="transparent" />
              <Tooltip contentStyle={TOOLTIP_STYLE} cursor={{ fill: 'rgba(255,255,255,0.05)' }} />
              <Legend wrapperStyle={{ fontSize: '11px', color: 'rgba(255,255,255,0.7)' }} />
              {yKeys.map((key, i) => (
                <Bar
                  key={key}
                  dataKey={key}
                  fill={CHART_COLORS[i % CHART_COLORS.length]}
                  radius={[4, 4, 0, 0]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        );
    }
  };

  return (
    <div className="mt-3 glass rounded-2xl border border-white/10 overflow-hidden">
      <div className="flex items-center space-x-2 px-4 py-2.5 border-b border-white/10">
        <ChartIcon className="h-3.5 w-3.5 text-brand-cyan" />
        <span className="text-xs font-medium text-white/60 uppercase tracking-wide">
          {chartConfig.title || 'Visualization'}
        </span>
      </div>
      <div className="px-2 py-3">
        {renderChart()}
      </div>
    </div>
  );
}
