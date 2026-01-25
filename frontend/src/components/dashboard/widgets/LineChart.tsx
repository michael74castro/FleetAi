import {
  LineChart as RechartsLineChart,
  AreaChart as RechartsAreaChart,
  Line,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import type { WidgetConfig, ChartData } from '@/types';

interface LineChartProps {
  config: WidgetConfig;
  data?: ChartData;
  isLoading: boolean;
  type?: 'line' | 'area';
}

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'];

export default function LineChart({ config, data, isLoading, type = 'line' }: LineChartProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
      </div>
    );
  }

  if (!data || !data.data || data.data.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        No data available
      </div>
    );
  }

  const dimensions = data.dimensions || config.dimensions || [];
  const xAxisKey = dimensions[0] || Object.keys(data.data[0])[0];

  // Get metric keys (all keys except dimension)
  const metricKeys = Object.keys(data.data[0]).filter((key) => key !== xAxisKey);

  const ChartComponent = type === 'area' ? RechartsAreaChart : RechartsLineChart;
  const DataComponent = type === 'area' ? Area : Line;

  return (
    <ResponsiveContainer width="100%" height="100%">
      <ChartComponent data={data.data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
        <XAxis
          dataKey={xAxisKey}
          tick={{ fontSize: 12 }}
          stroke="hsl(var(--muted-foreground))"
        />
        <YAxis
          tick={{ fontSize: 12 }}
          stroke="hsl(var(--muted-foreground))"
        />
        <Tooltip
          contentStyle={{
            backgroundColor: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: 'var(--radius)',
          }}
        />
        <Legend />
        {metricKeys.map((key, index) => (
          type === 'area' ? (
            <Area
              key={key}
              type="monotone"
              dataKey={key}
              stroke={COLORS[index % COLORS.length]}
              fill={COLORS[index % COLORS.length]}
              fillOpacity={0.3}
              strokeWidth={2}
            />
          ) : (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={COLORS[index % COLORS.length]}
              strokeWidth={2}
              dot={{ r: 3 }}
              activeDot={{ r: 5 }}
            />
          )
        ))}
      </ChartComponent>
    </ResponsiveContainer>
  );
}
