import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { formatNumber, calculatePercentChange } from '@/lib/utils';
import type { WidgetConfig, KPIData } from '@/types';

interface KPICardProps {
  config: WidgetConfig;
  data?: KPIData;
  isLoading: boolean;
}

export default function KPICard({ config, data, isLoading }: KPICardProps) {
  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center h-full animate-pulse">
        <div className="h-8 w-24 bg-muted rounded"></div>
        <div className="h-4 w-16 bg-muted rounded mt-2"></div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        No data available
      </div>
    );
  }

  const value = data.value;
  const format = data.format || config.format || 'number';
  const formattedValue = formatNumber(value, format);

  // Calculate trend
  let trend: 'up' | 'down' | 'neutral' = 'neutral';
  let percentChange = 0;

  if (data.comparison_value !== undefined && data.comparison_value !== null) {
    percentChange = calculatePercentChange(value, data.comparison_value);
    trend = percentChange > 0 ? 'up' : percentChange < 0 ? 'down' : 'neutral';
  }

  return (
    <div className="flex flex-col items-center justify-center h-full">
      <span className="text-3xl font-bold">{formattedValue}</span>

      {config.label && (
        <span className="text-sm text-muted-foreground mt-1">{config.label}</span>
      )}

      {data.comparison_value !== undefined && (
        <div className={`flex items-center mt-2 text-sm ${
          trend === 'up' ? 'text-green-600' :
          trend === 'down' ? 'text-red-600' :
          'text-muted-foreground'
        }`}>
          {trend === 'up' && <TrendingUp className="h-4 w-4 mr-1" />}
          {trend === 'down' && <TrendingDown className="h-4 w-4 mr-1" />}
          {trend === 'neutral' && <Minus className="h-4 w-4 mr-1" />}
          <span>{percentChange >= 0 ? '+' : ''}{percentChange.toFixed(1)}%</span>
          <span className="text-muted-foreground ml-1">vs prev</span>
        </div>
      )}
    </div>
  );
}
