import type { WidgetConfig, GaugeData } from '@/types';
import { formatNumber } from '@/lib/utils';

interface GaugeWidgetProps {
  config: WidgetConfig;
  data?: GaugeData;
  isLoading: boolean;
}

export default function GaugeWidget({ config, data, isLoading }: GaugeWidgetProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
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

  const percentage = Math.min(data.percentage, 100);
  const thresholds = data.thresholds || config.thresholds || [];

  // Determine color based on thresholds
  let color = '#3b82f6'; // default blue
  for (const threshold of thresholds.sort((a, b) => a.value - b.value)) {
    if (percentage >= threshold.value) {
      color = threshold.color;
    }
  }

  // SVG gauge parameters
  const size = 120;
  const strokeWidth = 12;
  const radius = (size - strokeWidth) / 2;
  const circumference = radius * Math.PI; // Half circle
  const offset = circumference - (percentage / 100) * circumference;

  return (
    <div className="flex flex-col items-center justify-center h-full">
      <svg width={size} height={size / 2 + 20} className="overflow-visible">
        {/* Background arc */}
        <path
          d={`M ${strokeWidth / 2} ${size / 2} A ${radius} ${radius} 0 0 1 ${size - strokeWidth / 2} ${size / 2}`}
          fill="none"
          stroke="hsl(var(--muted))"
          strokeWidth={strokeWidth}
          strokeLinecap="round"
        />
        {/* Progress arc */}
        <path
          d={`M ${strokeWidth / 2} ${size / 2} A ${radius} ${radius} 0 0 1 ${size - strokeWidth / 2} ${size / 2}`}
          fill="none"
          stroke={color}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          style={{ transition: 'stroke-dashoffset 0.5s ease-in-out' }}
        />
        {/* Center text */}
        <text
          x={size / 2}
          y={size / 2 + 5}
          textAnchor="middle"
          className="text-2xl font-bold fill-current"
        >
          {percentage.toFixed(0)}%
        </text>
      </svg>

      <div className="text-center mt-2">
        <div className="text-sm text-muted-foreground">
          {formatNumber(data.value)} / {formatNumber(data.target)}
        </div>
        {config.label && (
          <div className="text-xs text-muted-foreground mt-1">{config.label}</div>
        )}
      </div>
    </div>
  );
}
