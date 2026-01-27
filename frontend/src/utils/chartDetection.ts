import type { ChartConfig } from '@/types';

/**
 * Auto-detect appropriate chart type from data shape.
 * Frontend fallback when backend doesn't provide chart_config.
 */
export function detectChartConfig(
  data: Record<string, unknown>[]
): ChartConfig | null {
  if (!data || data.length === 0) return null;

  const firstRow = data[0];
  const keys = Object.keys(firstRow);

  const numericKeys: string[] = [];
  const categoricalKeys: string[] = [];
  const dateKeys: string[] = [];

  for (const key of keys) {
    const sample = data.find(r => r[key] != null)?.[key];
    if (sample == null) {
      categoricalKeys.push(key);
      continue;
    }
    if (typeof sample === 'number') {
      numericKeys.push(key);
    } else if (typeof sample === 'string') {
      const lcKey = key.toLowerCase();
      if (['date', 'month', 'year', 'week', 'period', 'time'].some(p => lcKey.includes(p))) {
        dateKeys.push(key);
      } else {
        categoricalKeys.push(key);
      }
    } else {
      categoricalKeys.push(key);
    }
  }

  // Single scalar value — no chart needed
  if (data.length === 1 && numericKeys.length === 1 && categoricalKeys.length === 0) {
    return null;
  }

  // Date dimension + numerics → line chart
  if (dateKeys.length > 0 && numericKeys.length > 0) {
    return {
      chart_type: 'line',
      x_axis_key: dateKeys[0],
      y_axis_keys: numericKeys.slice(0, 3),
    };
  }

  // Few categories + 1 numeric → pie chart
  if (categoricalKeys.length > 0 && numericKeys.length === 1 && data.length <= 8) {
    return {
      chart_type: 'pie',
      x_axis_key: categoricalKeys[0],
      y_axis_keys: [numericKeys[0]],
    };
  }

  // Categorical + numerics → bar chart
  if (categoricalKeys.length > 0 && numericKeys.length > 0) {
    return {
      chart_type: 'bar',
      x_axis_key: categoricalKeys[0],
      y_axis_keys: numericKeys.slice(0, 4),
    };
  }

  // Numerics only, multiple rows → line chart
  if (numericKeys.length > 0 && data.length > 2) {
    return {
      chart_type: 'line',
      x_axis_key: keys[0],
      y_axis_keys: numericKeys.slice(0, 3),
    };
  }

  return null;
}
