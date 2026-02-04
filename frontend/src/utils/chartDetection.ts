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

  const idPatterns = ['_id', 'period', 'year', 'month', 'week', 'code', 'number'];
  const isIdKey = (k: string) => idPatterns.some(p => k.toLowerCase().includes(p));

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
      if (isIdKey(key)) {
        categoricalKeys.push(key);
      } else {
        numericKeys.push(key);
      }
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
    const yKeys = numericKeys.filter(k => k !== dateKeys[0]).slice(0, 3);
    if (yKeys.length > 0) {
      return {
        chart_type: 'line',
        x_axis_key: dateKeys[0],
        y_axis_keys: yKeys,
      };
    }
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
    const xKey = categoricalKeys[0];
    const yKeys = numericKeys.filter(k => k !== xKey).slice(0, 4);
    if (yKeys.length > 0) {
      return {
        chart_type: 'bar',
        x_axis_key: xKey,
        y_axis_keys: yKeys,
      };
    }
  }

  // Numerics only, multiple rows → line chart
  if (numericKeys.length > 0 && data.length > 2) {
    const xKey = keys[0];
    const yKeys = numericKeys.filter(k => k !== xKey).slice(0, 3);
    if (yKeys.length > 0) {
      return {
        chart_type: 'line',
        x_axis_key: xKey,
        y_axis_keys: yKeys,
      };
    }
  }

  return null;
}
