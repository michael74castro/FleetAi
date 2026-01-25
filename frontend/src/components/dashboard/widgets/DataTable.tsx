import type { WidgetConfig, TableData } from '@/types';

interface DataTableProps {
  config: WidgetConfig;
  data?: TableData;
  isLoading: boolean;
}

export default function DataTable({ config, data, isLoading }: DataTableProps) {
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

  const columns = data.columns || (config.columns?.map(c => typeof c === 'string' ? c : c.field) ?? Object.keys(data.data[0]));
  const columnConfigs = config.columns || [];

  const getColumnLabel = (col: string) => {
    const colConfig = columnConfigs.find(c => (typeof c === 'string' ? c : c.field) === col);
    return colConfig && typeof colConfig !== 'string' ? colConfig.label || col : col;
  };

  const formatValue = (value: unknown, col: string) => {
    if (value === null || value === undefined) return '-';

    const colConfig = columnConfigs.find(c => (typeof c === 'string' ? c : c.field) === col);
    const format = colConfig && typeof colConfig !== 'string' ? colConfig.format : undefined;

    if (format === 'currency' && typeof value === 'number') {
      return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value);
    }
    if (format === 'percent' && typeof value === 'number') {
      return `${(value * 100).toFixed(1)}%`;
    }
    if (format === 'date' && (typeof value === 'string' || value instanceof Date)) {
      return new Date(value).toLocaleDateString();
    }
    if (format === 'number' && typeof value === 'number') {
      return value.toLocaleString();
    }
    return String(value);
  };

  return (
    <div className="h-full overflow-auto">
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-card border-b">
          <tr>
            {columns.map((col) => (
              <th
                key={col}
                className="px-3 py-2 text-left font-medium text-muted-foreground whitespace-nowrap"
              >
                {getColumnLabel(col)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y">
          {data.data.map((row, rowIndex) => (
            <tr key={rowIndex} className="hover:bg-muted/50">
              {columns.map((col) => (
                <td key={col} className="px-3 py-2 whitespace-nowrap">
                  {formatValue(row[col], col)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
