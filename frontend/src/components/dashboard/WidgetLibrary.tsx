import { X, BarChart3, LineChart, PieChart, Table, Gauge, AreaChart, Hash } from 'lucide-react';
import type { WidgetType } from '@/types';

interface WidgetLibraryProps {
  onSelect: (type: WidgetType) => void;
  onClose: () => void;
}

const widgets = [
  {
    type: 'kpi_card' as WidgetType,
    name: 'KPI Card',
    description: 'Display a single metric with optional comparison',
    icon: Hash,
  },
  {
    type: 'line_chart' as WidgetType,
    name: 'Line Chart',
    description: 'Show trends over time',
    icon: LineChart,
  },
  {
    type: 'area_chart' as WidgetType,
    name: 'Area Chart',
    description: 'Show trends with filled area',
    icon: AreaChart,
  },
  {
    type: 'bar_chart' as WidgetType,
    name: 'Bar Chart',
    description: 'Compare values across categories',
    icon: BarChart3,
  },
  {
    type: 'pie_chart' as WidgetType,
    name: 'Pie Chart',
    description: 'Show distribution of values',
    icon: PieChart,
  },
  {
    type: 'donut_chart' as WidgetType,
    name: 'Donut Chart',
    description: 'Distribution with center metric',
    icon: PieChart,
  },
  {
    type: 'table' as WidgetType,
    name: 'Data Table',
    description: 'Display tabular data',
    icon: Table,
  },
  {
    type: 'gauge' as WidgetType,
    name: 'Gauge',
    description: 'Show progress towards a goal',
    icon: Gauge,
  },
];

export default function WidgetLibrary({ onSelect, onClose }: WidgetLibraryProps) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="w-full max-w-2xl rounded-lg bg-card p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-semibold">Add Widget</h2>
          <button onClick={onClose} className="p-1 rounded hover:bg-muted">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          {widgets.map((widget) => {
            const Icon = widget.icon;
            return (
              <button
                key={widget.type}
                onClick={() => onSelect(widget.type)}
                className="flex items-start space-x-3 p-4 rounded-lg border hover:border-primary hover:bg-primary/5 transition-colors text-left"
              >
                <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary/10 shrink-0">
                  <Icon className="h-5 w-5 text-primary" />
                </div>
                <div>
                  <h3 className="font-medium">{widget.name}</h3>
                  <p className="text-sm text-muted-foreground">{widget.description}</p>
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
