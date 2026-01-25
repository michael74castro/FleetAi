import { useEffect } from 'react';
import { RefreshCw, GripVertical, MoreVertical } from 'lucide-react';
import { useDashboardStore } from '@/store/dashboardStore';
import KPICard from './widgets/KPICard';
import LineChart from './widgets/LineChart';
import BarChart from './widgets/BarChart';
import PieChart from './widgets/PieChart';
import DataTable from './widgets/DataTable';
import GaugeWidget from './widgets/GaugeWidget';
import type { Widget, WidgetData } from '@/types';

interface WidgetRendererProps {
  widget: Widget;
  data?: WidgetData;
  isEditing: boolean;
  onRefresh: () => void;
}

export default function WidgetRenderer({
  widget,
  data,
  isEditing,
  onRefresh,
}: WidgetRendererProps) {
  const { removeWidget, selectWidget } = useDashboardStore();

  useEffect(() => {
    // Auto-refresh based on interval
    if (widget.refresh_interval && widget.refresh_interval > 0) {
      const interval = setInterval(onRefresh, widget.refresh_interval * 1000);
      return () => clearInterval(interval);
    }
  }, [widget.refresh_interval, onRefresh]);

  useEffect(() => {
    // Initial data load
    onRefresh();
  }, []);

  const renderWidget = () => {
    const isLoading = !data;

    switch (widget.widget_type) {
      case 'kpi_card':
        return <KPICard config={widget.config} data={data} isLoading={isLoading} />;
      case 'line_chart':
      case 'area_chart':
        return (
          <LineChart
            config={widget.config}
            data={data}
            isLoading={isLoading}
            type={widget.widget_type === 'area_chart' ? 'area' : 'line'}
          />
        );
      case 'bar_chart':
        return <BarChart config={widget.config} data={data} isLoading={isLoading} />;
      case 'pie_chart':
      case 'donut_chart':
        return (
          <PieChart
            config={widget.config}
            data={data}
            isLoading={isLoading}
            type={widget.widget_type === 'donut_chart' ? 'donut' : 'pie'}
          />
        );
      case 'table':
        return <DataTable config={widget.config} data={data} isLoading={isLoading} />;
      case 'gauge':
        return <GaugeWidget config={widget.config} data={data} isLoading={isLoading} />;
      default:
        return (
          <div className="flex items-center justify-center h-full text-muted-foreground">
            Unsupported widget type: {widget.widget_type}
          </div>
        );
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b">
        <div className="flex items-center space-x-2">
          {isEditing && (
            <div className="widget-drag-handle cursor-grab">
              <GripVertical className="h-4 w-4 text-muted-foreground" />
            </div>
          )}
          <h3 className="font-medium text-sm truncate">{widget.title}</h3>
        </div>

        <div className="flex items-center space-x-1">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onRefresh();
            }}
            className="p-1 rounded hover:bg-muted"
            title="Refresh"
          >
            <RefreshCw className="h-3 w-3 text-muted-foreground" />
          </button>

          {isEditing && (
            <div className="relative group">
              <button
                className="p-1 rounded hover:bg-muted"
                onClick={(e) => e.stopPropagation()}
              >
                <MoreVertical className="h-3 w-3 text-muted-foreground" />
              </button>
              <div className="absolute right-0 top-full mt-1 w-32 rounded-md border bg-card shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-10">
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    selectWidget(widget.widget_id);
                  }}
                  className="flex w-full items-center px-3 py-2 text-sm hover:bg-muted"
                >
                  Configure
                </button>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    removeWidget(widget.widget_id);
                  }}
                  className="flex w-full items-center px-3 py-2 text-sm text-destructive hover:bg-muted"
                >
                  Remove
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 p-4 overflow-hidden">{renderWidget()}</div>
    </div>
  );
}
