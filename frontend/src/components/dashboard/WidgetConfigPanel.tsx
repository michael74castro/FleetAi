import { useState, useEffect } from 'react';
import { X, Trash2 } from 'lucide-react';
import { useDashboardStore } from '@/store/dashboardStore';
import { api } from '@/services/api';
import type { Widget, Dataset, DatasetColumn } from '@/types';

interface WidgetConfigPanelProps {
  widget: Widget;
  onClose: () => void;
}

export default function WidgetConfigPanel({ widget, onClose }: WidgetConfigPanelProps) {
  const { updateWidget, removeWidget } = useDashboardStore();
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [columns, setColumns] = useState<DatasetColumn[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  const [title, setTitle] = useState(widget.title);
  const [dataset, setDataset] = useState(widget.config.dataset || '');
  const [metric, setMetric] = useState(widget.config.metric || '');
  const [dimension, setDimension] = useState(widget.config.dimension || widget.config.dimensions?.[0] || '');
  const [refreshInterval, setRefreshInterval] = useState(widget.refresh_interval || 0);

  useEffect(() => {
    loadDatasets();
  }, []);

  useEffect(() => {
    if (dataset) {
      loadColumns(dataset);
    }
  }, [dataset]);

  const loadDatasets = async () => {
    try {
      const response = await api.getDatasets();
      setDatasets(response.items || []);
    } catch (error) {
      console.error('Failed to load datasets:', error);
    }
  };

  const loadColumns = async (datasetName: string) => {
    setIsLoading(true);
    try {
      const schema = await api.getDatasetSchema(datasetName);
      setColumns(schema.columns || []);
    } catch (error) {
      console.error('Failed to load schema:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSave = () => {
    updateWidget(widget.widget_id, {
      title,
      refresh_interval: refreshInterval || undefined,
      config: {
        ...widget.config,
        dataset,
        metric: metric || undefined,
        dimension: dimension || undefined,
        dimensions: dimension ? [dimension] : undefined,
      },
    });
    onClose();
  };

  const handleDelete = () => {
    if (confirm('Are you sure you want to remove this widget?')) {
      removeWidget(widget.widget_id);
      onClose();
    }
  };

  const renderConfigFields = () => {
    switch (widget.widget_type) {
      case 'kpi_card':
        return (
          <>
            <div>
              <label className="text-sm font-medium">Metric</label>
              <select
                value={metric}
                onChange={(e) => setMetric(e.target.value)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              >
                <option value="">Select metric</option>
                <option value="COUNT(*)">Count</option>
                {columns.filter(c => ['int', 'decimal', 'float', 'money'].some(t => c.type.toLowerCase().includes(t))).map((col) => (
                  <option key={col.name} value={`SUM([${col.name}])`}>
                    Sum of {col.name}
                  </option>
                ))}
                {columns.filter(c => ['int', 'decimal', 'float', 'money'].some(t => c.type.toLowerCase().includes(t))).map((col) => (
                  <option key={`avg_${col.name}`} value={`AVG([${col.name}])`}>
                    Average of {col.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-sm font-medium">Label</label>
              <input
                type="text"
                value={widget.config.label || ''}
                onChange={(e) => updateWidget(widget.widget_id, {
                  config: { ...widget.config, label: e.target.value }
                })}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                placeholder="e.g., Total Vehicles"
              />
            </div>
          </>
        );

      case 'line_chart':
      case 'area_chart':
      case 'bar_chart':
        return (
          <>
            <div>
              <label className="text-sm font-medium">X-Axis (Dimension)</label>
              <select
                value={dimension}
                onChange={(e) => setDimension(e.target.value)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              >
                <option value="">Select dimension</option>
                {columns.map((col) => (
                  <option key={col.name} value={col.name}>
                    {col.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-sm font-medium">Y-Axis (Metric)</label>
              <select
                value={metric}
                onChange={(e) => setMetric(e.target.value)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              >
                <option value="">Select metric</option>
                <option value="COUNT(*)">Count</option>
                {columns.filter(c => ['int', 'decimal', 'float', 'money'].some(t => c.type.toLowerCase().includes(t))).map((col) => (
                  <option key={col.name} value={`SUM([${col.name}])`}>
                    Sum of {col.name}
                  </option>
                ))}
              </select>
            </div>
          </>
        );

      case 'pie_chart':
      case 'donut_chart':
        return (
          <>
            <div>
              <label className="text-sm font-medium">Category</label>
              <select
                value={dimension}
                onChange={(e) => setDimension(e.target.value)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              >
                <option value="">Select category</option>
                {columns.map((col) => (
                  <option key={col.name} value={col.name}>
                    {col.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-sm font-medium">Value</label>
              <select
                value={metric}
                onChange={(e) => setMetric(e.target.value)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              >
                <option value="">Select value</option>
                <option value="COUNT(*)">Count</option>
                {columns.filter(c => ['int', 'decimal', 'float', 'money'].some(t => c.type.toLowerCase().includes(t))).map((col) => (
                  <option key={col.name} value={`SUM([${col.name}])`}>
                    Sum of {col.name}
                  </option>
                ))}
              </select>
            </div>
          </>
        );

      case 'gauge':
        return (
          <>
            <div>
              <label className="text-sm font-medium">Metric</label>
              <select
                value={metric}
                onChange={(e) => setMetric(e.target.value)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              >
                <option value="">Select metric</option>
                <option value="COUNT(*)">Count</option>
                {columns.filter(c => ['int', 'decimal', 'float', 'money'].some(t => c.type.toLowerCase().includes(t))).map((col) => (
                  <option key={col.name} value={`SUM([${col.name}])`}>
                    Sum of {col.name}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-sm font-medium">Target Value</label>
              <input
                type="number"
                value={widget.config.target || 100}
                onChange={(e) => updateWidget(widget.widget_id, {
                  config: { ...widget.config, target: parseInt(e.target.value) }
                })}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              />
            </div>
          </>
        );

      default:
        return null;
    }
  };

  return (
    <div className="w-80 border-l bg-card p-4 overflow-y-auto">
      <div className="flex items-center justify-between mb-4">
        <h3 className="font-semibold">Configure Widget</h3>
        <button onClick={onClose} className="p-1 rounded hover:bg-muted">
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="space-y-4">
        {/* Title */}
        <div>
          <label className="text-sm font-medium">Title</label>
          <input
            type="text"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
          />
        </div>

        {/* Dataset */}
        <div>
          <label className="text-sm font-medium">Dataset</label>
          <select
            value={dataset}
            onChange={(e) => setDataset(e.target.value)}
            className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
          >
            <option value="">Select a dataset</option>
            {datasets.map((ds) => (
              <option key={ds.name} value={ds.name}>
                {ds.display_name || ds.name}
              </option>
            ))}
          </select>
        </div>

        {isLoading ? (
          <div className="flex justify-center py-4">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-primary"></div>
          </div>
        ) : (
          dataset && renderConfigFields()
        )}

        {/* Refresh Interval */}
        <div>
          <label className="text-sm font-medium">Auto-refresh (seconds)</label>
          <input
            type="number"
            value={refreshInterval}
            onChange={(e) => setRefreshInterval(parseInt(e.target.value) || 0)}
            min="0"
            step="30"
            className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
            placeholder="0 = disabled"
          />
        </div>

        {/* Actions */}
        <div className="pt-4 border-t space-y-2">
          <button
            onClick={handleSave}
            className="w-full rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
          >
            Apply Changes
          </button>
          <button
            onClick={handleDelete}
            className="w-full flex items-center justify-center space-x-2 rounded-md border border-destructive px-4 py-2 text-sm font-medium text-destructive hover:bg-destructive/10"
          >
            <Trash2 className="h-4 w-4" />
            <span>Remove Widget</span>
          </button>
        </div>
      </div>
    </div>
  );
}
