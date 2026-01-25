import { useMemo } from 'react';
import GridLayout, { Layout } from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import { useDashboardStore } from '@/store/dashboardStore';
import WidgetRenderer from './WidgetRenderer';
import type { Widget, LayoutConfig } from '@/types';

interface DashboardGridProps {
  widgets: Widget[];
  layoutConfig: LayoutConfig;
  isEditing: boolean;
  onWidgetSelect?: (widgetId: number | null) => void;
  selectedWidgetId?: number | null;
}

export default function DashboardGrid({
  widgets,
  layoutConfig,
  isEditing,
  onWidgetSelect,
  selectedWidgetId,
}: DashboardGridProps) {
  const { updateWidgetPosition, widgetData, loadWidgetData } = useDashboardStore();

  const layout: Layout[] = useMemo(
    () =>
      widgets.map((widget) => ({
        i: widget.widget_id.toString(),
        x: widget.position.x,
        y: widget.position.y,
        w: widget.position.w,
        h: widget.position.h,
        minW: 2,
        minH: 2,
      })),
    [widgets]
  );

  const handleLayoutChange = (newLayout: Layout[]) => {
    if (!isEditing) return;

    newLayout.forEach((item) => {
      const widgetId = parseInt(item.i);
      const widget = widgets.find((w) => w.widget_id === widgetId);
      if (widget) {
        const hasChanged =
          widget.position.x !== item.x ||
          widget.position.y !== item.y ||
          widget.position.w !== item.w ||
          widget.position.h !== item.h;

        if (hasChanged) {
          updateWidgetPosition(widgetId, {
            x: item.x,
            y: item.y,
            w: item.w,
            h: item.h,
          });
        }
      }
    });
  };

  return (
    <GridLayout
      className="layout"
      layout={layout}
      cols={layoutConfig.cols || 12}
      rowHeight={layoutConfig.rowHeight || 100}
      width={1200}
      margin={layoutConfig.margin || [16, 16]}
      isDraggable={isEditing}
      isResizable={isEditing}
      onLayoutChange={handleLayoutChange}
      draggableHandle=".widget-drag-handle"
    >
      {widgets.map((widget) => (
        <div
          key={widget.widget_id.toString()}
          className={`rounded-lg border bg-card shadow-sm transition-shadow ${
            isEditing ? 'cursor-move' : ''
          } ${selectedWidgetId === widget.widget_id ? 'ring-2 ring-primary' : ''}`}
          onClick={() => isEditing && onWidgetSelect?.(widget.widget_id)}
        >
          <WidgetRenderer
            widget={widget}
            data={widgetData[widget.widget_id]}
            isEditing={isEditing}
            onRefresh={() => loadWidgetData(widget.widget_id)}
          />
        </div>
      ))}
    </GridLayout>
  );
}
