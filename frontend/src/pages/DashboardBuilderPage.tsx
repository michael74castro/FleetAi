import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Save, X, Plus, Eye, Settings } from 'lucide-react';
import { useDashboardStore } from '@/store/dashboardStore';
import DashboardGrid from '@/components/dashboard/DashboardGrid';
import WidgetLibrary from '@/components/dashboard/WidgetLibrary';
import WidgetConfigPanel from '@/components/dashboard/WidgetConfigPanel';
import type { Widget, WidgetType } from '@/types';

export default function DashboardBuilderPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const {
    currentDashboard,
    isLoading,
    pendingChanges,
    selectedWidgetId,
    loadDashboard,
    saveDashboard,
    createDashboard,
    setCurrentDashboard,
    setEditing,
    addWidget,
    selectWidget,
  } = useDashboardStore();

  const [showWidgetLibrary, setShowWidgetLibrary] = useState(false);
  const [dashboardName, setDashboardName] = useState('');
  const [dashboardDescription, setDashboardDescription] = useState('');
  const [showSettings, setShowSettings] = useState(false);

  const isNewDashboard = !id || id === 'new';

  useEffect(() => {
    if (!isNewDashboard) {
      loadDashboard(parseInt(id));
    } else {
      // New dashboard
      setCurrentDashboard({
        dashboard_id: 0,
        name: 'New Dashboard',
        description: '',
        layout_config: { cols: 12, rowHeight: 100 },
        is_default: false,
        is_shared: false,
        created_by: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        widgets: [],
      });
    }
    setEditing(true);

    return () => {
      setEditing(false);
    };
  }, [id, isNewDashboard, loadDashboard, setCurrentDashboard, setEditing]);

  useEffect(() => {
    if (currentDashboard) {
      setDashboardName(currentDashboard.name);
      setDashboardDescription(currentDashboard.description || '');
    }
  }, [currentDashboard]);

  const handleSave = async () => {
    if (isNewDashboard) {
      const dashboard = await createDashboard(dashboardName, dashboardDescription);
      // Add widgets to the new dashboard
      if (currentDashboard) {
        for (const widget of currentDashboard.widgets) {
          // Widgets will be saved via the dashboard update
        }
      }
      navigate(`/dashboards/${dashboard.dashboard_id}`);
    } else {
      await saveDashboard();
      navigate(`/dashboards/${id}`);
    }
  };

  const handleCancel = () => {
    if (pendingChanges && !confirm('You have unsaved changes. Are you sure you want to leave?')) {
      return;
    }
    navigate(isNewDashboard ? '/dashboards' : `/dashboards/${id}`);
  };

  const handleAddWidget = (widgetType: WidgetType) => {
    const newWidget: Omit<Widget, 'widget_id'> = {
      widget_type: widgetType,
      title: `New ${widgetType.replace('_', ' ')}`,
      position: { x: 0, y: Infinity, w: 4, h: 3 },
      config: {},
    };
    addWidget(newWidget);
    setShowWidgetLibrary(false);
  };

  const selectedWidget = currentDashboard?.widgets.find((w) => w.widget_id === selectedWidgetId);

  if (isLoading && !currentDashboard) {
    return (
      <div className="flex h-64 items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
      </div>
    );
  }

  if (!currentDashboard) {
    return (
      <div className="flex h-64 items-center justify-center">
        <p className="text-muted-foreground">Dashboard not found</p>
      </div>
    );
  }

  return (
    <div className="flex h-[calc(100vh-7rem)] flex-col">
      {/* Toolbar */}
      <div className="flex items-center justify-between border-b bg-card px-4 py-2">
        <div className="flex items-center space-x-4">
          <input
            type="text"
            value={dashboardName}
            onChange={(e) => setDashboardName(e.target.value)}
            className="border-none bg-transparent text-lg font-semibold focus:outline-none focus:ring-0"
            placeholder="Dashboard name"
          />
          {pendingChanges && (
            <span className="text-xs text-muted-foreground">(unsaved changes)</span>
          )}
        </div>

        <div className="flex items-center space-x-2">
          <button
            onClick={() => setShowWidgetLibrary(true)}
            className="flex items-center space-x-2 rounded-md border px-3 py-1.5 text-sm hover:bg-muted"
          >
            <Plus className="h-4 w-4" />
            <span>Add Widget</span>
          </button>
          <button
            onClick={() => setShowSettings(true)}
            className="flex items-center space-x-2 rounded-md border px-3 py-1.5 text-sm hover:bg-muted"
          >
            <Settings className="h-4 w-4" />
          </button>
          <button
            onClick={() => navigate(`/dashboards/${id}`)}
            disabled={isNewDashboard}
            className="flex items-center space-x-2 rounded-md border px-3 py-1.5 text-sm hover:bg-muted disabled:opacity-50"
          >
            <Eye className="h-4 w-4" />
            <span>Preview</span>
          </button>
          <button
            onClick={handleCancel}
            className="flex items-center space-x-2 rounded-md border px-3 py-1.5 text-sm hover:bg-muted"
          >
            <X className="h-4 w-4" />
            <span>Cancel</span>
          </button>
          <button
            onClick={handleSave}
            disabled={isLoading || !dashboardName.trim()}
            className="flex items-center space-x-2 rounded-md bg-primary px-4 py-1.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            <Save className="h-4 w-4" />
            <span>Save</span>
          </button>
        </div>
      </div>

      {/* Main content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Dashboard canvas */}
        <div className="flex-1 overflow-auto p-4 bg-muted/30">
          {currentDashboard.widgets.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full">
              <p className="text-muted-foreground mb-4">No widgets yet. Add widgets to get started.</p>
              <button
                onClick={() => setShowWidgetLibrary(true)}
                className="flex items-center space-x-2 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              >
                <Plus className="h-4 w-4" />
                <span>Add Widget</span>
              </button>
            </div>
          ) : (
            <DashboardGrid
              widgets={currentDashboard.widgets}
              layoutConfig={currentDashboard.layout_config}
              isEditing={true}
              onWidgetSelect={selectWidget}
              selectedWidgetId={selectedWidgetId}
            />
          )}
        </div>

        {/* Config panel */}
        {selectedWidget && (
          <WidgetConfigPanel
            widget={selectedWidget}
            onClose={() => selectWidget(null)}
          />
        )}
      </div>

      {/* Widget Library Modal */}
      {showWidgetLibrary && (
        <WidgetLibrary
          onSelect={handleAddWidget}
          onClose={() => setShowWidgetLibrary(false)}
        />
      )}

      {/* Settings Modal */}
      {showSettings && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="w-full max-w-md rounded-lg bg-card p-6">
            <h2 className="text-lg font-semibold">Dashboard Settings</h2>
            <div className="mt-4 space-y-4">
              <div>
                <label className="text-sm font-medium">Name</label>
                <input
                  type="text"
                  value={dashboardName}
                  onChange={(e) => setDashboardName(e.target.value)}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </div>
              <div>
                <label className="text-sm font-medium">Description</label>
                <textarea
                  value={dashboardDescription}
                  onChange={(e) => setDashboardDescription(e.target.value)}
                  rows={3}
                  className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                />
              </div>
              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id="shared"
                  checked={currentDashboard.is_shared}
                  onChange={() => {
                    setCurrentDashboard({
                      ...currentDashboard,
                      is_shared: !currentDashboard.is_shared,
                    });
                  }}
                  className="rounded border-input"
                />
                <label htmlFor="shared" className="text-sm">Share with others</label>
              </div>
            </div>
            <div className="mt-6 flex justify-end">
              <button
                onClick={() => setShowSettings(false)}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              >
                Done
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
