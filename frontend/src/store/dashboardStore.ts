import { create } from 'zustand';
import type { Dashboard, Widget, WidgetData } from '@/types';
import { api } from '@/services/api';

interface DashboardState {
  // Data
  dashboards: Dashboard[];
  currentDashboard: Dashboard | null;
  widgetData: Record<number, WidgetData>;
  isLoading: boolean;
  error: string | null;

  // Editing state
  isEditing: boolean;
  selectedWidgetId: number | null;
  pendingChanges: boolean;

  // Filters
  globalFilters: Record<string, unknown>;
  dateRange: { start: string; end: string } | null;

  // Actions
  loadDashboards: () => Promise<void>;
  loadDashboard: (id: number) => Promise<void>;
  saveDashboard: () => Promise<void>;
  createDashboard: (name: string, description?: string) => Promise<Dashboard>;
  deleteDashboard: (id: number) => Promise<void>;
  cloneDashboard: (id: number, name: string) => Promise<Dashboard>;

  // Dashboard editing
  setCurrentDashboard: (dashboard: Dashboard | null) => void;
  updateDashboardName: (name: string) => void;
  updateDashboardDescription: (description: string) => void;
  toggleShared: () => void;
  setEditing: (editing: boolean) => void;

  // Widget management
  addWidget: (widget: Omit<Widget, 'widget_id'>) => void;
  updateWidget: (widgetId: number, updates: Partial<Widget>) => void;
  removeWidget: (widgetId: number) => void;
  selectWidget: (widgetId: number | null) => void;
  updateWidgetPosition: (widgetId: number, position: Widget['position']) => void;

  // Widget data
  loadWidgetData: (widgetId: number) => Promise<void>;
  refreshAllWidgets: () => Promise<void>;

  // Filters
  setGlobalFilter: (key: string, value: unknown) => void;
  clearGlobalFilters: () => void;
  setDateRange: (range: { start: string; end: string } | null) => void;

  // Error handling
  setError: (error: string | null) => void;
}

let nextTempWidgetId = -1;

export const useDashboardStore = create<DashboardState>((set, get) => ({
  dashboards: [],
  currentDashboard: null,
  widgetData: {},
  isLoading: false,
  error: null,
  isEditing: false,
  selectedWidgetId: null,
  pendingChanges: false,
  globalFilters: {},
  dateRange: null,

  loadDashboards: async () => {
    set({ isLoading: true, error: null });
    try {
      const response = await api.getDashboards({ page_size: 100 });
      set({ dashboards: response.items, isLoading: false });
    } catch (error) {
      set({ error: 'Failed to load dashboards', isLoading: false });
    }
  },

  loadDashboard: async (id) => {
    set({ isLoading: true, error: null });
    try {
      const dashboard = await api.getDashboard(id);
      set({ currentDashboard: dashboard, isLoading: false, pendingChanges: false });
      // Load widget data
      get().refreshAllWidgets();
    } catch (error) {
      set({ error: 'Failed to load dashboard', isLoading: false });
    }
  },

  saveDashboard: async () => {
    const { currentDashboard } = get();
    if (!currentDashboard) return;

    set({ isLoading: true, error: null });
    try {
      // Update dashboard metadata
      await api.updateDashboard(currentDashboard.dashboard_id, {
        name: currentDashboard.name,
        description: currentDashboard.description,
        layout_config: currentDashboard.layout_config,
        is_shared: currentDashboard.is_shared,
      });

      // Sync widgets
      for (const widget of currentDashboard.widgets) {
        if (widget.widget_id < 0) {
          // New widget
          const newWidget = await api.addWidget(currentDashboard.dashboard_id, {
            widget_type: widget.widget_type,
            title: widget.title,
            position: widget.position,
            config: widget.config,
            refresh_interval: widget.refresh_interval,
          });
          widget.widget_id = newWidget.widget_id;
        } else {
          // Update existing
          await api.updateWidget(currentDashboard.dashboard_id, widget.widget_id, {
            title: widget.title,
            position: widget.position,
            config: widget.config,
            refresh_interval: widget.refresh_interval,
          });
        }
      }

      set({ pendingChanges: false, isLoading: false });
    } catch (error) {
      set({ error: 'Failed to save dashboard', isLoading: false });
    }
  },

  createDashboard: async (name, description) => {
    const dashboard = await api.createDashboard({ name, description });
    set((state) => ({ dashboards: [...state.dashboards, dashboard] }));
    return dashboard;
  },

  deleteDashboard: async (id) => {
    await api.deleteDashboard(id);
    set((state) => ({
      dashboards: state.dashboards.filter((d) => d.dashboard_id !== id),
      currentDashboard: state.currentDashboard?.dashboard_id === id ? null : state.currentDashboard,
    }));
  },

  cloneDashboard: async (id, name) => {
    const dashboard = await api.cloneDashboard(id, name);
    set((state) => ({ dashboards: [...state.dashboards, dashboard] }));
    return dashboard;
  },

  setCurrentDashboard: (dashboard) => set({ currentDashboard: dashboard, pendingChanges: false }),

  updateDashboardName: (name) =>
    set((state) => ({
      currentDashboard: state.currentDashboard
        ? { ...state.currentDashboard, name }
        : null,
      pendingChanges: true,
    })),

  updateDashboardDescription: (description) =>
    set((state) => ({
      currentDashboard: state.currentDashboard
        ? { ...state.currentDashboard, description }
        : null,
      pendingChanges: true,
    })),

  toggleShared: () =>
    set((state) => ({
      currentDashboard: state.currentDashboard
        ? { ...state.currentDashboard, is_shared: !state.currentDashboard.is_shared }
        : null,
      pendingChanges: true,
    })),

  setEditing: (editing) => set({ isEditing: editing }),

  addWidget: (widget) => {
    const tempId = nextTempWidgetId--;
    set((state) => ({
      currentDashboard: state.currentDashboard
        ? {
            ...state.currentDashboard,
            widgets: [...state.currentDashboard.widgets, { ...widget, widget_id: tempId }],
          }
        : null,
      pendingChanges: true,
    }));
  },

  updateWidget: (widgetId, updates) =>
    set((state) => ({
      currentDashboard: state.currentDashboard
        ? {
            ...state.currentDashboard,
            widgets: state.currentDashboard.widgets.map((w) =>
              w.widget_id === widgetId ? { ...w, ...updates } : w
            ),
          }
        : null,
      pendingChanges: true,
    })),

  removeWidget: (widgetId) =>
    set((state) => ({
      currentDashboard: state.currentDashboard
        ? {
            ...state.currentDashboard,
            widgets: state.currentDashboard.widgets.filter((w) => w.widget_id !== widgetId),
          }
        : null,
      selectedWidgetId: state.selectedWidgetId === widgetId ? null : state.selectedWidgetId,
      pendingChanges: true,
    })),

  selectWidget: (widgetId) => set({ selectedWidgetId: widgetId }),

  updateWidgetPosition: (widgetId, position) =>
    set((state) => ({
      currentDashboard: state.currentDashboard
        ? {
            ...state.currentDashboard,
            widgets: state.currentDashboard.widgets.map((w) =>
              w.widget_id === widgetId ? { ...w, position } : w
            ),
          }
        : null,
      pendingChanges: true,
    })),

  loadWidgetData: async (widgetId) => {
    const { currentDashboard, globalFilters, dateRange } = get();
    if (!currentDashboard) return;

    try {
      const data = await api.getWidgetData(currentDashboard.dashboard_id, widgetId, {
        filters: globalFilters,
        date_range: dateRange || undefined,
      });
      set((state) => ({
        widgetData: { ...state.widgetData, [widgetId]: data },
      }));
    } catch (error) {
      console.error(`Failed to load widget ${widgetId} data:`, error);
    }
  },

  refreshAllWidgets: async () => {
    const { currentDashboard, loadWidgetData } = get();
    if (!currentDashboard) return;

    await Promise.all(
      currentDashboard.widgets.map((w) => loadWidgetData(w.widget_id))
    );
  },

  setGlobalFilter: (key, value) =>
    set((state) => ({
      globalFilters: { ...state.globalFilters, [key]: value },
    })),

  clearGlobalFilters: () => set({ globalFilters: {} }),

  setDateRange: (range) => set({ dateRange: range }),

  setError: (error) => set({ error }),
}));
