import { create } from 'zustand';
import type { Report, ReportExecution, ColumnConfig, FilterConfig, SortConfig, AggregationConfig } from '@/types';
import { api } from '@/services/api';

interface ReportExecutionResult {
  columns: ColumnConfig[];
  data: Record<string, unknown>[];
  total_rows: number;
  aggregations?: Record<string, unknown>;
}

interface ReportState {
  // Data
  reports: Report[];
  currentReport: Report | null;
  executionResult: ReportExecutionResult | null;
  executions: ReportExecution[];
  isLoading: boolean;
  isExecuting: boolean;
  error: string | null;

  // Editing state
  isEditing: boolean;
  pendingChanges: boolean;

  // Pagination
  currentPage: number;
  pageSize: number;

  // Runtime parameters
  parameters: Record<string, unknown>;

  // Actions
  loadReports: () => Promise<void>;
  loadReport: (id: number) => Promise<void>;
  saveReport: () => Promise<void>;
  createReport: (name: string, datasetName: string, description?: string) => Promise<Report>;
  deleteReport: (id: number) => Promise<void>;

  // Report editing
  setCurrentReport: (report: Report | null) => void;
  updateReportName: (name: string) => void;
  updateReportDescription: (description: string) => void;
  toggleShared: () => void;
  setEditing: (editing: boolean) => void;

  // Config editing
  addColumn: (column: ColumnConfig) => void;
  updateColumn: (index: number, column: ColumnConfig) => void;
  removeColumn: (index: number) => void;
  reorderColumns: (fromIndex: number, toIndex: number) => void;

  addFilter: (filter: FilterConfig) => void;
  updateFilter: (index: number, filter: FilterConfig) => void;
  removeFilter: (index: number) => void;

  setSorting: (sorting: SortConfig[]) => void;
  setGrouping: (grouping: string[]) => void;

  addAggregation: (agg: AggregationConfig) => void;
  removeAggregation: (index: number) => void;

  // Execution
  executeReport: (page?: number) => Promise<void>;
  exportReport: (format: 'excel' | 'pdf' | 'csv') => Promise<ReportExecution>;
  loadExecutions: () => Promise<void>;
  downloadExecution: (executionId: number) => Promise<void>;

  // Parameters
  setParameter: (key: string, value: unknown) => void;
  clearParameters: () => void;

  // Pagination
  setPage: (page: number) => void;
  setPageSize: (size: number) => void;

  // Error handling
  setError: (error: string | null) => void;
}

export const useReportStore = create<ReportState>((set, get) => ({
  reports: [],
  currentReport: null,
  executionResult: null,
  executions: [],
  isLoading: false,
  isExecuting: false,
  error: null,
  isEditing: false,
  pendingChanges: false,
  currentPage: 1,
  pageSize: 50,
  parameters: {},

  loadReports: async () => {
    set({ isLoading: true, error: null });
    try {
      const response = await api.getReports({ page_size: 100 });
      set({ reports: response.items, isLoading: false });
    } catch (error) {
      set({ error: 'Failed to load reports', isLoading: false });
    }
  },

  loadReport: async (id) => {
    set({ isLoading: true, error: null });
    try {
      const report = await api.getReport(id);
      set({
        currentReport: report,
        isLoading: false,
        pendingChanges: false,
        currentPage: 1,
        executionResult: null,
      });
    } catch (error) {
      set({ error: 'Failed to load report', isLoading: false });
    }
  },

  saveReport: async () => {
    const { currentReport } = get();
    if (!currentReport) return;

    set({ isLoading: true, error: null });
    try {
      await api.updateReport(currentReport.report_id, {
        name: currentReport.name,
        description: currentReport.description,
        config: currentReport.config,
        is_shared: currentReport.is_shared,
      });
      set({ pendingChanges: false, isLoading: false });
    } catch (error) {
      set({ error: 'Failed to save report', isLoading: false });
    }
  },

  createReport: async (name, datasetName, description) => {
    const report = await api.createReport({
      name,
      dataset_name: datasetName,
      description,
      config: {
        columns: [],
        filters: [],
        sorting: [],
      },
    });
    set((state) => ({ reports: [...state.reports, report] }));
    return report;
  },

  deleteReport: async (id) => {
    await api.deleteReport(id);
    set((state) => ({
      reports: state.reports.filter((r) => r.report_id !== id),
      currentReport: state.currentReport?.report_id === id ? null : state.currentReport,
    }));
  },

  setCurrentReport: (report) =>
    set({ currentReport: report, pendingChanges: false, executionResult: null }),

  updateReportName: (name) =>
    set((state) => ({
      currentReport: state.currentReport ? { ...state.currentReport, name } : null,
      pendingChanges: true,
    })),

  updateReportDescription: (description) =>
    set((state) => ({
      currentReport: state.currentReport ? { ...state.currentReport, description } : null,
      pendingChanges: true,
    })),

  toggleShared: () =>
    set((state) => ({
      currentReport: state.currentReport
        ? { ...state.currentReport, is_shared: !state.currentReport.is_shared }
        : null,
      pendingChanges: true,
    })),

  setEditing: (editing) => set({ isEditing: editing }),

  addColumn: (column) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              columns: [...state.currentReport.config.columns, column],
            },
          }
        : null,
      pendingChanges: true,
    })),

  updateColumn: (index, column) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              columns: state.currentReport.config.columns.map((c, i) =>
                i === index ? column : c
              ),
            },
          }
        : null,
      pendingChanges: true,
    })),

  removeColumn: (index) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              columns: state.currentReport.config.columns.filter((_, i) => i !== index),
            },
          }
        : null,
      pendingChanges: true,
    })),

  reorderColumns: (fromIndex, toIndex) =>
    set((state) => {
      if (!state.currentReport) return state;
      const columns = [...state.currentReport.config.columns];
      const [removed] = columns.splice(fromIndex, 1);
      columns.splice(toIndex, 0, removed);
      return {
        currentReport: {
          ...state.currentReport,
          config: { ...state.currentReport.config, columns },
        },
        pendingChanges: true,
      };
    }),

  addFilter: (filter) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              filters: [...state.currentReport.config.filters, filter],
            },
          }
        : null,
      pendingChanges: true,
    })),

  updateFilter: (index, filter) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              filters: state.currentReport.config.filters.map((f, i) =>
                i === index ? filter : f
              ),
            },
          }
        : null,
      pendingChanges: true,
    })),

  removeFilter: (index) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              filters: state.currentReport.config.filters.filter((_, i) => i !== index),
            },
          }
        : null,
      pendingChanges: true,
    })),

  setSorting: (sorting) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: { ...state.currentReport.config, sorting },
          }
        : null,
      pendingChanges: true,
    })),

  setGrouping: (grouping) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: { ...state.currentReport.config, grouping },
          }
        : null,
      pendingChanges: true,
    })),

  addAggregation: (agg) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              aggregations: [...(state.currentReport.config.aggregations || []), agg],
            },
          }
        : null,
      pendingChanges: true,
    })),

  removeAggregation: (index) =>
    set((state) => ({
      currentReport: state.currentReport
        ? {
            ...state.currentReport,
            config: {
              ...state.currentReport.config,
              aggregations: (state.currentReport.config.aggregations || []).filter(
                (_, i) => i !== index
              ),
            },
          }
        : null,
      pendingChanges: true,
    })),

  executeReport: async (page) => {
    const { currentReport, pageSize, parameters } = get();
    if (!currentReport) return;

    const targetPage = page ?? 1;
    set({ isExecuting: true, error: null, currentPage: targetPage });

    try {
      const result = await api.executeReport(currentReport.report_id, {
        parameters,
        page: targetPage,
        page_size: pageSize,
      });
      set({ executionResult: result, isExecuting: false });
    } catch (error) {
      set({ error: 'Failed to execute report', isExecuting: false });
    }
  },

  exportReport: async (format) => {
    const { currentReport, parameters } = get();
    if (!currentReport) throw new Error('No report selected');

    const execution = await api.exportReport(currentReport.report_id, format, parameters);
    set((state) => ({ executions: [execution, ...state.executions] }));
    return execution;
  },

  loadExecutions: async () => {
    // Executions are typically loaded with the report
  },

  downloadExecution: async (executionId) => {
    const { currentReport } = get();
    if (!currentReport) return;

    const blob = await api.downloadReportExecution(currentReport.report_id, executionId);
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `report_${currentReport.report_id}_${executionId}.xlsx`;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);
  },

  setParameter: (key, value) =>
    set((state) => ({
      parameters: { ...state.parameters, [key]: value },
    })),

  clearParameters: () => set({ parameters: {} }),

  setPage: (page) => {
    set({ currentPage: page });
    get().executeReport(page);
  },

  setPageSize: (size) => {
    set({ pageSize: size, currentPage: 1 });
    get().executeReport(1);
  },

  setError: (error) => set({ error }),
}));
