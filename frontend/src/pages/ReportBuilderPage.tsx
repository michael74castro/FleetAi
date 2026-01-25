import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Save, X, Plus, Trash2, GripVertical, Play } from 'lucide-react';
import { useReportStore } from '@/store/reportStore';
import { api } from '@/services/api';
import type { Dataset, DatasetColumn, ColumnConfig, FilterConfig, SortConfig } from '@/types';

export default function ReportBuilderPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const {
    currentReport,
    isLoading,
    pendingChanges,
    loadReport,
    saveReport,
    createReport,
    setCurrentReport,
    addColumn,
    updateColumn,
    removeColumn,
    addFilter,
    updateFilter,
    removeFilter,
    setSorting,
    executeReport,
    executionResult,
    isExecuting,
  } = useReportStore();

  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [columns, setColumns] = useState<DatasetColumn[]>([]);
  const [reportName, setReportName] = useState('');
  const [reportDescription, setReportDescription] = useState('');
  const [selectedDataset, setSelectedDataset] = useState('');
  const [showPreview, setShowPreview] = useState(false);

  const isNewReport = !id || id === 'new';

  useEffect(() => {
    loadDatasets();
    if (!isNewReport) {
      loadReport(parseInt(id));
    }
  }, [id, isNewReport, loadReport]);

  useEffect(() => {
    if (currentReport) {
      setReportName(currentReport.name);
      setReportDescription(currentReport.description || '');
      setSelectedDataset(currentReport.dataset_name);
    }
  }, [currentReport]);

  useEffect(() => {
    if (selectedDataset) {
      loadColumns(selectedDataset);
    }
  }, [selectedDataset]);

  const loadDatasets = async () => {
    try {
      const response = await api.getDatasets();
      setDatasets(response.items || []);
    } catch (error) {
      console.error('Failed to load datasets:', error);
    }
  };

  const loadColumns = async (datasetName: string) => {
    try {
      const schema = await api.getDatasetSchema(datasetName);
      setColumns(schema.columns || []);
    } catch (error) {
      console.error('Failed to load schema:', error);
    }
  };

  const handleSave = async () => {
    if (isNewReport) {
      const report = await createReport(reportName, selectedDataset, reportDescription);
      navigate(`/reports/${report.report_id}`);
    } else {
      await saveReport();
      navigate(`/reports/${id}`);
    }
  };

  const handleCancel = () => {
    if (pendingChanges && !confirm('You have unsaved changes. Are you sure you want to leave?')) {
      return;
    }
    navigate(isNewReport ? '/reports' : `/reports/${id}`);
  };

  const handleAddColumn = () => {
    if (columns.length === 0) return;
    const availableColumns = columns.filter(
      (c) => !currentReport?.config.columns.some((rc) => rc.field === c.name)
    );
    if (availableColumns.length > 0) {
      addColumn({ field: availableColumns[0].name, label: availableColumns[0].name });
    }
  };

  const handleAddFilter = () => {
    if (columns.length === 0) return;
    addFilter({ field: columns[0].name, op: '=', value: '' });
  };

  const handlePreview = () => {
    setShowPreview(true);
    executeReport(1);
  };

  // Initialize new report
  useEffect(() => {
    if (isNewReport && !currentReport) {
      setCurrentReport({
        report_id: 0,
        name: 'New Report',
        description: '',
        dataset_name: '',
        config: {
          columns: [],
          filters: [],
          sorting: [],
        },
        is_shared: false,
        created_by: 0,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      });
    }
  }, [isNewReport, currentReport, setCurrentReport]);

  if (isLoading && !currentReport) {
    return (
      <div className="flex h-64 items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
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
            value={reportName}
            onChange={(e) => setReportName(e.target.value)}
            className="border-none bg-transparent text-lg font-semibold focus:outline-none focus:ring-0"
            placeholder="Report name"
          />
          {pendingChanges && (
            <span className="text-xs text-muted-foreground">(unsaved changes)</span>
          )}
        </div>

        <div className="flex items-center space-x-2">
          <button
            onClick={handlePreview}
            disabled={isExecuting || !selectedDataset}
            className="flex items-center space-x-2 rounded-md border px-3 py-1.5 text-sm hover:bg-muted disabled:opacity-50"
          >
            <Play className="h-4 w-4" />
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
            disabled={isLoading || !reportName.trim() || !selectedDataset}
            className="flex items-center space-x-2 rounded-md bg-primary px-4 py-1.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            <Save className="h-4 w-4" />
            <span>Save</span>
          </button>
        </div>
      </div>

      {/* Main content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Config panel */}
        <div className="w-80 border-r bg-card p-4 overflow-y-auto">
          <div className="space-y-6">
            {/* Dataset */}
            <div>
              <label className="text-sm font-medium">Dataset</label>
              <select
                value={selectedDataset}
                onChange={(e) => {
                  setSelectedDataset(e.target.value);
                  if (currentReport) {
                    setCurrentReport({
                      ...currentReport,
                      dataset_name: e.target.value,
                      config: { ...currentReport.config, columns: [], filters: [] },
                    });
                  }
                }}
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

            {/* Description */}
            <div>
              <label className="text-sm font-medium">Description</label>
              <textarea
                value={reportDescription}
                onChange={(e) => setReportDescription(e.target.value)}
                rows={2}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                placeholder="Report description"
              />
            </div>

            {/* Columns */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="text-sm font-medium">Columns</label>
                <button
                  onClick={handleAddColumn}
                  disabled={!selectedDataset}
                  className="text-xs text-primary hover:underline disabled:opacity-50"
                >
                  <Plus className="h-3 w-3 inline mr-1" />
                  Add
                </button>
              </div>
              <div className="space-y-2">
                {currentReport?.config.columns.map((col, index) => (
                  <div key={index} className="flex items-center space-x-2 p-2 rounded border bg-background">
                    <GripVertical className="h-4 w-4 text-muted-foreground cursor-grab" />
                    <select
                      value={col.field}
                      onChange={(e) => updateColumn(index, { ...col, field: e.target.value })}
                      className="flex-1 rounded border border-input bg-background px-2 py-1 text-sm"
                    >
                      {columns.map((c) => (
                        <option key={c.name} value={c.name}>
                          {c.name}
                        </option>
                      ))}
                    </select>
                    <button
                      onClick={() => removeColumn(index)}
                      className="p-1 text-muted-foreground hover:text-destructive"
                    >
                      <Trash2 className="h-3 w-3" />
                    </button>
                  </div>
                ))}
                {(!currentReport?.config.columns || currentReport.config.columns.length === 0) && (
                  <p className="text-xs text-muted-foreground">No columns added</p>
                )}
              </div>
            </div>

            {/* Filters */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="text-sm font-medium">Filters</label>
                <button
                  onClick={handleAddFilter}
                  disabled={!selectedDataset}
                  className="text-xs text-primary hover:underline disabled:opacity-50"
                >
                  <Plus className="h-3 w-3 inline mr-1" />
                  Add
                </button>
              </div>
              <div className="space-y-2">
                {currentReport?.config.filters.map((filter, index) => (
                  <div key={index} className="p-2 rounded border bg-background space-y-2">
                    <div className="flex items-center space-x-2">
                      <select
                        value={filter.field}
                        onChange={(e) => updateFilter(index, { ...filter, field: e.target.value })}
                        className="flex-1 rounded border border-input bg-background px-2 py-1 text-sm"
                      >
                        {columns.map((c) => (
                          <option key={c.name} value={c.name}>
                            {c.name}
                          </option>
                        ))}
                      </select>
                      <button
                        onClick={() => removeFilter(index)}
                        className="p-1 text-muted-foreground hover:text-destructive"
                      >
                        <Trash2 className="h-3 w-3" />
                      </button>
                    </div>
                    <div className="flex items-center space-x-2">
                      <select
                        value={filter.op}
                        onChange={(e) => updateFilter(index, { ...filter, op: e.target.value })}
                        className="w-20 rounded border border-input bg-background px-2 py-1 text-sm"
                      >
                        <option value="=">=</option>
                        <option value="!=">!=</option>
                        <option value=">">&gt;</option>
                        <option value="<">&lt;</option>
                        <option value=">=">&gt;=</option>
                        <option value="<=">&lt;=</option>
                        <option value="LIKE">Contains</option>
                        <option value="IN">In</option>
                      </select>
                      <input
                        type="text"
                        value={String(filter.value || '')}
                        onChange={(e) => updateFilter(index, { ...filter, value: e.target.value })}
                        className="flex-1 rounded border border-input bg-background px-2 py-1 text-sm"
                        placeholder="Value"
                      />
                    </div>
                  </div>
                ))}
                {(!currentReport?.config.filters || currentReport.config.filters.length === 0) && (
                  <p className="text-xs text-muted-foreground">No filters added</p>
                )}
              </div>
            </div>

            {/* Sorting */}
            <div>
              <label className="text-sm font-medium">Sort By</label>
              <div className="mt-1 flex space-x-2">
                <select
                  value={currentReport?.config.sorting?.[0]?.field || ''}
                  onChange={(e) => {
                    if (e.target.value) {
                      setSorting([{ field: e.target.value, direction: currentReport?.config.sorting?.[0]?.direction || 'asc' }]);
                    } else {
                      setSorting([]);
                    }
                  }}
                  className="flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm"
                >
                  <option value="">None</option>
                  {columns.map((c) => (
                    <option key={c.name} value={c.name}>
                      {c.name}
                    </option>
                  ))}
                </select>
                <select
                  value={currentReport?.config.sorting?.[0]?.direction || 'asc'}
                  onChange={(e) => {
                    const field = currentReport?.config.sorting?.[0]?.field;
                    if (field) {
                      setSorting([{ field, direction: e.target.value as 'asc' | 'desc' }]);
                    }
                  }}
                  disabled={!currentReport?.config.sorting?.[0]?.field}
                  className="w-24 rounded-md border border-input bg-background px-3 py-2 text-sm"
                >
                  <option value="asc">Asc</option>
                  <option value="desc">Desc</option>
                </select>
              </div>
            </div>
          </div>
        </div>

        {/* Preview area */}
        <div className="flex-1 overflow-auto p-4 bg-muted/30">
          {showPreview ? (
            isExecuting ? (
              <div className="flex items-center justify-center h-full">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
              </div>
            ) : executionResult && executionResult.data.length > 0 ? (
              <div className="rounded-lg border bg-card overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="border-b bg-muted/50">
                      <tr>
                        {executionResult.columns.map((col) => (
                          <th
                            key={col.field}
                            className="px-4 py-3 text-left font-medium whitespace-nowrap"
                          >
                            {col.label || col.field}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y">
                      {executionResult.data.slice(0, 20).map((row, rowIndex) => (
                        <tr key={rowIndex} className="hover:bg-muted/50">
                          {executionResult.columns.map((col) => (
                            <td key={col.field} className="px-4 py-3 whitespace-nowrap">
                              {String(row[col.field] ?? '-')}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {executionResult.total_rows > 20 && (
                  <div className="px-4 py-2 border-t text-sm text-muted-foreground">
                    Showing first 20 of {executionResult.total_rows} rows
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground">
                No data available
              </div>
            )
          ) : (
            <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
              <p>Select a dataset and add columns to configure your report</p>
              <button
                onClick={handlePreview}
                disabled={!selectedDataset || !currentReport?.config.columns.length}
                className="mt-4 flex items-center space-x-2 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                <Play className="h-4 w-4" />
                <span>Preview Report</span>
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
