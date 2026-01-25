import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { Edit, Download, RefreshCw, ChevronLeft, ChevronRight, FileSpreadsheet, FileText as FilePdf, FileDown } from 'lucide-react';
import { useReportStore } from '@/store/reportStore';
import { formatNumber } from '@/lib/utils';

export default function ReportViewPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const {
    currentReport,
    executionResult,
    isLoading,
    isExecuting,
    error,
    currentPage,
    pageSize,
    loadReport,
    executeReport,
    exportReport,
    setPage,
    setPageSize,
  } = useReportStore();

  const [exportFormat, setExportFormat] = useState<'excel' | 'pdf' | 'csv'>('excel');
  const [isExporting, setIsExporting] = useState(false);

  useEffect(() => {
    if (id) {
      loadReport(parseInt(id));
    }
  }, [id, loadReport]);

  useEffect(() => {
    if (currentReport && !executionResult && !isExecuting) {
      executeReport();
    }
  }, [currentReport]);

  const handleExport = async () => {
    setIsExporting(true);
    try {
      await exportReport(exportFormat);
      // Show success message
    } catch (error) {
      console.error('Export failed:', error);
    } finally {
      setIsExporting(false);
    }
  };

  const formatValue = (value: unknown, column: string) => {
    if (value === null || value === undefined) return '-';

    const colConfig = currentReport?.config.columns.find((c) => c.field === column);
    const format = colConfig?.format;

    if (format === 'currency' && typeof value === 'number') {
      return formatNumber(value, 'currency');
    }
    if (format === 'percent' && typeof value === 'number') {
      return formatNumber(value, 'percent');
    }
    if (format === 'date' && (typeof value === 'string' || value instanceof Date)) {
      return new Date(value).toLocaleDateString();
    }
    if (format === 'number' && typeof value === 'number') {
      return formatNumber(value);
    }
    return String(value);
  };

  if (isLoading && !currentReport) {
    return (
      <div className="flex h-64 items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-64">
        <p className="text-destructive">{error}</p>
        <Link to="/reports" className="mt-4 text-primary hover:underline">
          Back to reports
        </Link>
      </div>
    );
  }

  if (!currentReport) {
    return (
      <div className="flex flex-col items-center justify-center h-64">
        <p className="text-muted-foreground">Report not found</p>
        <Link to="/reports" className="mt-4 text-primary hover:underline">
          Back to reports
        </Link>
      </div>
    );
  }

  const totalPages = executionResult
    ? Math.ceil(executionResult.total_rows / pageSize)
    : 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">{currentReport.name}</h1>
          {currentReport.description && (
            <p className="text-muted-foreground">{currentReport.description}</p>
          )}
        </div>

        <div className="flex items-center space-x-2">
          <button
            onClick={() => executeReport()}
            disabled={isExecuting}
            className="flex items-center space-x-2 rounded-md border px-3 py-2 text-sm hover:bg-muted disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${isExecuting ? 'animate-spin' : ''}`} />
            <span>Refresh</span>
          </button>

          {/* Export dropdown */}
          <div className="flex items-center">
            <select
              value={exportFormat}
              onChange={(e) => setExportFormat(e.target.value as 'excel' | 'pdf' | 'csv')}
              className="rounded-l-md border border-r-0 border-input bg-background px-3 py-2 text-sm"
            >
              <option value="excel">Excel</option>
              <option value="csv">CSV</option>
              <option value="pdf">PDF</option>
            </select>
            <button
              onClick={handleExport}
              disabled={isExporting}
              className="flex items-center space-x-2 rounded-r-md border bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
            >
              {exportFormat === 'excel' ? <FileSpreadsheet className="h-4 w-4" /> :
               exportFormat === 'pdf' ? <FilePdf className="h-4 w-4" /> :
               <FileDown className="h-4 w-4" />}
              <span>{isExporting ? 'Exporting...' : 'Export'}</span>
            </button>
          </div>

          <button
            onClick={() => navigate(`/reports/${id}/edit`)}
            className="flex items-center space-x-2 rounded-md bg-secondary px-4 py-2 text-sm font-medium hover:bg-secondary/80"
          >
            <Edit className="h-4 w-4" />
            <span>Edit</span>
          </button>
        </div>
      </div>

      {/* Aggregations */}
      {executionResult?.aggregations && Object.keys(executionResult.aggregations).length > 0 && (
        <div className="flex flex-wrap gap-4">
          {Object.entries(executionResult.aggregations).map(([key, value]) => (
            <div key={key} className="rounded-lg border bg-card p-4">
              <p className="text-sm text-muted-foreground">{key}</p>
              <p className="text-2xl font-bold">
                {formatNumber(value as number, 'number')}
              </p>
            </div>
          ))}
        </div>
      )}

      {/* Data table */}
      <div className="rounded-lg border bg-card">
        {isExecuting ? (
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
          </div>
        ) : executionResult && executionResult.data.length > 0 ? (
          <>
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
                  {executionResult.data.map((row, rowIndex) => (
                    <tr key={rowIndex} className="hover:bg-muted/50">
                      {executionResult.columns.map((col) => (
                        <td key={col.field} className="px-4 py-3 whitespace-nowrap">
                          {formatValue(row[col.field], col.field)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            <div className="flex items-center justify-between border-t px-4 py-3">
              <div className="flex items-center space-x-2 text-sm text-muted-foreground">
                <span>
                  Showing {(currentPage - 1) * pageSize + 1} to{' '}
                  {Math.min(currentPage * pageSize, executionResult.total_rows)} of{' '}
                  {executionResult.total_rows} results
                </span>
                <select
                  value={pageSize}
                  onChange={(e) => setPageSize(parseInt(e.target.value))}
                  className="rounded-md border border-input bg-background px-2 py-1 text-sm"
                >
                  <option value="25">25</option>
                  <option value="50">50</option>
                  <option value="100">100</option>
                </select>
              </div>

              <div className="flex items-center space-x-2">
                <button
                  onClick={() => setPage(currentPage - 1)}
                  disabled={currentPage === 1}
                  className="rounded-md border px-3 py-1 text-sm hover:bg-muted disabled:opacity-50"
                >
                  <ChevronLeft className="h-4 w-4" />
                </button>
                <span className="text-sm">
                  Page {currentPage} of {totalPages}
                </span>
                <button
                  onClick={() => setPage(currentPage + 1)}
                  disabled={currentPage >= totalPages}
                  className="rounded-md border px-3 py-1 text-sm hover:bg-muted disabled:opacity-50"
                >
                  <ChevronRight className="h-4 w-4" />
                </button>
              </div>
            </div>
          </>
        ) : (
          <div className="flex items-center justify-center h-64 text-muted-foreground">
            No data available
          </div>
        )}
      </div>
    </div>
  );
}
