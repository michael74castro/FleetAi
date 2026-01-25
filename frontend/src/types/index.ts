// User & Auth Types
export interface User {
  user_id: number;
  azure_id: string;
  email: string;
  display_name: string;
  role_name: string;
  customer_ids: string[] | null;
  permissions: string[];
  is_active: boolean;
}

export interface UserCustomerAccess {
  user_id: number;
  customer_id: string;
  customer_name: string;
  granted_at: string;
}

// Dashboard Types
export interface Dashboard {
  dashboard_id: number;
  name: string;
  description?: string;
  layout_config: LayoutConfig;
  is_default: boolean;
  is_shared: boolean;
  created_by: number;
  created_at: string;
  updated_at: string;
  widgets: Widget[];
}

export interface LayoutConfig {
  cols: number;
  rowHeight: number;
  margin?: [number, number];
}

export interface Widget {
  widget_id: number;
  widget_type: WidgetType;
  title: string;
  position: WidgetPosition;
  config: WidgetConfig;
  refresh_interval?: number;
}

export type WidgetType =
  | 'kpi_card'
  | 'line_chart'
  | 'bar_chart'
  | 'pie_chart'
  | 'donut_chart'
  | 'area_chart'
  | 'table'
  | 'gauge'
  | 'map';

export interface WidgetPosition {
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface WidgetConfig {
  dataset?: string;
  metric?: string;
  metrics?: string[];
  dimensions?: string[];
  dimension?: string;
  label?: string;
  format?: string;
  comparison?: string;
  filters?: FilterConfig[];
  columns?: ColumnConfig[];
  sorting?: SortConfig[];
  limit?: number;
  target?: number;
  thresholds?: ThresholdConfig[];
  pageSize?: number;
}

export interface FilterConfig {
  field: string;
  op: string;
  value: unknown;
  param?: string;
}

export interface ColumnConfig {
  field: string;
  label?: string;
  format?: string;
  sortable?: boolean;
  width?: number;
}

export interface SortConfig {
  field: string;
  direction: 'asc' | 'desc';
}

export interface ThresholdConfig {
  value: number;
  color: string;
  label?: string;
}

// Report Types
export interface Report {
  report_id: number;
  name: string;
  description?: string;
  dataset_name: string;
  config: ReportConfig;
  is_shared: boolean;
  created_by: number;
  created_at: string;
  updated_at: string;
  schedules?: ReportSchedule[];
}

export interface ReportConfig {
  columns: ColumnConfig[];
  filters: FilterConfig[];
  sorting: SortConfig[];
  grouping?: string[];
  aggregations?: AggregationConfig[];
}

export interface AggregationConfig {
  field: string;
  function: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX';
  label: string;
}

export interface ReportSchedule {
  schedule_id: number;
  cron_expression: string;
  export_format: 'excel' | 'pdf' | 'csv';
  recipients: string[];
  is_active: boolean;
  next_run?: string;
  last_run?: string;
}

export interface ReportExecution {
  execution_id: number;
  status: 'pending' | 'running' | 'success' | 'failed';
  export_format: string;
  file_path?: string;
  row_count?: number;
  error_message?: string;
  started_at: string;
  completed_at?: string;
}

// Dataset Types
export interface Dataset {
  dataset_id: number;
  name: string;
  display_name: string;
  description?: string;
  source_object: string;
  category?: string;
  rbac_column?: string;
  is_active: boolean;
}

export interface DatasetColumn {
  name: string;
  type: string;
  nullable: boolean;
}

// AI Types
export interface Conversation {
  conversation_id: number;
  title?: string;
  created_at: string;
  updated_at: string;
  messages: Message[];
}

export interface Message {
  message_id: number;
  role: 'user' | 'assistant' | 'system';
  content: string;
  metadata?: MessageMetadata;
  created_at: string;
}

export interface MessageMetadata {
  model?: string;
  tokens?: number;
  sql?: string;
  error?: string;
}

export interface ChatResponse {
  message: string;
  conversation_id: number;
  message_id: number;
  data?: Record<string, unknown>[];
  suggestions?: string[];
  sources?: string[];
  metadata?: Record<string, unknown>;
}

export interface SQLGenerationResult {
  sql: string;
  explanation: string;
  is_safe: boolean;
  safety_notes?: string;
  results?: Record<string, unknown>[];
  row_count?: number;
}

export interface InsightResult {
  title: string;
  summary: string;
  details?: string;
  recommendations?: string[];
}

export interface WidgetSuggestion {
  widget_type: WidgetType;
  title: string;
  description: string;
  config: WidgetConfig;
  relevance_score: number;
}

// Common Types
export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface ApiError {
  detail: string;
  status_code?: number;
}

// Widget Data Types
export interface KPIData {
  value: number;
  comparison_value?: number;
  format: string;
  label: string;
}

export interface ChartData {
  data: Record<string, unknown>[];
  dimensions: string[];
  metrics: string[];
}

export interface DistributionData {
  data: { category: string; value: number }[];
  total: number;
}

export interface TableData {
  columns: string[];
  data: Record<string, unknown>[];
  pageSize: number;
}

export interface GaugeData {
  value: number;
  target: number;
  percentage: number;
  thresholds: ThresholdConfig[];
}

export type WidgetData = KPIData | ChartData | DistributionData | TableData | GaugeData;
