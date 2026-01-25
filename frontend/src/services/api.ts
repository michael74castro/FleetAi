import axios, { AxiosInstance, AxiosError } from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api/v1';
const TOKEN_KEY = 'fleetai_token';
const REFRESH_TOKEN_KEY = 'fleetai_refresh_token';

class ApiService {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: API_BASE_URL,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Request interceptor for auth token
    this.client.interceptors.request.use(
      (config) => {
        const token = localStorage.getItem(TOKEN_KEY);
        if (token) {
          config.headers.Authorization = `Bearer ${token}`;
        }
        return config;
      },
      (error) => Promise.reject(error)
    );

    // Response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error: AxiosError) => {
        if (error.response?.status === 401) {
          // Clear tokens and redirect to login
          this.clearTokens();
          window.location.href = '/login';
        }
        return Promise.reject(error);
      }
    );
  }

  // Token management
  setTokens(accessToken: string, refreshToken: string) {
    localStorage.setItem(TOKEN_KEY, accessToken);
    localStorage.setItem(REFRESH_TOKEN_KEY, refreshToken);
  }

  clearTokens() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(REFRESH_TOKEN_KEY);
  }

  getToken() {
    return localStorage.getItem(TOKEN_KEY);
  }

  isAuthenticated() {
    return !!this.getToken();
  }

  // Auth
  async login(username: string, password: string) {
    const formData = new URLSearchParams();
    formData.append('username', username);
    formData.append('password', password);

    const response = await this.client.post('/auth/login', formData, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    });

    const { access_token, refresh_token } = response.data;
    this.setTokens(access_token, refresh_token);

    return response.data;
  }

  async register(email: string, password: string, displayName: string) {
    const response = await this.client.post('/auth/register', {
      email,
      password,
      display_name: displayName,
    });
    return response.data;
  }

  async getCurrentUser() {
    const response = await this.client.get('/auth/me');
    return response.data;
  }

  async validateToken() {
    const response = await this.client.get('/auth/validate');
    return response.data;
  }

  async logout() {
    try {
      await this.client.post('/auth/logout');
    } finally {
      this.clearTokens();
    }
  }

  async initTestData() {
    const response = await this.client.post('/auth/init-test-data');
    return response.data;
  }

  // Dashboards
  async getDashboards(params?: { page?: number; page_size?: number }) {
    const response = await this.client.get('/dashboards', { params });
    return response.data;
  }

  async getDashboard(id: number) {
    const response = await this.client.get(`/dashboards/${id}`);
    return response.data;
  }

  async createDashboard(data: {
    name: string;
    description?: string;
    layout_config?: Record<string, unknown>;
    is_default?: boolean;
    is_shared?: boolean;
  }) {
    const response = await this.client.post('/dashboards', data);
    return response.data;
  }

  async updateDashboard(id: number, data: Partial<{
    name: string;
    description: string;
    layout_config: Record<string, unknown>;
    is_default: boolean;
    is_shared: boolean;
  }>) {
    const response = await this.client.put(`/dashboards/${id}`, data);
    return response.data;
  }

  async deleteDashboard(id: number) {
    await this.client.delete(`/dashboards/${id}`);
  }

  async cloneDashboard(id: number, name: string) {
    const response = await this.client.post(`/dashboards/${id}/clone`, { new_name: name });
    return response.data;
  }

  // Widgets
  async addWidget(dashboardId: number, widget: {
    widget_type: string;
    title: string;
    position: { x: number; y: number; w: number; h: number };
    config: Record<string, unknown>;
    refresh_interval?: number;
  }) {
    const response = await this.client.post(`/dashboards/${dashboardId}/widgets`, widget);
    return response.data;
  }

  async updateWidget(dashboardId: number, widgetId: number, data: Partial<{
    title: string;
    position: { x: number; y: number; w: number; h: number };
    config: Record<string, unknown>;
    refresh_interval: number;
  }>) {
    const response = await this.client.put(`/dashboards/${dashboardId}/widgets/${widgetId}`, data);
    return response.data;
  }

  async deleteWidget(dashboardId: number, widgetId: number) {
    await this.client.delete(`/dashboards/${dashboardId}/widgets/${widgetId}`);
  }

  async getWidgetData(dashboardId: number, widgetId: number, params?: {
    filters?: Record<string, unknown>;
    date_range?: { start: string; end: string };
  }) {
    const response = await this.client.post(`/dashboards/${dashboardId}/widgets/${widgetId}/data`, params);
    return response.data;
  }

  // Reports
  async getReports(params?: { page?: number; page_size?: number }) {
    const response = await this.client.get('/reports', { params });
    return response.data;
  }

  async getReport(id: number) {
    const response = await this.client.get(`/reports/${id}`);
    return response.data;
  }

  async createReport(data: {
    name: string;
    description?: string;
    dataset_name: string;
    config: Record<string, unknown>;
    is_shared?: boolean;
  }) {
    const response = await this.client.post('/reports', data);
    return response.data;
  }

  async updateReport(id: number, data: Partial<{
    name: string;
    description: string;
    config: Record<string, unknown>;
    is_shared: boolean;
  }>) {
    const response = await this.client.put(`/reports/${id}`, data);
    return response.data;
  }

  async deleteReport(id: number) {
    await this.client.delete(`/reports/${id}`);
  }

  async executeReport(id: number, params?: {
    parameters?: Record<string, unknown>;
    page?: number;
    page_size?: number;
  }) {
    const response = await this.client.post(`/reports/${id}/execute`, params);
    return response.data;
  }

  async exportReport(id: number, format: 'excel' | 'pdf' | 'csv', parameters?: Record<string, unknown>) {
    const response = await this.client.post(`/reports/${id}/export`, { format, parameters });
    return response.data;
  }

  async getReportExecution(reportId: number, executionId: number) {
    const response = await this.client.get(`/reports/${reportId}/executions/${executionId}`);
    return response.data;
  }

  async downloadReportExecution(reportId: number, executionId: number) {
    const response = await this.client.get(`/reports/${reportId}/executions/${executionId}/download`, {
      responseType: 'blob',
    });
    return response.data;
  }

  // Report Schedules
  async createReportSchedule(reportId: number, data: {
    cron_expression: string;
    export_format: 'excel' | 'pdf' | 'csv';
    recipients: string[];
    is_active?: boolean;
  }) {
    const response = await this.client.post(`/reports/${reportId}/schedules`, data);
    return response.data;
  }

  async updateReportSchedule(reportId: number, scheduleId: number, data: Partial<{
    cron_expression: string;
    export_format: string;
    recipients: string[];
    is_active: boolean;
  }>) {
    const response = await this.client.put(`/reports/${reportId}/schedules/${scheduleId}`, data);
    return response.data;
  }

  async deleteReportSchedule(reportId: number, scheduleId: number) {
    await this.client.delete(`/reports/${reportId}/schedules/${scheduleId}`);
  }

  // Datasets
  async getDatasets(params?: { category?: string; page?: number; page_size?: number }) {
    const response = await this.client.get('/datasets', { params });
    return response.data;
  }

  async getDatasetSchema(name: string) {
    const response = await this.client.get(`/datasets/${name}/schema`);
    return response.data;
  }

  async previewDataset(name: string, params?: { limit?: number }) {
    const response = await this.client.get(`/datasets/${name}/preview`, { params });
    return response.data;
  }

  async getDistinctValues(datasetName: string, column: string, params?: { search?: string; limit?: number }) {
    const response = await this.client.get(`/datasets/${datasetName}/columns/${column}/values`, { params });
    return response.data;
  }

  async getAggregation(datasetName: string, data: {
    metrics: { field: string; function: string; alias?: string }[];
    dimensions?: string[];
    filters?: { field: string; op: string; value: unknown }[];
  }) {
    const response = await this.client.post(`/datasets/${datasetName}/aggregate`, data);
    return response.data;
  }

  // AI Agent
  async sendChatMessage(message: string, conversationId?: number) {
    const response = await this.client.post('/ai/chat', {
      message,
      conversation_id: conversationId,
    });
    return response.data;
  }

  async getConversations(params?: { page?: number; page_size?: number }) {
    const response = await this.client.get('/ai/conversations', { params });
    return response.data;
  }

  async getConversation(id: number) {
    const response = await this.client.get(`/ai/conversations/${id}`);
    return response.data;
  }

  async deleteConversation(id: number) {
    await this.client.delete(`/ai/conversations/${id}`);
  }

  async generateSQL(query: string, execute?: boolean) {
    const response = await this.client.post('/ai/generate-sql', { query, execute });
    return response.data;
  }

  async generateInsights(data: {
    dataset: string;
    metric?: string;
    filters?: Record<string, unknown>;
    insight_type?: string;
  }) {
    const response = await this.client.post('/ai/insights', data);
    return response.data;
  }

  async suggestDashboardWidgets(dataset: string, description?: string) {
    const response = await this.client.post('/ai/suggest-widgets', { dataset, description });
    return response.data;
  }

  async suggestReportConfig(purpose: string, dataset?: string) {
    const response = await this.client.post('/ai/suggest-report', { purpose, dataset });
    return response.data;
  }

  // Admin
  async getUsers(params?: { page?: number; page_size?: number; search?: string }) {
    const response = await this.client.get('/admin/users', { params });
    return response.data;
  }

  async getUser(id: number) {
    const response = await this.client.get(`/admin/users/${id}`);
    return response.data;
  }

  async updateUser(id: number, data: Partial<{
    role_id: number;
    is_active: boolean;
  }>) {
    const response = await this.client.put(`/admin/users/${id}`, data);
    return response.data;
  }

  async grantCustomerAccess(userId: number, customerIds: string[]) {
    const response = await this.client.post(`/admin/users/${userId}/customers`, { customer_ids: customerIds });
    return response.data;
  }

  async revokeCustomerAccess(userId: number, customerId: string) {
    await this.client.delete(`/admin/users/${userId}/customers/${customerId}`);
  }

  async getRoles() {
    const response = await this.client.get('/admin/roles');
    return response.data;
  }

  async getSystemStats() {
    const response = await this.client.get('/admin/stats');
    return response.data;
  }

  // Fleet KPIs
  async getFleetKPIs() {
    const response = await this.client.get('/fleet/kpis');
    return response.data;
  }

  async getFleetOverview() {
    const response = await this.client.get('/fleet/overview');
    return response.data;
  }

  async getTopCustomers(limit: number = 5) {
    const response = await this.client.get('/fleet/top-customers', { params: { limit } });
    return response.data;
  }

  async getVehiclesByStatus() {
    const response = await this.client.get('/fleet/vehicles-by-status');
    return response.data;
  }

  // Fleet Operations
  async getOperationsKPIs() {
    const response = await this.client.get('/fleet/operations/kpis');
    return response.data;
  }

  async getVehiclesList(params?: {
    page?: number;
    page_size?: number;
    search?: string;
    driver?: string;
    license_plate?: string;
    status?: string;
    customer_id?: number;
    make?: string;
    sort_by?: string;
    sort_order?: 'asc' | 'desc';
  }) {
    const response = await this.client.get('/fleet/vehicles', { params });
    return response.data;
  }

  async getVehicleDetails(vehicleId: number) {
    const response = await this.client.get(`/fleet/vehicles/${vehicleId}`);
    return response.data;
  }

  async getDriversList(params?: {
    page?: number;
    page_size?: number;
    search?: string;
  }) {
    const response = await this.client.get('/fleet/drivers', { params });
    return response.data;
  }

  async exportVehicles(format: 'excel' | 'csv', params?: {
    search?: string;
    driver?: string;
    license_plate?: string;
    status?: string;
  }) {
    const response = await this.client.get('/fleet/vehicles/export', {
      params: { format, ...params },
      responseType: 'blob'
    });
    return response.data;
  }
}

export const api = new ApiService();
