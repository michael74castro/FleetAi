import { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';

import { useAuthStore } from '@/store/authStore';
import { api } from '@/services/api';

// Layout
import Layout from '@/components/layout/Layout';

// Pages
import LoginPage from '@/pages/LoginPage';
import HomePage from '@/pages/HomePage';
import OperationPage from '@/pages/OperationPage';
import RenewalsOrdersPage from '@/pages/RenewalsOrdersPage';
import ServiceMOTPage from '@/pages/ServiceMOTPage';
import AnalysisPage from '@/pages/AnalysisPage';
import AnalysisFleetPage from '@/pages/AnalysisFleetPage';
import AnalysisRenewalsPage from '@/pages/AnalysisRenewalsPage';
import AnalysisFinesPage from '@/pages/AnalysisFinesPage';
import DashboardsPage from '@/pages/DashboardsPage';
import DashboardViewPage from '@/pages/DashboardViewPage';
import DashboardBuilderPage from '@/pages/DashboardBuilderPage';
import ReportsPage from '@/pages/ReportsPage';
import ReportViewPage from '@/pages/ReportViewPage';
import ReportBuilderPage from '@/pages/ReportBuilderPage';
import AIAssistantPage from '@/pages/AIAssistantPage';
import AdminPage from '@/pages/AdminPage';
import NotFoundPage from '@/pages/NotFoundPage';

// Protected Route component
function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, isLoading, setUser, setLoading, setError } = useAuthStore();
  const [checking, setChecking] = useState(true);

  useEffect(() => {
    const checkAuth = async () => {
      if (api.isAuthenticated()) {
        try {
          const user = await api.getCurrentUser();
          setUser(user);
        } catch (err) {
          setError('Session expired');
          api.clearTokens();
        }
      }
      setChecking(false);
      setLoading(false);
    };

    checkAuth();
  }, [setUser, setLoading, setError]);

  if (checking || isLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
      </div>
    );
  }

  if (!api.isAuthenticated()) {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
}

// Admin Route component
function AdminRoute({ children }: { children: React.ReactNode }) {
  const { user, hasRole } = useAuthStore();

  if (!user || !hasRole('admin')) {
    return <Navigate to="/dashboards" replace />;
  }

  return <>{children}</>;
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* Public routes */}
        <Route path="/login" element={<LoginPage />} />

        {/* Protected routes */}
        <Route
          path="/"
          element={
            <ProtectedRoute>
              <Layout />
            </ProtectedRoute>
          }
        >
          {/* Home */}
          <Route index element={<HomePage />} />

          {/* Operation - Fleet, Renewals & Orders, Service & MOT */}
          <Route path="operation" element={<OperationPage />} />
          <Route path="operation/renewals" element={<RenewalsOrdersPage />} />
          <Route path="operation/service" element={<ServiceMOTPage />} />
          <Route path="operation/vehicle/:vehicleId" element={<OperationPage />} />

          {/* Analysis */}
          <Route path="analysis" element={<AnalysisPage />} />
          <Route path="analysis/fleet" element={<AnalysisFleetPage />} />
          <Route path="analysis/renewals" element={<AnalysisRenewalsPage />} />
          <Route path="analysis/fines" element={<AnalysisFinesPage />} />

          {/* Dashboards (legacy, accessible via direct URL) */}
          <Route path="dashboards" element={<DashboardsPage />} />
          <Route path="dashboards/:id" element={<DashboardViewPage />} />
          <Route path="dashboards/:id/edit" element={<DashboardBuilderPage />} />
          <Route path="dashboards/new" element={<DashboardBuilderPage />} />

          {/* Reports */}
          <Route path="reports" element={<ReportsPage />} />
          <Route path="reports/:id" element={<ReportViewPage />} />
          <Route path="reports/:id/edit" element={<ReportBuilderPage />} />
          <Route path="reports/new" element={<ReportBuilderPage />} />

          {/* AI Assistant */}
          <Route path="assistant" element={<AIAssistantPage />} />
          <Route path="assistant/:conversationId" element={<AIAssistantPage />} />

          {/* Admin */}
          <Route
            path="admin/*"
            element={
              <AdminRoute>
                <AdminPage />
              </AdminRoute>
            }
          />
        </Route>

        {/* 404 */}
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
