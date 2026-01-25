import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Truck, Loader2, Mail, Lock, ArrowRight } from 'lucide-react';
import { api } from '@/services/api';
import { useAuthStore } from '@/store/authStore';

// Check if Azure AD is configured
const AZURE_AD_ENABLED = !!(
  import.meta.env.VITE_AZURE_CLIENT_ID &&
  import.meta.env.VITE_AZURE_TENANT_ID &&
  import.meta.env.VITE_AZURE_CLIENT_ID !== 'your-client-id'
);

export default function LoginPage() {
  const navigate = useNavigate();
  const { setUser, isAuthenticated } = useAuthStore();

  const [email, setEmail] = useState('admin@fleetai.local');
  const [password, setPassword] = useState('admin123');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isAuthenticated || api.isAuthenticated()) {
      navigate('/dashboards');
    }
  }, [isAuthenticated, navigate]);

  const handleLocalLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    try {
      await api.login(email, password);
      const user = await api.getCurrentUser();
      setUser(user);
      navigate('/dashboards');
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string } } };
      setError(error.response?.data?.detail || 'Login failed. Please check your credentials.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleAzureLogin = async () => {
    const { msalInstance, loginRequest } = await import('@/lib/msal');
    await msalInstance.initialize();
    msalInstance.loginRedirect(loginRequest);
  };

  return (
    <div className="relative min-h-screen flex items-center justify-center overflow-hidden">
      {/* Animated gradient background */}
      <div className="fixed inset-0 gradient-bg-animated" />

      {/* Animated blobs */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="blob-orange w-[800px] h-[800px] -top-64 -left-64 opacity-40" />
        <div className="blob-cyan w-[600px] h-[600px] top-1/4 -right-48 opacity-30" />
        <div className="blob-teal w-[500px] h-[500px] -bottom-48 left-1/3 opacity-25" />
      </div>

      {/* Noise overlay */}
      <div className="fixed inset-0 noise-overlay pointer-events-none" />

      {/* Decorative elements */}
      <div className="fixed top-20 left-20 w-32 h-32 border border-white/10 rounded-3xl rotate-12 animate-float" />
      <div className="fixed bottom-32 right-32 w-24 h-24 border border-white/10 rounded-2xl -rotate-12 animate-float" style={{ animationDelay: '2s' }} />
      <div className="fixed top-1/3 right-20 w-16 h-16 bg-brand-orange/20 rounded-full blur-xl animate-pulse-glow" />

      {/* Main content */}
      <div className="relative z-10 w-full max-w-md px-4 animate-fade-in-up">
        {/* Logo section */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center mb-6">
            <div className="relative">
              <div className="absolute inset-0 bg-brand-orange rounded-2xl blur-2xl opacity-60 animate-pulse-glow" />
              <div className="relative flex h-20 w-20 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-orange via-brand-orange to-brand-amber shadow-glow-orange">
                <Truck className="h-10 w-10 text-white" />
              </div>
            </div>
          </div>
          <h1 className="text-4xl font-bold text-white tracking-tight">
            My<span className="text-gradient-orange">Fleet</span>
          </h1>
          <p className="mt-2 text-white/60 font-medium">
            AI-Powered Fleet Intelligence Platform
          </p>
        </div>

        {/* Glass card */}
        <div className="glass-panel p-8 space-y-6">
          <div className="text-center">
            <h2 className="text-xl font-semibold text-white">Welcome back</h2>
            <p className="text-sm text-white/50 mt-1">Sign in to your account to continue</p>
          </div>

          {/* Login Form */}
          <form onSubmit={handleLocalLogin} className="space-y-5">
            {error && (
              <div className="glass-card border-red-500/30 bg-red-500/10 p-4 rounded-xl animate-scale-in">
                <p className="text-sm text-red-300">{error}</p>
              </div>
            )}

            <div className="space-y-4">
              <div className="relative group">
                <Mail className="absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-white/40 group-focus-within:text-brand-orange transition-colors" />
                <input
                  id="email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full glass-input pl-12 pr-4 py-4 text-white"
                  placeholder="Email address"
                  required
                />
              </div>

              <div className="relative group">
                <Lock className="absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-white/40 group-focus-within:text-brand-orange transition-colors" />
                <input
                  id="password"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full glass-input pl-12 pr-4 py-4 text-white"
                  placeholder="Password"
                  required
                />
              </div>
            </div>

            <button
              type="submit"
              disabled={isLoading}
              className="relative w-full glass-button-primary py-4 text-white font-semibold rounded-xl overflow-hidden group disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span className="relative z-10 flex items-center justify-center space-x-2">
                {isLoading ? (
                  <Loader2 className="h-5 w-5 animate-spin" />
                ) : (
                  <>
                    <span>Sign In</span>
                    <ArrowRight className="h-5 w-5 group-hover:translate-x-1 transition-transform" />
                  </>
                )}
              </span>
            </button>
          </form>

          {/* Azure AD Login */}
          {AZURE_AD_ENABLED && (
            <>
              <div className="relative">
                <div className="absolute inset-0 flex items-center">
                  <div className="w-full border-t border-white/10" />
                </div>
                <div className="relative flex justify-center text-xs uppercase">
                  <span className="bg-brand-teal-dark px-4 text-white/40 font-medium">Or continue with</span>
                </div>
              </div>

              <button
                onClick={handleAzureLogin}
                className="w-full glass-button py-4 text-white font-medium rounded-xl group"
              >
                <span className="flex items-center justify-center space-x-3">
                  <svg className="h-5 w-5" viewBox="0 0 21 21" fill="none">
                    <rect width="9" height="9" fill="#F25022" />
                    <rect x="11" width="9" height="9" fill="#7FBA00" />
                    <rect y="11" width="9" height="9" fill="#00A4EF" />
                    <rect x="11" y="11" width="9" height="9" fill="#FFB900" />
                  </svg>
                  <span>Sign in with Microsoft</span>
                </span>
              </button>
            </>
          )}

          {/* Test credentials hint */}
          <div className="glass rounded-xl p-4 border-brand-cyan/20">
            <div className="flex items-start space-x-3">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-brand-cyan/20">
                <Lock className="h-4 w-4 text-brand-cyan" />
              </div>
              <div>
                <p className="text-xs font-semibold text-white/70 mb-1">Demo Credentials</p>
                <p className="text-xs text-white/50">
                  <span className="text-white/70">Email:</span> admin@fleetai.local
                </p>
                <p className="text-xs text-white/50">
                  <span className="text-white/70">Password:</span> admin123
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <p className="text-center text-xs text-white/40 mt-6">
          By signing in, you agree to our{' '}
          <a href="#" className="text-brand-cyan hover:text-brand-cyan-light transition-colors">Terms of Service</a>
          {' '}and{' '}
          <a href="#" className="text-brand-cyan hover:text-brand-cyan-light transition-colors">Privacy Policy</a>
        </p>
      </div>
    </div>
  );
}
