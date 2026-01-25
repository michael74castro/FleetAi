import { Link } from 'react-router-dom';
import { Home, ArrowLeft, AlertTriangle } from 'lucide-react';

export default function NotFoundPage() {
  return (
    <div className="relative min-h-screen flex items-center justify-center overflow-hidden">
      {/* Animated gradient background */}
      <div className="fixed inset-0 gradient-bg-animated" />

      {/* Animated blobs */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="blob-orange w-[600px] h-[600px] -top-32 -right-32 opacity-30" />
        <div className="blob-cyan w-[500px] h-[500px] -bottom-32 -left-32 opacity-25" />
      </div>

      {/* Noise overlay */}
      <div className="fixed inset-0 noise-overlay pointer-events-none" />

      {/* Content */}
      <div className="relative z-10 text-center px-4 animate-fade-in-up">
        {/* 404 Number */}
        <div className="relative inline-block mb-6">
          <div className="absolute inset-0 text-[200px] font-black text-brand-orange/10 blur-xl select-none">
            404
          </div>
          <h1 className="relative text-[150px] sm:text-[200px] font-black text-gradient-orange leading-none">
            404
          </h1>
        </div>

        {/* Icon */}
        <div className="relative inline-flex mb-8">
          <div className="absolute inset-0 bg-brand-amber rounded-full blur-2xl opacity-40 animate-pulse-glow" />
          <div className="relative flex h-20 w-20 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-amber/20 to-brand-orange/20 border border-brand-orange/30">
            <AlertTriangle className="h-10 w-10 text-brand-orange" />
          </div>
        </div>

        {/* Text */}
        <h2 className="text-3xl font-bold text-white mb-3">Page Not Found</h2>
        <p className="text-white/50 max-w-md mx-auto mb-10">
          The page you're looking for doesn't exist or has been moved to a new location.
        </p>

        {/* Actions */}
        <div className="flex items-center justify-center space-x-4">
          <Link
            to="/"
            className="glass-button-primary flex items-center space-x-2 px-6 py-3 rounded-xl text-white font-semibold group"
          >
            <Home className="h-5 w-5 group-hover:scale-110 transition-transform" />
            <span>Go Home</span>
          </Link>
          <button
            onClick={() => window.history.back()}
            className="glass-button flex items-center space-x-2 px-6 py-3 rounded-xl text-white/80 font-medium group"
          >
            <ArrowLeft className="h-5 w-5 group-hover:-translate-x-1 transition-transform" />
            <span>Go Back</span>
          </button>
        </div>
      </div>
    </div>
  );
}
