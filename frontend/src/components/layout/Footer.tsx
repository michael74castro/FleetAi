import { Link } from 'react-router-dom';
import { ChevronRight, ChevronUp } from 'lucide-react';

export default function Footer() {
  const scrollToTop = () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  return (
    <footer className="relative">
      {/* Gray separator at top */}
      <div className="h-8 bg-gray-200" />

      {/* Main footer content */}
      <div className="relative bg-[#E05206] text-white overflow-hidden">
        {/* Decorative swooshes in top right */}
        <div className="absolute top-0 right-0 w-[400px] h-[200px] pointer-events-none overflow-hidden">
          {/* Main swoosh shape */}
          <svg
            viewBox="0 0 400 200"
            className="absolute top-0 right-0 w-full h-full"
            preserveAspectRatio="none"
          >
            <defs>
              <linearGradient id="swoosh1" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stopColor="#F5A623" stopOpacity="0.8" />
                <stop offset="100%" stopColor="#E05206" stopOpacity="0.6" />
              </linearGradient>
              <linearGradient id="swoosh2" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stopColor="#F7B731" stopOpacity="0.7" />
                <stop offset="100%" stopColor="#F5A623" stopOpacity="0.5" />
              </linearGradient>
            </defs>
            {/* Back swoosh */}
            <path
              d="M 250 0 Q 200 50 220 100 Q 240 150 300 180 Q 360 210 400 160 L 400 0 Z"
              fill="url(#swoosh1)"
            />
            {/* Front swoosh */}
            <path
              d="M 320 0 Q 280 30 290 80 Q 300 130 350 150 Q 380 160 400 120 L 400 0 Z"
              fill="url(#swoosh2)"
            />
          </svg>
        </div>

        {/* Brand name and tagline in top right */}
        <div className="absolute top-8 right-12 text-right z-10">
          <p className="text-2xl font-semibold text-white mb-1">LeasePlan</p>
          <p className="text-white text-sm font-medium">What's next?</p>
        </div>

        {/* Main content */}
        <div className="relative z-10 px-10 py-16 max-w-7xl">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-x-24 gap-y-8">
            {/* Contact Column */}
            <div>
              <h3 className="text-base font-semibold mb-5 italic">Contact</h3>
              <div className="space-y-0.5 text-sm text-white/90">
                <p>LeasePlan Emirates L.L.C.</p>
                <p>Al Fahim Building Office#112-118</p>
                <p>36679 Abu Dhabi</p>
                <p>United Arab Emirates</p>
                <p className="pt-3">Contact Us here</p>
                <p>Tel: +9712.404.6444</p>
              </div>
            </div>

            {/* Operations Column */}
            <div>
              <h3 className="text-base font-semibold mb-5 italic">Operations</h3>
              <ul className="space-y-1.5">
                <li>
                  <Link
                    to="/operation"
                    className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                  >
                    Fleet
                    <ChevronRight className="h-4 w-4 ml-0.5 group-hover:translate-x-0.5 transition-transform" />
                  </Link>
                </li>
                <li>
                  <Link
                    to="/operation/renewals"
                    className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                  >
                    Renewals & orders
                    <ChevronRight className="h-4 w-4 ml-0.5 group-hover:translate-x-0.5 transition-transform" />
                  </Link>
                </li>
                <li>
                  <Link
                    to="/analysis/fines"
                    className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                  >
                    Fines
                    <ChevronRight className="h-4 w-4 ml-0.5 group-hover:translate-x-0.5 transition-transform" />
                  </Link>
                </li>
              </ul>
            </div>

            {/* Analysis Column */}
            <div>
              <h3 className="text-base font-semibold mb-5 italic">Analysis</h3>
              <ul className="space-y-1.5">
                <li>
                  <Link
                    to="/analysis/fleet"
                    className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                  >
                    Fleet
                    <ChevronRight className="h-4 w-4 ml-0.5 group-hover:translate-x-0.5 transition-transform" />
                  </Link>
                </li>
                <li>
                  <Link
                    to="/analysis/renewals"
                    className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                  >
                    Renewals & orders
                    <ChevronRight className="h-4 w-4 ml-0.5 group-hover:translate-x-0.5 transition-transform" />
                  </Link>
                </li>
                <li>
                  <Link
                    to="/analysis/fines"
                    className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                  >
                    Fines
                    <ChevronRight className="h-4 w-4 ml-0.5 group-hover:translate-x-0.5 transition-transform" />
                  </Link>
                </li>
              </ul>
            </div>

            {/* Reporting Column */}
            <div>
              <h3 className="text-base font-semibold mb-5 italic">Reporting</h3>
              <ul className="space-y-1.5">
                <li>
                  <Link
                    to="/reports"
                    className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                  >
                    List Reports
                    <ChevronRight className="h-4 w-4 ml-0.5 group-hover:translate-x-0.5 transition-transform" />
                  </Link>
                </li>
              </ul>
            </div>
          </div>

          {/* Bottom section */}
          <div className="mt-16">
            <div className="flex flex-col gap-1 text-sm text-white/80">
              <div className="flex flex-wrap items-center gap-1">
                <a href="#" className="hover:text-white hover:underline transition-colors">Terms of service</a>
                <span className="text-white/50">|</span>
                <a href="#" className="hover:text-white hover:underline transition-colors">Privacy Statement</a>
                <span className="text-white/50">|</span>
                <a href="#" className="hover:text-white hover:underline transition-colors">Cookie Policy</a>
              </div>
              <p>&copy; 2026 LeasePlan.</p>
            </div>
          </div>
        </div>

        {/* Scroll to top button */}
        <button
          onClick={scrollToTop}
          className="absolute bottom-8 right-8 w-12 h-12 bg-white/20 hover:bg-white/30 rounded-full flex items-center justify-center transition-colors z-20"
          aria-label="Scroll to top"
        >
          <ChevronUp className="h-6 w-6 text-white" />
        </button>
      </div>
    </footer>
  );
}
