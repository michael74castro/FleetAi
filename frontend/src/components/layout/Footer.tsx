import { Link } from 'react-router-dom';
import { ChevronRight, ChevronUp, Truck } from 'lucide-react';

export default function Footer() {
  const scrollToTop = () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  return (
    <footer className="relative bg-brand-orange text-white overflow-hidden">
      {/* Decorative elements in top right */}
      <div className="absolute top-0 right-0 w-96 h-64 pointer-events-none">
        {/* Orange gradient swooshes */}
        <div className="absolute top-4 right-16 w-32 h-32 bg-gradient-to-br from-amber-400/60 to-orange-500/40 rounded-full blur-2xl" />
        <div className="absolute top-12 right-8 w-24 h-24 bg-gradient-to-br from-amber-300/50 to-orange-400/30 rounded-full blur-xl" />
        <div className="absolute top-2 right-32 w-16 h-16 bg-gradient-to-br from-amber-400/70 to-orange-500/50 rounded-full blur-lg" />
      </div>

      {/* Brand name and tagline in top right */}
      <div className="absolute top-6 right-8 text-right z-10">
        <div className="flex items-center justify-end space-x-2 mb-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-white/20">
            <Truck className="h-4 w-4 text-white" />
          </div>
          <span className="text-xl font-bold">MyFleet</span>
        </div>
        <p className="text-white/90 text-sm font-medium">What's next?</p>
      </div>

      {/* Main content */}
      <div className="relative z-10 px-8 py-12 max-w-7xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8 lg:gap-12">
          {/* Contact Column */}
          <div>
            <h3 className="text-lg font-semibold mb-4 italic">Contact</h3>
            <div className="space-y-1 text-sm text-white/90">
              <p>LeasePlan Emirates L.L.C.</p>
              <p>Al Fahim Building Office#112-118</p>
              <p>36679 Abu Dhabi</p>
              <p>United Arab Emirates</p>
              <p className="pt-2">Contact Us here</p>
              <p>Tel: +9712.404.6444</p>
            </div>
          </div>

          {/* Operations Column */}
          <div>
            <h3 className="text-lg font-semibold mb-4 italic">Operations</h3>
            <ul className="space-y-2">
              <li>
                <Link
                  to="/operation"
                  className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                >
                  Fleet
                  <ChevronRight className="h-4 w-4 ml-1 group-hover:translate-x-0.5 transition-transform" />
                </Link>
              </li>
              <li>
                <Link
                  to="/operation/renewals"
                  className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                >
                  Renewals & orders
                  <ChevronRight className="h-4 w-4 ml-1 group-hover:translate-x-0.5 transition-transform" />
                </Link>
              </li>
              <li>
                <Link
                  to="/operation/service"
                  className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                >
                  Service & MOT
                  <ChevronRight className="h-4 w-4 ml-1 group-hover:translate-x-0.5 transition-transform" />
                </Link>
              </li>
            </ul>
          </div>

          {/* Analysis Column */}
          <div>
            <h3 className="text-lg font-semibold mb-4 italic">Analysis</h3>
            <ul className="space-y-2">
              <li>
                <Link
                  to="/analysis/fleet"
                  className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                >
                  Fleet
                  <ChevronRight className="h-4 w-4 ml-1 group-hover:translate-x-0.5 transition-transform" />
                </Link>
              </li>
              <li>
                <Link
                  to="/analysis/renewals"
                  className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                >
                  Renewals & orders
                  <ChevronRight className="h-4 w-4 ml-1 group-hover:translate-x-0.5 transition-transform" />
                </Link>
              </li>
              <li>
                <Link
                  to="/analysis/fines"
                  className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                >
                  Fines
                  <ChevronRight className="h-4 w-4 ml-1 group-hover:translate-x-0.5 transition-transform" />
                </Link>
              </li>
            </ul>
          </div>

          {/* Reporting Column */}
          <div>
            <h3 className="text-lg font-semibold mb-4 italic">Reporting</h3>
            <ul className="space-y-2">
              <li>
                <Link
                  to="/reports"
                  className="inline-flex items-center text-sm text-white/90 hover:text-white transition-colors group"
                >
                  List Reports
                  <ChevronRight className="h-4 w-4 ml-1 group-hover:translate-x-0.5 transition-transform" />
                </Link>
              </li>
            </ul>
          </div>
        </div>

        {/* Bottom section */}
        <div className="mt-12 pt-6 border-t border-white/20">
          <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
            <div className="flex flex-wrap items-center gap-2 text-sm text-white/80">
              <a href="#" className="hover:text-white transition-colors">Terms of service</a>
              <span className="text-white/40">|</span>
              <a href="#" className="hover:text-white transition-colors">Privacy Statement</a>
              <span className="text-white/40">|</span>
              <a href="#" className="hover:text-white transition-colors">Cookie Policy</a>
            </div>
            <p className="text-sm text-white/80">&copy; 2026 LeasePlan.</p>
          </div>
        </div>
      </div>

      {/* Scroll to top button */}
      <button
        onClick={scrollToTop}
        className="absolute bottom-6 right-6 w-10 h-10 bg-white/20 hover:bg-white/30 rounded-full flex items-center justify-center transition-colors z-20"
        aria-label="Scroll to top"
      >
        <ChevronUp className="h-5 w-5 text-white" />
      </button>
    </footer>
  );
}
