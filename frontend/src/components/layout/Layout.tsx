import { Outlet } from 'react-router-dom';
import Header from './Header';
import Footer from './Footer';

export default function Layout() {
  return (
    <div className="relative flex h-screen overflow-hidden">
      {/* Animated gradient background */}
      <div className="fixed inset-0 gradient-bg-animated" />

      {/* Animated blobs for depth */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="blob-orange w-[600px] h-[600px] -top-48 -left-48 opacity-30" />
        <div className="blob-cyan w-[500px] h-[500px] top-1/3 -right-32 opacity-25" />
        <div className="blob-teal w-[400px] h-[400px] -bottom-32 left-1/4 opacity-20" />
      </div>

      {/* Noise texture overlay for depth */}
      <div className="fixed inset-0 noise-overlay pointer-events-none" />

      {/* Main layout - full width without sidebar */}
      <div className="relative flex flex-col w-full h-full z-10">
        <Header />
        <main className="flex-1 overflow-auto glass-scrollbar" style={{ position: 'relative', zIndex: 1 }}>
          <div className="p-6 min-h-full">
            <Outlet />
          </div>
          <Footer />
        </main>
      </div>
    </div>
  );
}
