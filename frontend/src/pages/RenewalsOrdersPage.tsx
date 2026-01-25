import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  ChevronDown,
  ChevronRight,
  Search,
  SlidersHorizontal,
  Settings2,
  Download,
  Printer,
  Info,
  ChevronLeft,
  ChevronsLeft,
  ChevronsRight,
  Heart
} from 'lucide-react';
import { api } from '@/services/api';

interface RenewalsKPIs {
  overdue_renewals_with_order: number;
  overdue_renewals_no_order: number;
  renewals_due_without_order: number;
  renewal_orders: number;
  new_orders: number;
  last_data_update: string;
  last_monthly_closure: string;
}

interface RenewalOrder {
  id: number;
  registration_number: string;
  driver_name: string;
  status: string;
  object_no: number;
  model: string;
  make: string;
  vehicle_type: string;
  order_status: string;
  is_renewal: boolean;
  is_new: boolean;
  due_date: string;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

export default function RenewalsOrdersPage() {
  // State
  const [kpis, setKpis] = useState<RenewalsKPIs | null>(null);
  const [orders, setOrders] = useState<RenewalOrder[]>([]);
  const [loading, setLoading] = useState(true);
  const [tableLoading, setTableLoading] = useState(false);

  // Pagination
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [totalItems, setTotalItems] = useState(0);
  const [totalPages, setTotalPages] = useState(0);

  // Basic Filters
  const [driverSearch, setDriverSearch] = useState('');
  const [licensePlateSearch, setLicensePlateSearch] = useState('');
  const [selectedFavourite, setSelectedFavourite] = useState('');
  const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);

  // Advanced filters
  const [companyName, setCompanyName] = useState('');
  const [costCentre, setCostCentre] = useState('');
  const [vehicleType, setVehicleType] = useState('');
  const [makeFilter, setMakeFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [orderStatus, setOrderStatus] = useState('');

  // Checkboxes
  const [isOrderRenewal, setIsOrderRenewal] = useState(false);
  const [isOrderNew, setIsOrderNew] = useState(false);
  const [dueForRenewalWithoutOrder, setDueForRenewalWithoutOrder] = useState(false);
  const [dueForRenewal, setDueForRenewal] = useState(false);
  const [overdueWithOrder, setOverdueWithOrder] = useState(false);
  const [overdueWithoutOrder, setOverdueWithoutOrder] = useState(false);

  // View configuration
  const [currentView, setCurrentView] = useState('View 3');
  const [showColumnConfig, setShowColumnConfig] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState<string[]>([
    'registration_number',
    'driver_name',
    'status',
    'object_no',
    'model'
  ]);

  // Sort
  const [sortBy, setSortBy] = useState('registration_number');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');

  // All available columns
  const allColumns = [
    { key: 'registration_number', label: 'License Plate' },
    { key: 'driver_name', label: 'Driver' },
    { key: 'status', label: 'Status' },
    { key: 'object_no', label: 'ObjectNo' },
    { key: 'model', label: 'Model' },
    { key: 'make', label: 'Make' },
    { key: 'vehicle_type', label: 'Vehicle Type' },
    { key: 'order_status', label: 'Order Status' },
    { key: 'due_date', label: 'Due Date' }
  ];

  // Fetch KPIs
  useEffect(() => {
    const fetchKPIs = async () => {
      try {
        const data = await api.getOperationsKPIs();
        setKpis(data);
      } catch (error) {
        // Use fallback data
        setKpis({
          overdue_renewals_with_order: 10,
          overdue_renewals_no_order: 780,
          renewals_due_without_order: 926,
          renewal_orders: 4,
          new_orders: 64,
          last_data_update: '2026-01-23T04:33:32Z',
          last_monthly_closure: 'December 2025'
        });
      } finally {
        setLoading(false);
      }
    };
    fetchKPIs();
  }, []);

  // Fetch orders
  useEffect(() => {
    const fetchOrders = async () => {
      setTableLoading(true);
      try {
        // TODO: Replace with actual API call
        // const data = await api.getRenewalsOrders({ page, page_size: pageSize, ... });
        throw new Error('API not implemented');
      } catch (error) {
        // Use fallback mock data
        const mockOrders: RenewalOrder[] = Array.from({ length: pageSize }, (_, i) => ({
          id: i + 1 + (page - 1) * pageSize,
          registration_number: `UAE-${String(20000 + i + (page - 1) * pageSize).padStart(5, '0')}`,
          driver_name: `Driver ${i + 1 + (page - 1) * pageSize}`,
          status: ['Active', 'Pending', 'Due', 'Overdue'][i % 4],
          object_no: 100000 + i + (page - 1) * pageSize,
          model: ['Camry', 'Patrol', 'Pajero', 'Accord', 'Explorer'][i % 5],
          make: ['Toyota', 'Nissan', 'Mitsubishi', 'Honda', 'Ford'][i % 5],
          vehicle_type: ['Sedan', 'SUV', 'Pickup', 'Van'][i % 4],
          order_status: ['New', 'In Progress', 'Completed', 'Cancelled'][i % 4],
          is_renewal: i % 3 === 0,
          is_new: i % 5 === 0,
          due_date: new Date(Date.now() + (i - 5) * 24 * 60 * 60 * 1000).toISOString().split('T')[0]
        }));
        setOrders(mockOrders);
        setTotalItems(1804);
        setTotalPages(Math.ceil(1804 / pageSize));
      } finally {
        setTableLoading(false);
      }
    };
    fetchOrders();
  }, [page, pageSize, sortBy, sortOrder]);

  // Handle search
  const handleSearch = () => {
    setPage(1);
  };

  // Handle reset filters
  const handleReset = () => {
    setDriverSearch('');
    setLicensePlateSearch('');
    setSelectedFavourite('');
    setCompanyName('');
    setCostCentre('');
    setVehicleType('');
    setMakeFilter('');
    setStatusFilter('');
    setOrderStatus('');
    setIsOrderRenewal(false);
    setIsOrderNew(false);
    setDueForRenewalWithoutOrder(false);
    setDueForRenewal(false);
    setOverdueWithOrder(false);
    setOverdueWithoutOrder(false);
    setPage(1);
  };

  // Handle column sort
  const handleSort = (column: string) => {
    if (sortBy === column) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(column);
      setSortOrder('asc');
    }
  };

  // Handle export
  const handleExport = async () => {
    try {
      const blob = await api.exportVehicles('excel', {
        driver: driverSearch || undefined,
        license_plate: licensePlateSearch || undefined
      });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `renewals_orders_${new Date().toISOString().split('T')[0]}.xlsx`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (error) {
      console.error('Export failed:', error);
    }
  };

  // Handle print
  const handlePrint = () => {
    window.print();
  };

  // Toggle column visibility
  const toggleColumn = (columnKey: string) => {
    setVisibleColumns((prev) =>
      prev.includes(columnKey)
        ? prev.filter((c) => c !== columnKey)
        : [...prev, columnKey]
    );
  };

  // Format numbers
  const formatNumber = (num: number) => {
    return num.toLocaleString();
  };

  // Format date
  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'long',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      timeZone: 'UTC'
    }) + ' UTC';
  };

  // Pagination helpers
  const canGoPrevious = page > 1;
  const canGoNext = page < totalPages;

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Breadcrumb */}
      <nav className="flex items-center space-x-2 text-sm">
        <Link to="/" className="text-brand-orange hover:text-brand-orange-light transition-colors">
          Home
        </Link>
        <ChevronRight className="h-4 w-4 text-white/40" />
        <Link to="/operation" className="text-brand-orange hover:text-brand-orange-light transition-colors">
          Operation
        </Link>
        <ChevronRight className="h-4 w-4 text-white/40" />
        <span className="text-white/60">Renewals & orders</span>
      </nav>

      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <h1 className="text-2xl font-bold text-white">Operation - Renewals & Orders</h1>
          <button className="text-white/40 hover:text-white/60 transition-colors">
            <Info className="h-5 w-5" />
          </button>
        </div>
        <p className="text-sm text-white/50">
          Last data update: {kpis ? formatDate(kpis.last_data_update) : '...'} | Last monthly closure: {kpis?.last_monthly_closure || '...'}
        </p>
      </div>

      {/* KPI Cards - 5 tiles */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {/* Overdue Renewals with an order */}
        <div className="glass-panel p-5 hover:border-brand-orange/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-orange transition-colors">
              {loading ? '...' : formatNumber(kpis?.overdue_renewals_with_order || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Overdue Renewals with an order</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Overdue Renewals with no order */}
        <div className="glass-panel p-5 hover:border-brand-cyan/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-cyan transition-colors">
              {loading ? '...' : formatNumber(kpis?.overdue_renewals_no_order || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Overdue Renewals with no order</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Renewals Due without order */}
        <div className="glass-panel p-5 hover:border-brand-green/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-green transition-colors">
              {loading ? '...' : formatNumber(kpis?.renewals_due_without_order || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Renewals Due without order</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Renewal Orders */}
        <div className="glass-panel p-5 hover:border-brand-amber/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-amber transition-colors">
              {loading ? '...' : formatNumber(kpis?.renewal_orders || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Renewal Orders</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* New Orders */}
        <div className="glass-panel p-5 hover:border-brand-teal/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-teal transition-colors">
              {loading ? '...' : formatNumber(kpis?.new_orders || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">New Orders</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>
      </div>

      {/* Search & Filters */}
      <div className="glass-panel p-5">
        {/* Basic Filters Row */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Favourites */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Favourites</label>
            <div className="relative">
              <select
                value={selectedFavourite}
                onChange={(e) => setSelectedFavourite(e.target.value)}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
              >
                <option value="">Favourites</option>
                <option value="overdue">Overdue Renewals</option>
                <option value="due">Due for Renewal</option>
                <option value="new-orders">New Orders</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>

          {/* Driver */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">Driver</label>
            <input
              type="text"
              value={driverSearch}
              onChange={(e) => setDriverSearch(e.target.value)}
              placeholder="Driver"
              className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
            />
          </div>

          {/* License Plate */}
          <div>
            <label className="block text-sm font-medium text-white/70 mb-2">License Plate</label>
            <input
              type="text"
              value={licensePlateSearch}
              onChange={(e) => setLicensePlateSearch(e.target.value)}
              placeholder="License Plate"
              className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
            />
          </div>
        </div>

        {/* Advanced Criteria Toggle */}
        <button
          onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
          className="mt-4 flex items-center space-x-2 text-brand-orange hover:text-brand-orange-light transition-colors"
        >
          <SlidersHorizontal className="h-4 w-4" />
          <span className="text-sm font-medium">
            {showAdvancedFilters ? 'Close advanced criteria' : 'Advanced criteria'}
          </span>
        </button>

        {/* Advanced Filters */}
        {showAdvancedFilters && (
          <div className="mt-4 pt-4 border-t border-white/10 space-y-4">
            {/* Row 1: Company Name, Cost Centre, Vehicle Type */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Company Name</label>
                <div className="relative">
                  <select
                    value={companyName}
                    onChange={(e) => setCompanyName(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">Ignore criteria</option>
                    <option value="company1">Company 1</option>
                    <option value="company2">Company 2</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Cost Centre</label>
                <div className="relative">
                  <select
                    value={costCentre}
                    onChange={(e) => setCostCentre(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">Ignore criteria</option>
                    <option value="cc1">Cost Centre 1</option>
                    <option value="cc2">Cost Centre 2</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Vehicle Type</label>
                <div className="relative">
                  <select
                    value={vehicleType}
                    onChange={(e) => setVehicleType(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Types</option>
                    <option value="sedan">Sedan</option>
                    <option value="suv">SUV</option>
                    <option value="pickup">Pickup</option>
                    <option value="van">Van</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div></div>
            </div>

            {/* Row 2: Make, Status, Order Status */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Make</label>
                <div className="relative">
                  <select
                    value={makeFilter}
                    onChange={(e) => setMakeFilter(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Makes</option>
                    <option value="Toyota">Toyota</option>
                    <option value="Nissan">Nissan</option>
                    <option value="Mitsubishi">Mitsubishi</option>
                    <option value="Honda">Honda</option>
                    <option value="Ford">Ford</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Status</label>
                <div className="relative">
                  <select
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Status</option>
                    <option value="Active">Active</option>
                    <option value="Pending">Pending</option>
                    <option value="Due">Due</option>
                    <option value="Overdue">Overdue</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Order Status</label>
                <div className="relative">
                  <select
                    value={orderStatus}
                    onChange={(e) => setOrderStatus(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Order Status</option>
                    <option value="New">New</option>
                    <option value="In Progress">In Progress</option>
                    <option value="Completed">Completed</option>
                    <option value="Cancelled">Cancelled</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div></div>
            </div>

            {/* Row 3: Checkboxes */}
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={isOrderRenewal}
                  onChange={(e) => setIsOrderRenewal(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Is Order Renewal</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={isOrderNew}
                  onChange={(e) => setIsOrderNew(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Is Order New</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={dueForRenewalWithoutOrder}
                  onChange={(e) => setDueForRenewalWithoutOrder(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Due for Renewal without an order</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={dueForRenewal}
                  onChange={(e) => setDueForRenewal(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Due for Renewal</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={overdueWithOrder}
                  onChange={(e) => setOverdueWithOrder(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Overdue for Renewal with an order</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={overdueWithoutOrder}
                  onChange={(e) => setOverdueWithoutOrder(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Overdue for renewal without an order</span>
              </label>
            </div>

            {/* Save to favourites and Action buttons */}
            <div className="flex items-center justify-between pt-4">
              <button className="flex items-center space-x-2 text-brand-orange hover:text-brand-orange-light transition-colors">
                <Heart className="h-4 w-4" />
                <span className="text-sm font-medium">Save to favourites</span>
              </button>

              <div className="flex items-center space-x-3">
                <button
                  onClick={handleReset}
                  className="px-6 py-2.5 border border-white/20 text-white/70 hover:text-white hover:border-white/40 rounded-lg transition-colors"
                >
                  RESET
                </button>
                <button
                  onClick={handleSearch}
                  className="px-6 py-2.5 bg-brand-orange hover:bg-brand-orange-light text-white font-semibold rounded-lg transition-colors flex items-center space-x-2"
                >
                  <Search className="h-4 w-4" />
                  <span>SEARCH</span>
                </button>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* View Controls */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          {/* Switch to view */}
          <div className="flex items-center space-x-2">
            <span className="text-sm text-white/70">Switch to view</span>
            <div className="relative">
              <select
                value={currentView}
                onChange={(e) => setCurrentView(e.target.value)}
                className="bg-white/5 border border-white/10 rounded-lg px-4 py-2 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors pr-10"
              >
                <option value="View 1">View 1</option>
                <option value="View 2">View 2</option>
                <option value="View 3">View 3</option>
              </select>
              <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
            </div>
          </div>

          {/* Configure columns */}
          <button
            onClick={() => setShowColumnConfig(!showColumnConfig)}
            className="flex items-center space-x-2 text-brand-orange hover:text-brand-orange-light transition-colors"
          >
            <Settings2 className="h-4 w-4" />
            <span className="text-sm font-medium">Configure view & columns</span>
          </button>
        </div>

        {/* Print and Download */}
        <div className="flex items-center space-x-4">
          <button
            onClick={handlePrint}
            className="flex items-center space-x-2 text-white/50 hover:text-white transition-colors"
          >
            <Printer className="h-4 w-4" />
            <span className="text-sm">Print</span>
          </button>
          <button
            onClick={handleExport}
            className="flex items-center space-x-2 text-white/50 hover:text-white transition-colors"
          >
            <Download className="h-4 w-4" />
            <span className="text-sm">Download Table</span>
          </button>
        </div>
      </div>

      {/* Column Configuration Panel */}
      {showColumnConfig && (
        <div className="glass-panel p-4">
          <h3 className="text-sm font-semibold text-white mb-3">Visible Columns</h3>
          <div className="flex flex-wrap gap-2">
            {allColumns.map((col) => (
              <button
                key={col.key}
                onClick={() => toggleColumn(col.key)}
                className={`px-3 py-1.5 rounded-full text-sm transition-colors ${
                  visibleColumns.includes(col.key)
                    ? 'bg-brand-orange text-white'
                    : 'bg-white/5 text-white/50 hover:bg-white/10'
                }`}
              >
                {col.label}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Data Table */}
      <div className="glass-panel overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-white/10">
                {allColumns
                  .filter((col) => visibleColumns.includes(col.key))
                  .map((col) => (
                    <th
                      key={col.key}
                      onClick={() => handleSort(col.key)}
                      className="text-left px-4 py-3 text-sm font-semibold text-white/70 cursor-pointer hover:text-white transition-colors"
                    >
                      <div className="flex items-center space-x-1">
                        <span>{col.label}</span>
                        {sortBy === col.key && (
                          <ChevronDown
                            className={`h-4 w-4 transition-transform ${
                              sortOrder === 'desc' ? 'rotate-180' : ''
                            }`}
                          />
                        )}
                      </div>
                    </th>
                  ))}
              </tr>
            </thead>
            <tbody>
              {tableLoading ? (
                <tr>
                  <td
                    colSpan={visibleColumns.length}
                    className="px-4 py-8 text-center text-white/50"
                  >
                    Loading...
                  </td>
                </tr>
              ) : orders.length === 0 ? (
                <tr>
                  <td
                    colSpan={visibleColumns.length}
                    className="px-4 py-8 text-center text-white/50"
                  >
                    No orders found
                  </td>
                </tr>
              ) : (
                orders.map((order) => (
                  <tr
                    key={order.id}
                    className="border-b border-white/5 hover:bg-white/5 cursor-pointer transition-colors"
                  >
                    {visibleColumns.includes('registration_number') && (
                      <td className="px-4 py-3 text-sm text-white font-medium">
                        {order.registration_number || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('driver_name') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {order.driver_name || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('status') && (
                      <td className="px-4 py-3 text-sm">
                        <span
                          className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                            order.status === 'Active'
                              ? 'bg-brand-green/20 text-brand-green'
                              : order.status === 'Overdue'
                              ? 'bg-red-500/20 text-red-400'
                              : order.status === 'Due'
                              ? 'bg-brand-amber/20 text-brand-amber'
                              : 'bg-white/10 text-white/70'
                          }`}
                        >
                          {order.status || '-'}
                        </span>
                      </td>
                    )}
                    {visibleColumns.includes('object_no') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {order.object_no || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('model') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {order.model || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('make') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {order.make || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('vehicle_type') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {order.vehicle_type || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('order_status') && (
                      <td className="px-4 py-3 text-sm">
                        <span
                          className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                            order.order_status === 'New'
                              ? 'bg-brand-cyan/20 text-brand-cyan'
                              : order.order_status === 'Completed'
                              ? 'bg-brand-green/20 text-brand-green'
                              : order.order_status === 'Cancelled'
                              ? 'bg-red-500/20 text-red-400'
                              : 'bg-brand-amber/20 text-brand-amber'
                          }`}
                        >
                          {order.order_status || '-'}
                        </span>
                      </td>
                    )}
                    {visibleColumns.includes('due_date') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {order.due_date || '-'}
                      </td>
                    )}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div className="flex items-center justify-between px-4 py-3 border-t border-white/10">
          {/* Items per page */}
          <div className="flex items-center space-x-2">
            <span className="text-sm text-white/50">Items per page:</span>
            <div className="flex items-center space-x-1">
              {PAGE_SIZE_OPTIONS.map((size) => (
                <button
                  key={size}
                  onClick={() => {
                    setPageSize(size);
                    setPage(1);
                  }}
                  className={`px-2 py-1 text-sm rounded transition-colors ${
                    pageSize === size
                      ? 'bg-white/20 text-white'
                      : 'text-white/50 hover:text-white hover:bg-white/10'
                  }`}
                >
                  {size}
                </button>
              ))}
            </div>
          </div>

          {/* Page info and navigation */}
          <div className="flex items-center space-x-4">
            <span className="text-sm text-white/50">
              {totalItems > 0
                ? `${(page - 1) * pageSize + 1}-${Math.min(page * pageSize, totalItems)} of ${formatNumber(totalItems)}`
                : '0 items'}
            </span>
            <div className="flex items-center space-x-1">
              <button
                onClick={() => setPage(1)}
                disabled={!canGoPrevious}
                className="p-1 rounded hover:bg-white/10 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                <ChevronsLeft className="h-5 w-5 text-white/70" />
              </button>
              <button
                onClick={() => setPage(page - 1)}
                disabled={!canGoPrevious}
                className="p-1 rounded hover:bg-white/10 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                <ChevronLeft className="h-5 w-5 text-white/70" />
              </button>
              <span className="px-2 text-sm text-white/70">
                Page {page} of {totalPages || 1}
              </span>
              <button
                onClick={() => setPage(page + 1)}
                disabled={!canGoNext}
                className="p-1 rounded hover:bg-white/10 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                <ChevronRight className="h-5 w-5 text-white/70" />
              </button>
              <button
                onClick={() => setPage(totalPages)}
                disabled={!canGoNext}
                className="p-1 rounded hover:bg-white/10 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                <ChevronsRight className="h-5 w-5 text-white/70" />
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
