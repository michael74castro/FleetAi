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

interface FilterOptions {
  makes: string[];
  customers: { id: number; name: string }[];
  order_statuses: { code: number; label: string }[];
  renewal_statuses: { value: string; label: string }[];
}

interface RenewalItem {
  id?: number;
  vehicle_id?: number;
  object_no?: number;
  registration_number?: string;
  driver_name?: string;
  renewal_status?: string;
  make_name?: string;
  model_name?: string;
  make_and_model?: string;
  customer_name?: string;
  days_to_contract_end?: number;
  expected_end_date?: string;
  order_no?: number;
  order_status?: string;
  order_date?: string;
  is_renewal?: number;
  is_new?: number;
}

type FilterType = 'overdue_with_order' | 'overdue_no_order' | 'due_no_order' | 'renewal_orders' | 'new_orders' | 'all';

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

export default function RenewalsOrdersPage() {
  // State
  const [kpis, setKpis] = useState<RenewalsKPIs | null>(null);
  const [items, setItems] = useState<RenewalItem[]>([]);
  const [filterOptions, setFilterOptions] = useState<FilterOptions | null>(null);
  const [loading, setLoading] = useState(true);
  const [tableLoading, setTableLoading] = useState(false);

  // Filter type for API
  const [filterType, setFilterType] = useState<FilterType>('all');
  const [activeKpiTile, setActiveKpiTile] = useState<string | null>(null);

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
    'renewal_status',
    'object_no',
    'make_and_model',
    'days_to_contract_end',
    'order_status'
  ]);

  // Sort
  const [sortBy, setSortBy] = useState('registration_number');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');

  // All available columns
  const allColumns = [
    { key: 'registration_number', label: 'License Plate' },
    { key: 'driver_name', label: 'Driver' },
    { key: 'renewal_status', label: 'Status' },
    { key: 'object_no', label: 'ObjectNo' },
    { key: 'make_and_model', label: 'Make & Model' },
    { key: 'customer_name', label: 'Customer' },
    { key: 'days_to_contract_end', label: 'Days to End' },
    { key: 'expected_end_date', label: 'End Date' },
    { key: 'order_status', label: 'Order Status' },
    { key: 'order_no', label: 'Order No' }
  ];

  // Fetch KPIs and filter options
  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        const [kpisData, filtersData] = await Promise.all([
          api.getRenewalsKPIs(),
          api.getRenewalsFilterOptions()
        ]);
        setKpis(kpisData);
        setFilterOptions(filtersData);
      } catch (error) {
        console.error('Error fetching initial data:', error);
        // Use fallback data
        setKpis({
          overdue_renewals_with_order: 0,
          overdue_renewals_no_order: 0,
          renewals_due_without_order: 0,
          renewal_orders: 0,
          new_orders: 0,
          last_data_update: new Date().toISOString(),
          last_monthly_closure: 'December 2025'
        });
      } finally {
        setLoading(false);
      }
    };
    fetchInitialData();
  }, []);

  // Fetch list data
  useEffect(() => {
    const fetchList = async () => {
      setTableLoading(true);
      try {
        const data = await api.getRenewalsList({
          page,
          page_size: pageSize,
          filter_type: filterType,
          search: driverSearch || licensePlateSearch || undefined,
          sort_by: sortBy,
          sort_order: sortOrder,
          // Advanced filters
          make: makeFilter || undefined,
          renewal_status: statusFilter || undefined,
          order_status_code: orderStatus ? parseInt(orderStatus, 10) : undefined,
        });
        setItems(data.items || []);
        setTotalItems(data.total || 0);
        setTotalPages(data.total_pages || 1);
      } catch (error) {
        console.error('Error fetching renewals list:', error);
        setItems([]);
        setTotalItems(0);
        setTotalPages(1);
      } finally {
        setTableLoading(false);
      }
    };
    fetchList();
  }, [page, pageSize, sortBy, sortOrder, filterType, driverSearch, licensePlateSearch, makeFilter, statusFilter, orderStatus]);

  // Handle KPI tile click
  const handleTileClick = (tile: FilterType, tileName: string) => {
    setFilterType(tile);
    setActiveKpiTile(tileName);
    setPage(1); // Reset to first page when changing filter
  };

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
    setFilterType('all');
    setActiveKpiTile(null);
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
      const blob = await api.exportRenewals({
        filter_type: filterType,
        search: driverSearch || licensePlateSearch || undefined,
        make: makeFilter || undefined,
        renewal_status: statusFilter || undefined,
        order_status_code: orderStatus ? parseInt(orderStatus, 10) : undefined,
        format: 'csv'
      });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      const filename = filterType.includes('order') ? 'orders' : 'renewals';
      a.download = `${filename}_${new Date().toISOString().split('T')[0]}.csv`;
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
        <div
          onClick={() => handleTileClick('overdue_with_order', 'overdue_with_order')}
          className={`glass-panel p-5 transition-colors cursor-pointer group ${
            activeKpiTile === 'overdue_with_order'
              ? 'border-brand-orange ring-2 ring-brand-orange/30'
              : 'hover:border-brand-orange/30'
          }`}
        >
          <div className="flex flex-col items-center text-center">
            <span className={`text-3xl font-bold transition-colors ${
              activeKpiTile === 'overdue_with_order' ? 'text-brand-orange' : 'text-white group-hover:text-brand-orange'
            }`}>
              {loading ? '...' : formatNumber(kpis?.overdue_renewals_with_order || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Overdue Renewals with an order</span>
            <ChevronDown className={`h-4 w-4 mt-2 transition-colors ${
              activeKpiTile === 'overdue_with_order' ? 'text-white/50' : 'text-white/30 group-hover:text-white/50'
            }`} />
          </div>
        </div>

        {/* Overdue Renewals with no order */}
        <div
          onClick={() => handleTileClick('overdue_no_order', 'overdue_no_order')}
          className={`glass-panel p-5 transition-colors cursor-pointer group ${
            activeKpiTile === 'overdue_no_order'
              ? 'border-brand-cyan ring-2 ring-brand-cyan/30'
              : 'hover:border-brand-cyan/30'
          }`}
        >
          <div className="flex flex-col items-center text-center">
            <span className={`text-3xl font-bold transition-colors ${
              activeKpiTile === 'overdue_no_order' ? 'text-brand-cyan' : 'text-white group-hover:text-brand-cyan'
            }`}>
              {loading ? '...' : formatNumber(kpis?.overdue_renewals_no_order || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Overdue Renewals with no order</span>
            <ChevronDown className={`h-4 w-4 mt-2 transition-colors ${
              activeKpiTile === 'overdue_no_order' ? 'text-white/50' : 'text-white/30 group-hover:text-white/50'
            }`} />
          </div>
        </div>

        {/* Renewals Due without order */}
        <div
          onClick={() => handleTileClick('due_no_order', 'due_no_order')}
          className={`glass-panel p-5 transition-colors cursor-pointer group ${
            activeKpiTile === 'due_no_order'
              ? 'border-brand-green ring-2 ring-brand-green/30'
              : 'hover:border-brand-green/30'
          }`}
        >
          <div className="flex flex-col items-center text-center">
            <span className={`text-3xl font-bold transition-colors ${
              activeKpiTile === 'due_no_order' ? 'text-brand-green' : 'text-white group-hover:text-brand-green'
            }`}>
              {loading ? '...' : formatNumber(kpis?.renewals_due_without_order || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Renewals Due without order</span>
            <ChevronDown className={`h-4 w-4 mt-2 transition-colors ${
              activeKpiTile === 'due_no_order' ? 'text-white/50' : 'text-white/30 group-hover:text-white/50'
            }`} />
          </div>
        </div>

        {/* Renewal Orders */}
        <div
          onClick={() => handleTileClick('renewal_orders', 'renewal_orders')}
          className={`glass-panel p-5 transition-colors cursor-pointer group ${
            activeKpiTile === 'renewal_orders'
              ? 'border-brand-amber ring-2 ring-brand-amber/30'
              : 'hover:border-brand-amber/30'
          }`}
        >
          <div className="flex flex-col items-center text-center">
            <span className={`text-3xl font-bold transition-colors ${
              activeKpiTile === 'renewal_orders' ? 'text-brand-amber' : 'text-white group-hover:text-brand-amber'
            }`}>
              {loading ? '...' : formatNumber(kpis?.renewal_orders || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Renewal Orders</span>
            <ChevronDown className={`h-4 w-4 mt-2 transition-colors ${
              activeKpiTile === 'renewal_orders' ? 'text-white/50' : 'text-white/30 group-hover:text-white/50'
            }`} />
          </div>
        </div>

        {/* New Orders */}
        <div
          onClick={() => handleTileClick('new_orders', 'new_orders')}
          className={`glass-panel p-5 transition-colors cursor-pointer group ${
            activeKpiTile === 'new_orders'
              ? 'border-brand-teal ring-2 ring-brand-teal/30'
              : 'hover:border-brand-teal/30'
          }`}
        >
          <div className="flex flex-col items-center text-center">
            <span className={`text-3xl font-bold transition-colors ${
              activeKpiTile === 'new_orders' ? 'text-brand-teal' : 'text-white group-hover:text-brand-teal'
            }`}>
              {loading ? '...' : formatNumber(kpis?.new_orders || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">New Orders</span>
            <ChevronDown className={`h-4 w-4 mt-2 transition-colors ${
              activeKpiTile === 'new_orders' ? 'text-white/50' : 'text-white/30 group-hover:text-white/50'
            }`} />
          </div>
        </div>
      </div>

      {/* Active Filter Indicator */}
      {activeKpiTile && (
        <div className="flex items-center space-x-2">
          <span className="text-sm text-white/50">Filtered by:</span>
          <span className="px-3 py-1 bg-brand-orange/20 text-brand-orange rounded-full text-sm font-medium">
            {activeKpiTile === 'overdue_with_order' && 'Overdue with Order'}
            {activeKpiTile === 'overdue_no_order' && 'Overdue without Order'}
            {activeKpiTile === 'due_no_order' && 'Due without Order'}
            {activeKpiTile === 'renewal_orders' && 'Renewal Orders'}
            {activeKpiTile === 'new_orders' && 'New Orders'}
          </span>
          <button
            onClick={() => {
              setFilterType('all');
              setActiveKpiTile(null);
              setPage(1);
            }}
            className="text-white/50 hover:text-white text-sm underline"
          >
            Clear filter
          </button>
        </div>
      )}

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
                    {filterOptions?.makes.map((make) => (
                      <option key={make} value={make}>{make}</option>
                    ))}
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Renewal Status</label>
                <div className="relative">
                  <select
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Status</option>
                    {filterOptions?.renewal_statuses.map((status) => (
                      <option key={status.value} value={status.value}>{status.label}</option>
                    ))}
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
                    {filterOptions?.order_statuses.map((status) => (
                      <option key={status.code} value={status.code}>{status.label}</option>
                    ))}
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
              ) : items.length === 0 ? (
                <tr>
                  <td
                    colSpan={visibleColumns.length}
                    className="px-4 py-8 text-center text-white/50"
                  >
                    No records found
                  </td>
                </tr>
              ) : (
                items.map((item, index) => (
                  <tr
                    key={item.vehicle_id || item.order_no || index}
                    className="border-b border-white/5 hover:bg-white/5 cursor-pointer transition-colors"
                  >
                    {visibleColumns.includes('registration_number') && (
                      <td className="px-4 py-3 text-sm text-white font-medium">
                        {item.registration_number || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('driver_name') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {item.driver_name || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('renewal_status') && (
                      <td className="px-4 py-3 text-sm">
                        <span
                          className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                            item.renewal_status === 'Active'
                              ? 'bg-brand-green/20 text-brand-green'
                              : item.renewal_status === 'Overdue'
                              ? 'bg-red-500/20 text-red-400'
                              : item.renewal_status === 'Due Soon'
                              ? 'bg-brand-amber/20 text-brand-amber'
                              : item.renewal_status === 'Due'
                              ? 'bg-brand-cyan/20 text-brand-cyan'
                              : 'bg-white/10 text-white/70'
                          }`}
                        >
                          {item.renewal_status || '-'}
                        </span>
                      </td>
                    )}
                    {visibleColumns.includes('object_no') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {item.object_no || item.vehicle_id || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('make_and_model') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {item.make_and_model || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('customer_name') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {item.customer_name || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('days_to_contract_end') && (
                      <td className="px-4 py-3 text-sm">
                        <span
                          className={`font-medium ${
                            item.days_to_contract_end !== undefined && item.days_to_contract_end < 0
                              ? 'text-red-400'
                              : item.days_to_contract_end !== undefined && item.days_to_contract_end <= 30
                              ? 'text-brand-amber'
                              : 'text-white/70'
                          }`}
                        >
                          {item.days_to_contract_end !== undefined ? item.days_to_contract_end : '-'}
                        </span>
                      </td>
                    )}
                    {visibleColumns.includes('expected_end_date') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {item.expected_end_date || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('order_status') && (
                      <td className="px-4 py-3 text-sm">
                        {item.order_status ? (
                          <span
                            className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                              item.order_status === 'Created'
                                ? 'bg-brand-cyan/20 text-brand-cyan'
                                : item.order_status === 'Vehicle Delivered'
                                ? 'bg-brand-green/20 text-brand-green'
                                : item.order_status === 'Cancelled'
                                ? 'bg-red-500/20 text-red-400'
                                : 'bg-brand-amber/20 text-brand-amber'
                            }`}
                          >
                            {item.order_status}
                          </span>
                        ) : (
                          <span className="text-white/30">No order</span>
                        )}
                      </td>
                    )}
                    {visibleColumns.includes('order_no') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {item.order_no || '-'}
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
