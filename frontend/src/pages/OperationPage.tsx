import { useEffect, useState } from 'react';
import { useNavigate, Link, useLocation } from 'react-router-dom';
import {
  ChevronDown,
  ChevronRight,
  Search,
  SlidersHorizontal,
  Settings2,
  Download,
  Info,
  ChevronLeft,
  ChevronsLeft,
  ChevronsRight,
  Heart
} from 'lucide-react';
import { api } from '@/services/api';

interface OperationsKPIs {
  active_within_12_months: number;
  active_vehicles: number;
  started_last_12_months: number;
  terminated_last_12_months: number;
  last_data_update: string;
  last_monthly_closure: string;
}

interface Vehicle {
  vehicle_id: number;
  registration_number: string;
  vin_number: string;
  make_name: string;
  model_name: string;
  make_and_model: string;
  vehicle_year: number;
  color_name: string;
  fuel_type: string;
  customer_name: string;
  driver_name: string;
  driver_email: string;
  driver_phone: string;
  vehicle_status: string;
  current_odometer_km: number;
  monthly_lease_amount: number;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

export default function OperationPage() {
  const navigate = useNavigate();
  const location = useLocation();

  // Determine current page from URL
  const getCurrentPage = () => {
    if (location.pathname.includes('/renewals')) return 'renewals';
    if (location.pathname.includes('/service')) return 'service';
    return 'fleet';
  };
  const currentPage = getCurrentPage();

  // State
  const [kpis, setKpis] = useState<OperationsKPIs | null>(null);
  const [vehicles, setVehicles] = useState<Vehicle[]>([]);
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
  const [customerNo, setCustomerNo] = useState('');
  const [contractNumber, setContractNumber] = useState('');
  const [costCentre, setCostCentre] = useState('');
  const [makeFilter, setMakeFilter] = useState('');
  const [totalMileageFrom, setTotalMileageFrom] = useState('');
  const [totalMileageTo, setTotalMileageTo] = useState('');
  const [statusCode, setStatusCode] = useState(false);
  const [electricVehicle, setElectricVehicle] = useState(false);
  const [startedWithin12Months, setStartedWithin12Months] = useState(false);
  const [terminatedWithin12Months, setTerminatedWithin12Months] = useState(false);
  const [wasActiveIn12Months, setWasActiveIn12Months] = useState(false);

  // View configuration
  const [currentView, setCurrentView] = useState('View 2');
  const [showColumnConfig, setShowColumnConfig] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState<string[]>([
    'registration_number',
    'driver_name',
    'make_and_model',
    'customer_name',
    'vehicle_status',
    'current_odometer_km',
    'vehicle_year',
    'color_name',
    'fuel_type',
    'monthly_lease_amount',
    'vin_number',
    'driver_email',
    'driver_phone'
  ]);

  // Sort
  const [sortBy, setSortBy] = useState('registration_number');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');

  // All available columns
  const allColumns = [
    { key: 'registration_number', label: 'License Plate' },
    { key: 'driver_name', label: 'Driver' },
    { key: 'make_and_model', label: 'Make/Model' },
    { key: 'customer_name', label: 'Customer' },
    { key: 'vehicle_status', label: 'Status' },
    { key: 'current_odometer_km', label: 'Odometer (km)' },
    { key: 'vehicle_year', label: 'Year' },
    { key: 'color_name', label: 'Color' },
    { key: 'fuel_type', label: 'Fuel Type' },
    { key: 'monthly_lease_amount', label: 'Monthly Rate' },
    { key: 'vin_number', label: 'VIN' },
    { key: 'driver_email', label: 'Driver Email' },
    { key: 'driver_phone', label: 'Driver Phone' }
  ];

  // Fetch KPIs
  useEffect(() => {
    const fetchKPIs = async () => {
      try {
        const data = await api.getOperationsKPIs();
        setKpis(data);
      } catch (error) {
        console.error('Failed to fetch KPIs:', error);
      } finally {
        setLoading(false);
      }
    };
    fetchKPIs();
  }, []);

  // Active status filter
  const [activeStatusFilter, setActiveStatusFilter] = useState('Active');

  // Fetch vehicles
  useEffect(() => {
    const fetchVehicles = async () => {
      setTableLoading(true);
      try {
        const data = await api.getVehiclesList({
          page,
          page_size: pageSize,
          driver: driverSearch || undefined,
          license_plate: licensePlateSearch || undefined,
          make: makeFilter || undefined,
          status: activeStatusFilter || undefined,
          sort_by: sortBy,
          sort_order: sortOrder
        });
        setVehicles(data.items || []);
        setTotalItems(data.total || 0);
        setTotalPages(data.total_pages || 0);
      } catch (error) {
        console.error('Failed to fetch vehicles:', error);
        setVehicles([]);
        setTotalItems(0);
        setTotalPages(0);
      } finally {
        setTableLoading(false);
      }
    };
    fetchVehicles();
  }, [page, pageSize, sortBy, sortOrder, activeStatusFilter, driverSearch, licensePlateSearch, makeFilter]);

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
    setCustomerNo('');
    setContractNumber('');
    setCostCentre('');
    setMakeFilter('');
    setTotalMileageFrom('');
    setTotalMileageTo('');
    setStatusCode(false);
    setElectricVehicle(false);
    setStartedWithin12Months(false);
    setTerminatedWithin12Months(false);
    setWasActiveIn12Months(false);
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
      a.download = `fleet_vehicles_${new Date().toISOString().split('T')[0]}.xlsx`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (error) {
      console.error('Export failed:', error);
    }
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
        <span className="text-white/60">
          {currentPage === 'renewals' ? 'Renewals & orders' : currentPage === 'service' ? 'Service & MOT' : 'Fleet'}
        </span>
      </nav>

      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <h1 className="text-2xl font-bold text-white">
            Operation - {currentPage === 'renewals' ? 'Renewals & orders' : currentPage === 'service' ? 'Service & MOT' : 'Fleet'}
          </h1>
          <button className="text-white/40 hover:text-white/60 transition-colors">
            <Info className="h-5 w-5" />
          </button>
        </div>
        <p className="text-sm text-white/50">
          Last data update: {kpis ? formatDate(kpis.last_data_update) : '...'} | Last monthly closure: {kpis?.last_monthly_closure || '...'}
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Active within last 12 months */}
        <div
          onClick={() => { setActiveStatusFilter('active_within_12m'); setPage(1); }}
          className={`glass-panel p-6 hover:border-brand-orange/30 transition-colors cursor-pointer group ${activeStatusFilter === 'active_within_12m' ? 'border-brand-orange/50' : ''}`}
        >
          <div className="flex flex-col items-center text-center">
            <span className="text-4xl font-bold text-white group-hover:text-brand-orange transition-colors">
              {loading ? '...' : formatNumber(kpis?.active_within_12_months || 0)}
            </span>
            <span className="text-sm text-white/50 mt-2">Active within last 12 months</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Active vehicles */}
        <div
          onClick={() => { setActiveStatusFilter('Active'); setPage(1); }}
          className={`glass-panel p-6 hover:border-brand-cyan/30 transition-colors cursor-pointer group ${activeStatusFilter === 'Active' ? 'border-brand-cyan/50' : ''}`}
        >
          <div className="flex flex-col items-center text-center">
            <span className="text-4xl font-bold text-white group-hover:text-brand-cyan transition-colors">
              {loading ? '...' : formatNumber(kpis?.active_vehicles || 0)}
            </span>
            <span className="text-sm text-white/50 mt-2">Active vehicles</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Started in last 12 months */}
        <div
          onClick={() => { setActiveStatusFilter('started_12m'); setPage(1); }}
          className={`glass-panel p-6 hover:border-brand-green/30 transition-colors cursor-pointer group ${activeStatusFilter === 'started_12m' ? 'border-brand-green/50' : ''}`}
        >
          <div className="flex flex-col items-center text-center">
            <span className="text-4xl font-bold text-white group-hover:text-brand-green transition-colors">
              {loading ? '...' : formatNumber(kpis?.started_last_12_months || 0)}
            </span>
            <span className="text-sm text-white/50 mt-2">Started in last 12 months</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Terminated in last 12 months */}
        <div
          onClick={() => { setActiveStatusFilter('terminated_12m'); setPage(1); }}
          className={`glass-panel p-6 hover:border-brand-amber/30 transition-colors cursor-pointer group ${activeStatusFilter === 'terminated_12m' ? 'border-brand-amber/50' : ''}`}
        >
          <div className="flex flex-col items-center text-center">
            <span className="text-4xl font-bold text-white group-hover:text-brand-amber transition-colors">
              {loading ? '...' : formatNumber(kpis?.terminated_last_12_months || 0)}
            </span>
            <span className="text-sm text-white/50 mt-2">Terminated in last 12 months</span>
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
                <option value="active">Active Vehicles</option>
                <option value="terminated">Terminated Vehicles</option>
                <option value="high-mileage">High Mileage</option>
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
            {/* Row 1: Company Name, Customer No, Contract Number */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
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
                <label className="block text-sm font-medium text-white/70 mb-2">Customer No</label>
                <input
                  type="text"
                  value={customerNo}
                  onChange={(e) => setCustomerNo(e.target.value)}
                  placeholder=""
                  className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Contract Number</label>
                <input
                  type="text"
                  value={contractNumber}
                  onChange={(e) => setContractNumber(e.target.value)}
                  placeholder=""
                  className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                />
              </div>
            </div>

            {/* Row 2: Cost Centre, Make, Total mileage */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
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
                <label className="block text-sm font-medium text-white/70 mb-2">Total mileage</label>
                <div className="flex items-center space-x-2">
                  <input
                    type="number"
                    value={totalMileageFrom}
                    onChange={(e) => setTotalMileageFrom(e.target.value)}
                    placeholder=""
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                  <span className="text-white/50">and</span>
                  <input
                    type="number"
                    value={totalMileageTo}
                    onChange={(e) => setTotalMileageTo(e.target.value)}
                    placeholder=""
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                </div>
              </div>
            </div>

            {/* Row 3: Checkboxes */}
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={statusCode}
                  onChange={(e) => setStatusCode(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Status Code</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={electricVehicle}
                  onChange={(e) => setElectricVehicle(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Electric Vehicle</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={startedWithin12Months}
                  onChange={(e) => setStartedWithin12Months(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Started within last 12 months</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={terminatedWithin12Months}
                  onChange={(e) => setTerminatedWithin12Months(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Terminated within last 12 months</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={wasActiveIn12Months}
                  onChange={(e) => setWasActiveIn12Months(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Was Active In Last 12 Months</span>
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

        {/* Download */}
        <button
          onClick={handleExport}
          className="flex items-center space-x-2 text-white/50 hover:text-white transition-colors"
        >
          <Download className="h-4 w-4" />
          <span className="text-sm">Download Table</span>
        </button>
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
              ) : vehicles.length === 0 ? (
                <tr>
                  <td
                    colSpan={visibleColumns.length}
                    className="px-4 py-8 text-center text-white/50"
                  >
                    No vehicles found
                  </td>
                </tr>
              ) : (
                vehicles.map((vehicle) => (
                  <tr
                    key={vehicle.vehicle_id}
                    onClick={() => navigate(`/operation/vehicle/${vehicle.vehicle_id}`)}
                    className="border-b border-white/5 hover:bg-white/5 cursor-pointer transition-colors"
                  >
                    {visibleColumns.includes('registration_number') && (
                      <td className="px-4 py-3 text-sm text-white font-medium">
                        {vehicle.registration_number || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('driver_name') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.driver_name || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('make_and_model') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.make_and_model || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('customer_name') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.customer_name || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('vehicle_status') && (
                      <td className="px-4 py-3 text-sm">
                        <span
                          className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                            vehicle.vehicle_status === 'Active'
                              ? 'bg-brand-green/20 text-brand-green'
                              : 'bg-brand-amber/20 text-brand-amber'
                          }`}
                        >
                          {vehicle.vehicle_status || '-'}
                        </span>
                      </td>
                    )}
                    {visibleColumns.includes('current_odometer_km') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.current_odometer_km
                          ? formatNumber(vehicle.current_odometer_km)
                          : '-'}
                      </td>
                    )}
                    {visibleColumns.includes('vehicle_year') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.vehicle_year || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('color_name') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.color_name || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('fuel_type') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.fuel_type || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('monthly_lease_amount') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.monthly_lease_amount
                          ? `AED ${formatNumber(vehicle.monthly_lease_amount)}`
                          : '-'}
                      </td>
                    )}
                    {visibleColumns.includes('vin_number') && (
                      <td className="px-4 py-3 text-sm text-white/70 font-mono text-xs">
                        {vehicle.vin_number || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('driver_email') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.driver_email || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('driver_phone') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {vehicle.driver_phone || '-'}
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
