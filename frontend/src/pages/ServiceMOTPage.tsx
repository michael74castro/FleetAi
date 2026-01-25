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
import { cn } from '@/lib/utils';

interface ServiceKPIs {
  late_service_mots_ytd: number;
  overdue_without_booking: number;
  overdue_with_booking: number;
  due_mots_3_months_no_booking: number;
  last_data_update: string;
  last_monthly_closure: string;
}

interface ServiceRecord {
  id: number;
  registration_number: string;
  make_model: string;
  driver_name: string;
  status: string;
  mot_due_date: string;
  service_due_date: string;
  maintenance_type: string;
  booking_status: string;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

export default function ServiceMOTPage() {
  // State
  const [kpis, setKpis] = useState<ServiceKPIs | null>(null);
  const [records, setRecords] = useState<ServiceRecord[]>([]);
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

  // Advanced filters - Dropdowns
  const [companyName, setCompanyName] = useState('');
  const [costCentre, setCostCentre] = useState('');
  const [serviceBookingChannel, setServiceBookingChannel] = useState('');
  const [serviceTyreType, setServiceTyreType] = useState('');
  const [fuelType, setFuelType] = useState('');
  const [productType, setProductType] = useState('');
  const [makeFilter, setMakeFilter] = useState('');
  const [maintenanceStopType, setMaintenanceStopType] = useState('');
  const [serviceType, setServiceType] = useState('');
  const [statusFilter, setStatusFilter] = useState('');

  // Advanced filters - Date ranges
  const [motDueDateFrom, setMotDueDateFrom] = useState('');
  const [motDueDateTo, setMotDueDateTo] = useState('');
  const [maintenanceDueDateFrom, setMaintenanceDueDateFrom] = useState('');
  const [maintenanceDueDateTo, setMaintenanceDueDateTo] = useState('');
  const [maintenanceBookedDateFrom, setMaintenanceBookedDateFrom] = useState('');
  const [maintenanceBookedDateTo, setMaintenanceBookedDateTo] = useState('');
  const [maintenanceStopDateFrom, setMaintenanceStopDateFrom] = useState('');
  const [maintenanceStopDateTo, setMaintenanceStopDateTo] = useState('');
  const [daysOffRoadFrom, setDaysOffRoadFrom] = useState('');
  const [daysOffRoadTo, setDaysOffRoadTo] = useState('');
  const [tyreFittingRechargesFrom, setTyreFittingRechargesFrom] = useState('');
  const [tyreFittingRechargesTo, setTyreFittingRechargesTo] = useState('');

  // Checkboxes
  const [serviceMOTWasLate, setServiceMOTWasLate] = useState(false);
  const [lateServiceMOTYTD, setLateServiceMOTYTD] = useState(false);
  const [maintenanceOverdue, setMaintenanceOverdue] = useState(false);
  const [motDue, setMotDue] = useState(false);
  const [motDue3MonthsNoBooking, setMotDue3MonthsNoBooking] = useState(false);
  const [motOverdue, setMotOverdue] = useState(false);
  const [overdueWithBooking, setOverdueWithBooking] = useState(false);
  const [overdueWithoutBooking, setOverdueWithoutBooking] = useState(false);
  const [serviceDue, setServiceDue] = useState(false);
  const [serviceOverdue, setServiceOverdue] = useState(false);
  const [noBookings, setNoBookings] = useState(false);
  const [lateWinterSummerTyre, setLateWinterSummerTyre] = useState(false);
  const [tyreSwitchOverdue, setTyreSwitchOverdue] = useState(false);

  // View configuration
  const [currentView, setCurrentView] = useState('View 1');
  const [showColumnConfig, setShowColumnConfig] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState<string[]>([
    'registration_number',
    'make_model',
    'driver_name',
    'status'
  ]);

  // Sort
  const [sortBy, setSortBy] = useState('registration_number');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');

  // All available columns
  const allColumns = [
    { key: 'registration_number', label: 'License Plate' },
    { key: 'make_model', label: 'Make/Model' },
    { key: 'driver_name', label: 'Driver' },
    { key: 'status', label: 'Status' },
    { key: 'mot_due_date', label: 'MOT Due Date' },
    { key: 'service_due_date', label: 'Service Due Date' },
    { key: 'maintenance_type', label: 'Maintenance Type' },
    { key: 'booking_status', label: 'Booking Status' }
  ];

  // Fetch KPIs
  useEffect(() => {
    const fetchKPIs = async () => {
      try {
        // TODO: Replace with actual API call
        throw new Error('API not implemented');
      } catch {
        // Use fallback data
        setKpis({
          late_service_mots_ytd: 0,
          overdue_without_booking: 0,
          overdue_with_booking: 0,
          due_mots_3_months_no_booking: 0,
          last_data_update: '2026-01-24T04:33:32Z',
          last_monthly_closure: 'December 2025'
        });
      } finally {
        setLoading(false);
      }
    };
    fetchKPIs();
  }, []);

  // Fetch records
  useEffect(() => {
    const fetchRecords = async () => {
      setTableLoading(true);
      try {
        // TODO: Replace with actual API call
        throw new Error('API not implemented');
      } catch {
        // Use fallback mock data
        const mockRecords: ServiceRecord[] = Array.from({ length: pageSize }, (_, i) => ({
          id: i + 1 + (page - 1) * pageSize,
          registration_number: `UAE-${String(30000 + i + (page - 1) * pageSize).padStart(5, '0')}`,
          make_model: ['Toyota Camry', 'Nissan Patrol', 'Mitsubishi Pajero', 'Honda Accord', 'Ford Explorer'][i % 5],
          driver_name: `Driver ${i + 1 + (page - 1) * pageSize}`,
          status: ['Active', 'Due', 'Overdue', 'Booked'][i % 4],
          mot_due_date: new Date(Date.now() + (i - 5) * 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
          service_due_date: new Date(Date.now() + (i - 3) * 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
          maintenance_type: ['Regular Service', 'MOT', 'Tyre Change', 'Oil Change'][i % 4],
          booking_status: ['Booked', 'Not Booked', 'Pending', 'Completed'][i % 4]
        }));
        setRecords(mockRecords);
        setTotalItems(500);
        setTotalPages(Math.ceil(500 / pageSize));
      } finally {
        setTableLoading(false);
      }
    };
    fetchRecords();
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
    setServiceBookingChannel('');
    setServiceTyreType('');
    setFuelType('');
    setProductType('');
    setMakeFilter('');
    setMaintenanceStopType('');
    setServiceType('');
    setStatusFilter('');
    setMotDueDateFrom('');
    setMotDueDateTo('');
    setMaintenanceDueDateFrom('');
    setMaintenanceDueDateTo('');
    setMaintenanceBookedDateFrom('');
    setMaintenanceBookedDateTo('');
    setMaintenanceStopDateFrom('');
    setMaintenanceStopDateTo('');
    setDaysOffRoadFrom('');
    setDaysOffRoadTo('');
    setTyreFittingRechargesFrom('');
    setTyreFittingRechargesTo('');
    setServiceMOTWasLate(false);
    setLateServiceMOTYTD(false);
    setMaintenanceOverdue(false);
    setMotDue(false);
    setMotDue3MonthsNoBooking(false);
    setMotOverdue(false);
    setOverdueWithBooking(false);
    setOverdueWithoutBooking(false);
    setServiceDue(false);
    setServiceOverdue(false);
    setNoBookings(false);
    setLateWinterSummerTyre(false);
    setTyreSwitchOverdue(false);
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
    // TODO: Implement export
    console.log('Export clicked');
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
        <span className="text-white/60">Service & MOT</span>
      </nav>

      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <h1 className="text-2xl font-bold text-white">Operation - Service & MOT</h1>
          <button className="text-white/40 hover:text-white/60 transition-colors">
            <Info className="h-5 w-5" />
          </button>
        </div>
        <p className="text-sm text-white/50">
          Last data update: {kpis ? formatDate(kpis.last_data_update) : '...'} | Last monthly closure: {kpis?.last_monthly_closure || '...'}
        </p>
      </div>

      {/* KPI Cards - 4 tiles */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {/* No. Late Service/MOTs YTD */}
        <div className="glass-panel p-5 hover:border-brand-orange/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-orange transition-colors">
              {loading ? '...' : formatNumber(kpis?.late_service_mots_ytd || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">No. Late Service/MOTs YTD</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Overdue/late for Service/MOT w/o booking */}
        <div className="glass-panel p-5 hover:border-brand-cyan/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-cyan transition-colors">
              {loading ? '...' : formatNumber(kpis?.overdue_without_booking || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Overdue/late for Service/MOT w/o booking</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Overdue/late for Service/MOT w/ booking */}
        <div className="glass-panel p-5 hover:border-brand-green/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-green transition-colors">
              {loading ? '...' : formatNumber(kpis?.overdue_with_booking || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Overdue/late for Service/MOT w/ booking</span>
            <ChevronDown className="h-4 w-4 text-white/30 mt-2 group-hover:text-white/50 transition-colors" />
          </div>
        </div>

        {/* Due MOT's in next 3 mo. w/o booking */}
        <div className="glass-panel p-5 hover:border-brand-amber/30 transition-colors cursor-pointer group">
          <div className="flex flex-col items-center text-center">
            <span className="text-3xl font-bold text-white group-hover:text-brand-amber transition-colors">
              {loading ? '...' : formatNumber(kpis?.due_mots_3_months_no_booking || 0)}
            </span>
            <span className="text-xs text-white/50 mt-2">Due MOT's in next 3 mo. w/o booking</span>
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
                <option value="overdue">Overdue Services</option>
                <option value="mot-due">MOT Due</option>
                <option value="no-booking">No Booking</option>
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
            {/* Row 1: Company Name, Cost Centre, blank, MOT Due Date */}
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
              <div></div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">MOT Due Date</label>
                <div className="flex items-center space-x-2">
                  <input
                    type="date"
                    value={motDueDateFrom}
                    onChange={(e) => setMotDueDateFrom(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                  <span className="text-white/50 text-sm">and</span>
                  <input
                    type="date"
                    value={motDueDateTo}
                    onChange={(e) => setMotDueDateTo(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                </div>
              </div>
            </div>

            {/* Row 2: Maintenance Due Date, Tyre fitting recharges, blank, Maintenance Booked Date */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Maintenance Due Date</label>
                <div className="flex items-center space-x-2">
                  <input
                    type="date"
                    value={maintenanceDueDateFrom}
                    onChange={(e) => setMaintenanceDueDateFrom(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                  <span className="text-white/50 text-sm">and</span>
                  <input
                    type="date"
                    value={maintenanceDueDateTo}
                    onChange={(e) => setMaintenanceDueDateTo(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Tyre fitting recharges</label>
                <div className="flex items-center space-x-2">
                  <input
                    type="number"
                    value={tyreFittingRechargesFrom}
                    onChange={(e) => setTyreFittingRechargesFrom(e.target.value)}
                    placeholder="From"
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                  <span className="text-white/50 text-sm">and</span>
                  <input
                    type="number"
                    value={tyreFittingRechargesTo}
                    onChange={(e) => setTyreFittingRechargesTo(e.target.value)}
                    placeholder="To"
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                </div>
              </div>
              <div></div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Maintenance Booked Date</label>
                <div className="flex items-center space-x-2">
                  <input
                    type="date"
                    value={maintenanceBookedDateFrom}
                    onChange={(e) => setMaintenanceBookedDateFrom(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                  <span className="text-white/50 text-sm">and</span>
                  <input
                    type="date"
                    value={maintenanceBookedDateTo}
                    onChange={(e) => setMaintenanceBookedDateTo(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                </div>
              </div>
            </div>

            {/* Row 3: Maintenance stop date, No. days off road, blank, Service booking channel */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Maintenance stop date</label>
                <div className="flex items-center space-x-2">
                  <input
                    type="date"
                    value={maintenanceStopDateFrom}
                    onChange={(e) => setMaintenanceStopDateFrom(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                  <span className="text-white/50 text-sm">and</span>
                  <input
                    type="date"
                    value={maintenanceStopDateTo}
                    onChange={(e) => setMaintenanceStopDateTo(e.target.value)}
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">No. days off road</label>
                <div className="flex items-center space-x-2">
                  <input
                    type="number"
                    value={daysOffRoadFrom}
                    onChange={(e) => setDaysOffRoadFrom(e.target.value)}
                    placeholder="From"
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                  <span className="text-white/50 text-sm">and</span>
                  <input
                    type="number"
                    value={daysOffRoadTo}
                    onChange={(e) => setDaysOffRoadTo(e.target.value)}
                    placeholder="To"
                    className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2.5 text-white placeholder-white/30 focus:outline-none focus:border-brand-orange/50 transition-colors"
                  />
                </div>
              </div>
              <div></div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Service booking channel</label>
                <div className="relative">
                  <select
                    value={serviceBookingChannel}
                    onChange={(e) => setServiceBookingChannel(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Channels</option>
                    <option value="online">Online</option>
                    <option value="phone">Phone</option>
                    <option value="dealer">Dealer</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
            </div>

            {/* Row 4: Service tyre type, Fuel Type, blank, Product Type */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Service tyre type</label>
                <div className="relative">
                  <select
                    value={serviceTyreType}
                    onChange={(e) => setServiceTyreType(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Types</option>
                    <option value="summer">Summer</option>
                    <option value="winter">Winter</option>
                    <option value="all-season">All Season</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Fuel Type</label>
                <div className="relative">
                  <select
                    value={fuelType}
                    onChange={(e) => setFuelType(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Fuel Types</option>
                    <option value="petrol">Petrol</option>
                    <option value="diesel">Diesel</option>
                    <option value="electric">Electric</option>
                    <option value="hybrid">Hybrid</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div></div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Product Type</label>
                <div className="relative">
                  <select
                    value={productType}
                    onChange={(e) => setProductType(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Products</option>
                    <option value="lease">Lease</option>
                    <option value="fleet">Fleet</option>
                    <option value="rental">Rental</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
            </div>

            {/* Row 5: Make, Maintenance stop type, blank, Service type */}
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
                <label className="block text-sm font-medium text-white/70 mb-2">Maintenance stop type</label>
                <div className="relative">
                  <select
                    value={maintenanceStopType}
                    onChange={(e) => setMaintenanceStopType(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Types</option>
                    <option value="scheduled">Scheduled</option>
                    <option value="unscheduled">Unscheduled</option>
                    <option value="breakdown">Breakdown</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div></div>
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Service type</label>
                <div className="relative">
                  <select
                    value={serviceType}
                    onChange={(e) => setServiceType(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Service Types</option>
                    <option value="full">Full Service</option>
                    <option value="interim">Interim Service</option>
                    <option value="mot">MOT Only</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
            </div>

            {/* Row 6: Status, Late for winter/summer tyre fitting, blank, Tyre switch overdue */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-white/70 mb-2">Status</label>
                <div className="relative">
                  <select
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    className="w-full bg-white/5 border border-white/10 rounded-lg px-4 py-2.5 text-white appearance-none focus:outline-none focus:border-brand-orange/50 transition-colors"
                  >
                    <option value="">All Status</option>
                    <option value="active">Active</option>
                    <option value="due">Due</option>
                    <option value="overdue">Overdue</option>
                    <option value="booked">Booked</option>
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-white/50 pointer-events-none" />
                </div>
              </div>
              <div className="flex items-end pb-2">
                <label className="flex items-center space-x-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={lateWinterSummerTyre}
                    onChange={(e) => setLateWinterSummerTyre(e.target.checked)}
                    className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                  />
                  <span className="text-sm text-white/70">Late for winter/summer tyre fitting</span>
                </label>
              </div>
              <div></div>
              <div className="flex items-end pb-2">
                <label className="flex items-center space-x-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={tyreSwitchOverdue}
                    onChange={(e) => setTyreSwitchOverdue(e.target.checked)}
                    className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                  />
                  <span className="text-sm text-white/70">Tyre switch overdue</span>
                </label>
              </div>
            </div>

            {/* Checkboxes Grid */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 pt-2">
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={serviceMOTWasLate}
                  onChange={(e) => setServiceMOTWasLate(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Service/MOT was late</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={lateServiceMOTYTD}
                  onChange={(e) => setLateServiceMOTYTD(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Late Service/MOT YTD</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={maintenanceOverdue}
                  onChange={(e) => setMaintenanceOverdue(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Maintenance overdue</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={motDue}
                  onChange={(e) => setMotDue(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">MOT due</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={motDue3MonthsNoBooking}
                  onChange={(e) => setMotDue3MonthsNoBooking(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">MOT due by 3 months w/o booking</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={motOverdue}
                  onChange={(e) => setMotOverdue(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">MOT overdue</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={overdueWithBooking}
                  onChange={(e) => setOverdueWithBooking(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Overdue/late for Service/MOT w/ booking</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={overdueWithoutBooking}
                  onChange={(e) => setOverdueWithoutBooking(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Overdue/late for Service/MOT w/o booking</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={serviceDue}
                  onChange={(e) => setServiceDue(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Service due</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={serviceOverdue}
                  onChange={(e) => setServiceOverdue(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">Service overdue</span>
              </label>
              <label className="flex items-center space-x-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={noBookings}
                  onChange={(e) => setNoBookings(e.target.checked)}
                  className="w-4 h-4 rounded border-white/30 bg-white/5 text-brand-orange focus:ring-brand-orange/50"
                />
                <span className="text-sm text-white/70">No. bookings</span>
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
                className={cn(
                  'px-3 py-1.5 rounded-full text-sm transition-colors',
                  visibleColumns.includes(col.key)
                    ? 'bg-brand-orange text-white'
                    : 'bg-white/5 text-white/50 hover:bg-white/10'
                )}
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
                            className={cn(
                              'h-4 w-4 transition-transform',
                              sortOrder === 'desc' ? 'rotate-180' : ''
                            )}
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
              ) : records.length === 0 ? (
                <tr>
                  <td
                    colSpan={visibleColumns.length}
                    className="px-4 py-8 text-center text-white/50"
                  >
                    No records found
                  </td>
                </tr>
              ) : (
                records.map((record) => (
                  <tr
                    key={record.id}
                    className="border-b border-white/5 hover:bg-white/5 cursor-pointer transition-colors"
                  >
                    {visibleColumns.includes('registration_number') && (
                      <td className="px-4 py-3 text-sm text-white font-medium">
                        {record.registration_number || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('make_model') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {record.make_model || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('driver_name') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {record.driver_name || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('status') && (
                      <td className="px-4 py-3 text-sm">
                        <span
                          className={cn(
                            'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium',
                            record.status === 'Active'
                              ? 'bg-brand-green/20 text-brand-green'
                              : record.status === 'Overdue'
                              ? 'bg-red-500/20 text-red-400'
                              : record.status === 'Due'
                              ? 'bg-brand-amber/20 text-brand-amber'
                              : 'bg-brand-cyan/20 text-brand-cyan'
                          )}
                        >
                          {record.status || '-'}
                        </span>
                      </td>
                    )}
                    {visibleColumns.includes('mot_due_date') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {record.mot_due_date || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('service_due_date') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {record.service_due_date || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('maintenance_type') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {record.maintenance_type || '-'}
                      </td>
                    )}
                    {visibleColumns.includes('booking_status') && (
                      <td className="px-4 py-3 text-sm text-white/70">
                        {record.booking_status || '-'}
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
                  className={cn(
                    'px-2 py-1 text-sm rounded transition-colors',
                    pageSize === size
                      ? 'bg-white/20 text-white'
                      : 'text-white/50 hover:text-white hover:bg-white/10'
                  )}
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
