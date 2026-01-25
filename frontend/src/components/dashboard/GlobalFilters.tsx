import { useState, useEffect } from 'react';
import { Filter, X } from 'lucide-react';
import { useDashboardStore } from '@/store/dashboardStore';
import { api } from '@/services/api';

export default function GlobalFilters() {
  const { globalFilters, setGlobalFilter, clearGlobalFilters } = useDashboardStore();
  const [customers, setCustomers] = useState<{ value: string; label: string }[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [showFilters, setShowFilters] = useState(false);

  useEffect(() => {
    loadCustomers();
  }, []);

  const loadCustomers = async () => {
    setIsLoading(true);
    try {
      const response = await api.getDistinctValues('dim_customer', 'customer_name', { limit: 100 });
      setCustomers(response.values?.map((v: string) => ({ value: v, label: v })) || []);
    } catch (error) {
      console.error('Failed to load customers:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const activeFilterCount = Object.keys(globalFilters).filter(
    (k) => globalFilters[k] !== undefined && globalFilters[k] !== ''
  ).length;

  return (
    <div className="relative">
      <button
        onClick={() => setShowFilters(!showFilters)}
        className={`flex items-center space-x-2 rounded-md border px-3 py-2 text-sm hover:bg-muted ${
          activeFilterCount > 0 ? 'border-primary' : ''
        }`}
      >
        <Filter className="h-4 w-4" />
        <span>Filters</span>
        {activeFilterCount > 0 && (
          <span className="flex h-5 w-5 items-center justify-center rounded-full bg-primary text-xs text-primary-foreground">
            {activeFilterCount}
          </span>
        )}
      </button>

      {showFilters && (
        <div className="absolute left-0 top-full mt-2 w-72 rounded-lg border bg-card p-4 shadow-lg z-10">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-medium">Filters</h3>
            {activeFilterCount > 0 && (
              <button
                onClick={clearGlobalFilters}
                className="text-xs text-muted-foreground hover:text-foreground"
              >
                Clear all
              </button>
            )}
          </div>

          <div className="space-y-4">
            {/* Customer filter */}
            <div>
              <label className="text-sm font-medium">Customer</label>
              <select
                value={(globalFilters.customer_id as string) || ''}
                onChange={(e) => setGlobalFilter('customer_id', e.target.value || undefined)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                disabled={isLoading}
              >
                <option value="">All Customers</option>
                {customers.map((customer) => (
                  <option key={customer.value} value={customer.value}>
                    {customer.label}
                  </option>
                ))}
              </select>
            </div>

            {/* Status filter */}
            <div>
              <label className="text-sm font-medium">Status</label>
              <select
                value={(globalFilters.status as string) || ''}
                onChange={(e) => setGlobalFilter('status', e.target.value || undefined)}
                className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              >
                <option value="">All Statuses</option>
                <option value="active">Active</option>
                <option value="inactive">Inactive</option>
                <option value="pending">Pending</option>
              </select>
            </div>
          </div>

          <button
            onClick={() => setShowFilters(false)}
            className="mt-4 w-full rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
          >
            Apply Filters
          </button>
        </div>
      )}
    </div>
  );
}
