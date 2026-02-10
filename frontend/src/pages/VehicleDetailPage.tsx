import { useEffect, useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { ChevronRight, ArrowLeft } from 'lucide-react';
import { api } from '@/services/api';

interface GaugeData {
  value: number;
  target: number;
  percentage: number;
}

interface VehicleDetail {
  vehicle_id: number;
  object_no: number;
  registration_number: string;
  vin_number: string;
  make_name: string;
  model_name: string;
  make_and_model: string;
  vehicle_year: number | null;
  color_name: string | null;
  body_type: string | null;
  fuel_type: string | null;
  vehicle_status: string | null;
  vehicle_status_code: number | null;
  is_active: number;
  lease_type: string | null;
  lease_type_description: string | null;
  purchase_price: number | null;
  residual_value: number | null;
  monthly_lease_amount: number | null;
  lease_duration_months: number | null;
  annual_km_allowance: number | null;
  current_odometer_km: number | null;
  last_odometer_date: string | null;
  lease_start_date: string | null;
  lease_end_date: string | null;
  expected_end_date: string | null;
  months_driven: number | null;
  months_remaining: number | null;
  days_to_contract_end: number | null;
  customer_id: number | null;
  customer_name: string | null;
  contract_position_number: number | null;
  driver_name: string | null;
  driver_first_name: string | null;
  driver_last_name: string | null;
  driver_email: string | null;
  driver_mobile: string | null;
  contract_number: number | null;
  monthly_rate_total: number | null;
  cost_centre: string | null;
  company_name: string | null;
  start_mileage: number | null;
  renewal_order_no: number | null;
  renewal_status: string | null;
  gauge_duration: GaugeData;
  gauge_km: GaugeData;
}

function formatDate(dateString: string | null | undefined): string | null {
  if (!dateString) return null;
  try {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });
  } catch (_e) {
    return dateString;
  }
}

function formatNumber(num: number | null | undefined): string | null {
  if (num == null) return null;
  return num.toLocaleString();
}

function formatCurrency(num: number | null | undefined): string | null {
  if (num == null) return null;
  return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

// Detail row component
function DetailRow({ label, value }: { label: string; value: string | number | null | undefined }) {
  const displayValue = value != null && value !== '' ? String(value) : null;

  return (
    <div className="flex items-start py-2.5 border-b border-white/5">
      <span className="w-[280px] shrink-0 text-sm text-white/50">{label}:</span>
      {displayValue ? (
        <span className="text-sm text-white font-medium">{displayValue}</span>
      ) : (
        <span className="text-sm text-white/25 italic">(not available)</span>
      )}
    </div>
  );
}

// Gauge component matching the design's colorful speedometer style
function GaugeChart({ data, label, sublabel }: { data: GaugeData; label: string; sublabel: string }) {
  const pct = Math.min(Math.max(data.percentage, 0), 150);
  const displayPct = Math.min(pct, 100);

  // Gauge parameters
  const width = 280;
  const height = 170;
  const cx = width / 2;
  const cy = 140;
  const radius = 110;
  const strokeWidth = 20;
  const startAngle = Math.PI;

  // Color segments for the arc (green -> yellow -> orange -> red)
  const segments = [
    { start: 0, end: 0.25, color: '#22c55e' },
    { start: 0.25, end: 0.5, color: '#a3e635' },
    { start: 0.5, end: 0.65, color: '#eab308' },
    { start: 0.65, end: 0.8, color: '#f97316' },
    { start: 0.8, end: 1.0, color: '#ef4444' },
  ];

  function getPoint(fraction: number) {
    const angle = startAngle - fraction * Math.PI;
    return {
      x: cx + radius * Math.cos(angle),
      y: cy - radius * Math.sin(angle),
    };
  }

  // Needle angle
  const needleFraction = displayPct / 100;
  const needleAngle = startAngle - needleFraction * Math.PI;
  const needleLen = radius - 30;
  const needleTip = {
    x: cx + needleLen * Math.cos(needleAngle),
    y: cy - needleLen * Math.sin(needleAngle),
  };

  // Determine gauge value color
  let valueColor = '#22c55e';
  if (pct > 80) valueColor = '#ef4444';
  else if (pct > 65) valueColor = '#f97316';
  else if (pct > 50) valueColor = '#eab308';
  else if (pct > 25) valueColor = '#a3e635';

  return (
    <div className="flex flex-col items-center">
      <h3 className="text-sm font-semibold text-white/70 mb-2">{label}</h3>
      <svg width={width} height={height} className="overflow-visible">
        {/* Background track */}
        <path
          d={`M ${cx - radius} ${cy} A ${radius} ${radius} 0 0 1 ${cx + radius} ${cy}`}
          fill="none"
          stroke="rgba(255,255,255,0.08)"
          strokeWidth={strokeWidth}
        />
        {/* Color segments */}
        {segments.map((seg, i) => {
          const s = getPoint(seg.start);
          const e = getPoint(seg.end);
          const largeArc = seg.end - seg.start > 0.5 ? 1 : 0;
          return (
            <path
              key={i}
              d={`M ${s.x} ${s.y} A ${radius} ${radius} 0 ${largeArc} 1 ${e.x} ${e.y}`}
              fill="none"
              stroke={seg.color}
              strokeWidth={strokeWidth}
              strokeLinecap="butt"
              opacity={0.8}
            />
          );
        })}
        {/* Scale tick marks */}
        {[0, 0.25, 0.5, 0.75, 1.0].map((f, i) => {
          const tickOuter = radius + strokeWidth / 2 + 2;
          const tickInner = radius - strokeWidth / 2 - 2;
          const angle = startAngle - f * Math.PI;
          return (
            <line
              key={i}
              x1={cx + tickOuter * Math.cos(angle)}
              y1={cy - tickOuter * Math.sin(angle)}
              x2={cx + tickInner * Math.cos(angle)}
              y2={cy - tickInner * Math.sin(angle)}
              stroke="rgba(255,255,255,0.3)"
              strokeWidth={1.5}
            />
          );
        })}
        {/* Needle */}
        <line
          x1={cx}
          y1={cy}
          x2={needleTip.x}
          y2={needleTip.y}
          stroke="white"
          strokeWidth={2.5}
          strokeLinecap="round"
          style={{ transition: 'all 0.8s ease-in-out' }}
        />
        {/* Center dot */}
        <circle cx={cx} cy={cy} r={6} fill="white" />
        <circle cx={cx} cy={cy} r={3} fill={valueColor} />
        {/* Value text */}
        <text
          x={cx}
          y={cy + 30}
          textAnchor="middle"
          className="text-lg font-bold"
          fill={valueColor}
        >
          {data.percentage.toFixed(0)}%
        </text>
      </svg>
      <div className="text-xs text-white/40 mt-1">
        {formatNumber(data.value)} / {formatNumber(data.target)} {sublabel}
      </div>
    </div>
  );
}

export default function VehicleDetailPage() {
  const { vehicleId } = useParams<{ vehicleId: string }>();
  const navigate = useNavigate();
  const [vehicle, setVehicle] = useState<VehicleDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchVehicle = async () => {
      if (!vehicleId) return;
      setLoading(true);
      setError(null);
      try {
        const data = await api.getVehicleDetails(Number(vehicleId));
        if (data.error) {
          setError(data.error);
        } else {
          setVehicle(data);
        }
      } catch (err) {
        console.error('Failed to fetch vehicle details:', err);
        setError('Failed to load vehicle details');
      } finally {
        setLoading(false);
      }
    };
    fetchVehicle();
  }, [vehicleId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-brand-orange"></div>
      </div>
    );
  }

  if (error || !vehicle) {
    return (
      <div className="space-y-6 animate-fade-in">
        <div className="glass-panel p-8 text-center">
          <p className="text-white/60">{error || 'Vehicle not found'}</p>
          <button
            onClick={() => navigate(-1)}
            className="mt-4 px-4 py-2 bg-brand-orange hover:bg-brand-orange-light text-white rounded-lg transition-colors"
          >
            Go Back
          </button>
        </div>
      </div>
    );
  }

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
        <Link to="/operation" className="text-brand-orange hover:text-brand-orange-light transition-colors">
          Fleet
        </Link>
        <ChevronRight className="h-4 w-4 text-white/40" />
        <span className="text-white/60">Vehicle details</span>
      </nav>

      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <button
            onClick={() => navigate(-1)}
            className="p-1.5 rounded-lg hover:bg-white/10 transition-colors"
          >
            <ArrowLeft className="h-5 w-5 text-white/60" />
          </button>
          <h1 className="text-2xl font-bold text-white">Vehicle details</h1>
        </div>
      </div>

      {/* Details Panel */}
      <div className="glass-panel p-6">
        {/* Vehicle Specification */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-12">
          <div>
            <DetailRow label="License plate" value={vehicle.registration_number} />
            <DetailRow label="Make" value={vehicle.make_name} />
            <DetailRow label="Model" value={vehicle.model_name} />
            <DetailRow label="Edition" value={vehicle.make_and_model} />
            <DetailRow label="Body type" value={vehicle.body_type} />
            <DetailRow label="No. of doors" value={null} />
            <DetailRow label="Vehicle type" value={vehicle.lease_type_description} />
            <DetailRow label="Fuel type" value={vehicle.fuel_type} />
            <DetailRow label="Engine size" value={null} />
            <DetailRow label="Power" value={null} />
            <DetailRow label="Year Model" value={vehicle.vehicle_year} />
            <DetailRow label="Serial No" value={vehicle.vin_number} />
            <DetailRow label="First use date" value={formatDate(vehicle.lease_start_date)} />
            <DetailRow label="CO2 emissions rate" value={null} />
            <DetailRow label="Colour" value={vehicle.color_name} />
          </div>

          <div>
            {/* Status & Dates */}
            <DetailRow label="Status" value={vehicle.vehicle_status} />
            <DetailRow label="Expected delivery date" value={null} />
            <DetailRow label="Start date" value={formatDate(vehicle.lease_start_date)} />
            <DetailRow label="Exp. End date" value={formatDate(vehicle.expected_end_date)} />
            <DetailRow label="Duration" value={vehicle.lease_duration_months != null ? `${vehicle.lease_duration_months}` : null} />
            <DetailRow label="Mileage" value={formatNumber(vehicle.current_odometer_km)} />
            <DetailRow label="Start Mileage" value={formatNumber(vehicle.start_mileage)} />
            <DetailRow label="Last known mileage" value={formatNumber(vehicle.current_odometer_km)} />

            {/* Organization */}
            <DetailRow label="Product" value={vehicle.lease_type_description} />
            <DetailRow label="Cost Centre" value={vehicle.cost_centre} />
            <DetailRow label="Cost Centre level 2" value={null} />
            <DetailRow label="Company" value={vehicle.company_name || vehicle.customer_name} />

            {/* Driver */}
            <DetailRow label="Driver's name" value={vehicle.driver_last_name} />
            <DetailRow label="Driver's first name" value={vehicle.driver_first_name} />
            <DetailRow label="Driver email" value={vehicle.driver_email} />
            <DetailRow label="Driver mobile phone no." value={vehicle.driver_mobile} />
          </div>
        </div>

        {/* Contract & Administrative - full width */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-12 mt-0">
          <div>
            <DetailRow label="Contract no." value={vehicle.contract_number} />
            <DetailRow label="Object no." value={vehicle.object_no} />
            <DetailRow label="RFL Expiry date" value={null} />
            <DetailRow label="MOT due date" value={null} />
            <DetailRow label="Next expected service date" value={null} />
            <DetailRow label="Next expected tyre fitting date" value={null} />
          </div>
          <div>
            <DetailRow label="Monthly rental" value={vehicle.monthly_lease_amount != null ? formatCurrency(vehicle.monthly_lease_amount) : null} />
            <DetailRow label="Renewal Order No." value={vehicle.renewal_order_no} />
            <DetailRow label="Renewal Status" value={vehicle.renewal_status || vehicle.object_no?.toString()} />
          </div>
        </div>
      </div>

      {/* Gauge Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="glass-panel p-6 flex justify-center">
          <GaugeChart
            data={vehicle.gauge_duration}
            label="Gauge duration"
            sublabel="Months Driven"
          />
        </div>
        <div className="glass-panel p-6 flex justify-center">
          <GaugeChart
            data={vehicle.gauge_km}
            label="Gauge km"
            sublabel="Mileage driven"
          />
        </div>
      </div>
    </div>
  );
}
