import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  BellOff,
  Boxes,
  Calendar,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Cpu,
  Database,
  EyeOff,
  Flame,
  FolderKanban,
  Layers,
  Network,
  Play,
  Search,
  Server,
  ServerCrash,
  ShieldAlert,
  Sparkles,
  Terminal,
  Volume2,
  X,
} from 'lucide-react';

type AlertState = 'ACTIVE' | 'RESOLVED' | 'OK' | 'MUTED';
type ArchitectureComponent = 'otel' | 'kafka' | 'vector' | 'clickhouse' | 'elasticsearch';
type WindowFilter = 'ALL' | 'ACTIVE' | 'RESOLVED' | 'OK';
type SourceFilter = 'ALL' | 'watcher' | 'kibana_rule';
type PriorityFilter = 'ALL' | 'P1' | 'P2' | 'P3' | 'P4';
type GroupByMode = 'component' | 'state' | 'priority' | 'none';

interface TimeRange {
  label: string;
  value: number;
  short: string;
}

const TIME_RANGES: TimeRange[] = [
  { label: 'Dernières 15 minutes', value: 15 * 60 * 1000, short: '15m' },
  { label: 'Dernière heure', value: 60 * 60 * 1000, short: '1h' },
  { label: 'Dernières 3 heures', value: 3 * 60 * 60 * 1000, short: '3h' },
  { label: 'Dernières 12 heures', value: 12 * 60 * 60 * 1000, short: '12h' },
  { label: 'Dernières 24 heures', value: 24 * 60 * 60 * 1000, short: '24h' },
  { label: 'Derniers 7 jours', value: 7 * 24 * 60 * 60 * 1000, short: '7j' },
];

const COMPONENT_META: Record<
  ArchitectureComponent,
  { title: string; subtitle: string; role: string; order: number }
> = {
  otel: {
    title: 'OTel Collectors',
    subtitle: 'Ingress OTLP / Envoy',
    role: 'Collecte Traces, Logs & Metrics',
    order: 1,
  },
  kafka: {
    title: 'Kafka Cluster',
    subtitle: 'Bus de Message / Topics',
    role: 'Buffering & Découplage Haut Débit',
    order: 2,
  },
  vector: {
    title: 'Vector Pipeline',
    subtitle: 'Parsing VRL & Routing',
    role: 'Transformation, Enrichissement & Sink',
    order: 3,
  },
  clickhouse: {
    title: 'ClickHouse',
    subtitle: 'Stockage Colonnaire OLAP',
    role: 'Analytics Logs, Traces & Métriques',
    order: 4,
  },
  elasticsearch: {
    title: 'Elasticsearch',
    subtitle: 'Moteur Watchers & Kibana',
    role: 'Indexation & Évaluation des Règles',
    order: 5,
  },
};

interface UnifiedAlert {
  id: string;
  name: string;
  source: 'watcher' | 'kibana_rule';
  component: ArchitectureComponent;
  priority: 'P1' | 'P2' | 'P3' | 'P4';
  severity: string;
  enabled: boolean;
  state: AlertState;
  schedule: string;
  activeCount: number;
  fireCount24h: number;
  hourlyBuckets: number[];
  triggeredAt?: string;
  resolvedAt?: string;
  lastCheckedAt?: string;
  summaryReason?: string;
  conditionDsl?: string;
  tags: string[];
}

interface ClusterSnapshot {
  updatedAt: string;
  queryDurationMs: number;
  esHealthy: boolean;
  demoMode: boolean;
  clusterName: string;
  errorMessage?: string;
  alerts: UnifiedAlert[];
}

// Normalisation défensive de chaque alerte reçue du backend
function sanitizeComponent(raw?: string, name = '', id = '', tags: string[] = []): ArchitectureComponent {
  const c = (raw || '').toLowerCase().trim();
  if (c === 'otel' || c === 'kafka' || c === 'vector' || c === 'clickhouse' || c === 'elasticsearch') {
    return c;
  }
  const hay = `${name} ${id} ${tags.join(' ')}`.toLowerCase();
  if (hay.includes('kafka') || hay.includes('consumer') || hay.includes('broker') || hay.includes('topic')) return 'kafka';
  if (hay.includes('vector') || hay.includes('vrl')) return 'vector';
  if (hay.includes('clickhouse') || hay.includes('mergetree')) return 'clickhouse';
  if (hay.includes('otel') || hay.includes('envoy') || hay.includes('span')) return 'otel';
  return 'elasticsearch';
}

function sanitizePriority(raw?: string, severity = ''): 'P1' | 'P2' | 'P3' | 'P4' {
  const p = (raw || '').toUpperCase().trim();
  if (p === 'P1' || p === 'P2' || p === 'P3' || p === 'P4') return p;
  const s = (severity || '').toLowerCase();
  if (s === 'critical') return 'P1';
  if (s === 'high') return 'P2';
  if (s === 'medium') return 'P3';
  return 'P4';
}

function sanitizeSnapshot(raw: any): ClusterSnapshot {
  const rawAlerts = Array.isArray(raw?.alerts) ? raw.alerts : [];
  const safeAlerts: UnifiedAlert[] = rawAlerts.map((a: any, idx: number) => {
    const tags = Array.isArray(a?.tags) ? a.tags.map(String) : [];
    const name = String(a?.name || a?.id || `alert-${idx}`);
    const id = String(a?.id || `id-${idx}`);
    return {
      id,
      name,
      source: a?.source === 'watcher' ? 'watcher' : 'kibana_rule',
      component: sanitizeComponent(a?.component, name, id, tags),
      priority: sanitizePriority(a?.priority, a?.severity),
      severity: String(a?.severity || 'medium'),
      enabled: a?.enabled !== undefined ? Boolean(a.enabled) : true,
      state:
        a?.state === 'ACTIVE' || a?.state === 'RESOLVED' || a?.state === 'OK' || a?.state === 'MUTED'
          ? a.state
          : 'OK',
      schedule: String(a?.schedule || '1m'),
      activeCount: Number(a?.activeCount || 0),
      fireCount24h: Number(a?.fireCount24h || 0),
      hourlyBuckets:
        Array.isArray(a?.hourlyBuckets) && a.hourlyBuckets.length === 24
          ? a.hourlyBuckets.map(Number)
          : new Array(24).fill(0),
      triggeredAt: a?.triggeredAt ? String(a.triggeredAt) : undefined,
      resolvedAt: a?.resolvedAt ? String(a.resolvedAt) : undefined,
      lastCheckedAt: a?.lastCheckedAt ? String(a.lastCheckedAt) : undefined,
      summaryReason: a?.summaryReason ? String(a.summaryReason) : undefined,
      conditionDsl: a?.conditionDsl ? String(a.conditionDsl) : undefined,
      tags,
    };
  });

  return {
    updatedAt: String(raw?.updatedAt || new Date().toISOString()),
    queryDurationMs: Number(raw?.queryDurationMs || 0),
    esHealthy: raw?.esHealthy !== undefined ? Boolean(raw.esHealthy) : true,
    demoMode: raw?.demoMode !== undefined ? Boolean(raw.demoMode) : true,
    clusterName: String(raw?.clusterName || 'Cluster Elasticsearch'),
    errorMessage: raw?.errorMessage ? String(raw.errorMessage) : undefined,
    alerts: safeAlerts,
  };
}

function formatRelativeTime(isoString?: string, nowMs: number = Date.now()): string {
  if (!isoString) return '—';
  const parsed = new Date(isoString).getTime();
  if (Number.isNaN(parsed)) return '—';
  const diffSec = Math.max(0, Math.floor((nowMs - parsed) / 1000));
  if (diffSec < 60) return `il y a ${diffSec}s`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `il y a ${diffMin}m`;
  const diffHours = Math.floor(diffMin / 60);
  if (diffHours < 24) {
    const remMin = diffMin % 60;
    return remMin > 0 ? `il y a ${diffHours}h ${remMin}m` : `il y a ${diffHours}h`;
  }
  const diffDays = Math.floor(diffHours / 24);
  return `il y a ${diffDays}j`;
}

function isResolvedWithin(alert: UnifiedAlert, maxAgeMs: number, nowMs: number): boolean {
  if (alert.state !== 'RESOLVED' || !alert.resolvedAt) return false;
  const parsed = new Date(alert.resolvedAt).getTime();
  if (Number.isNaN(parsed)) return false;
  const age = nowMs - parsed;
  return age >= 0 && age <= maxAgeMs;
}

// Error Boundary pour empêcher tout écran noir silencieux
class SREErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; errorMsg: string }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { hasError: false, errorMsg: '' };
  }
  static getDerivedStateFromError(error: Error) {
    return { hasError: true, errorMsg: error?.message || String(error) };
  }
  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-slate-950 text-rose-200 p-8 flex flex-col items-center justify-center">
          <div className="max-w-lg w-full bg-rose-950/40 border border-rose-500/50 rounded-2xl p-6 space-y-4">
            <h1 className="text-base font-bold text-white flex items-center gap-2">
              <ServerCrash className="w-5 h-5 text-rose-400" /> Exception UI Interceptée
            </h1>
            <pre className="p-3 rounded bg-slate-950 text-xs font-mono text-rose-300 overflow-x-auto">
              {this.state.errorMsg}
            </pre>
            <button
              onClick={() => window.location.reload()}
              className="px-4 py-2 rounded-lg bg-rose-600 text-white text-xs font-semibold cursor-pointer"
            >
              Recharger la console
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

export default function App() {
  return (
    <SREErrorBoundary>
      <AlertConsoleInner />
    </SREErrorBoundary>
  );
}

function AlertConsoleInner() {
  const [snapshot, setSnapshot] = useState<ClusterSnapshot>(() =>
    sanitizeSnapshot({
      updatedAt: new Date().toISOString(),
      queryDurationMs: 0,
      esHealthy: true,
      demoMode: true,
      clusterName: 'Connexion au backend Go...',
      alerts: [],
    })
  );
  const [sseConnected, setSseConnected] = useState<boolean>(false);
  const [nowMs, setNowMs] = useState<number>(Date.now());

  const [timeRange, setTimeRange] = useState<TimeRange>(TIME_RANGES[4]);
  const [isTimePickerOpen, setIsTimePickerOpen] = useState(false);

  const [showTopologyTab, setShowTopologyTab] = useState<boolean>(true);
  const [groupBy, setGroupBy] = useState<GroupByMode>('component');
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean>>({});

  const [windowFilter, setWindowFilter] = useState<WindowFilter>('ALL');
  const [sourceFilter, setSourceFilter] = useState<SourceFilter>('ALL');
  const [priorityFilter, setPriorityFilter] = useState<PriorityFilter>('ALL');
  const [componentFilter, setComponentFilter] = useState<ArchitectureComponent | null>(null);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [hideMuted, setHideMuted] = useState<boolean>(false);

  const [selectedAlertId, setSelectedAlertId] = useState<string | null>(null);
  const [showInspectModal, setShowInspectModal] = useState<boolean>(false);

  const searchInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const timer = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        searchInputRef.current?.focus();
      } else if (e.key === 'Escape') {
        setShowInspectModal(false);
        setIsTimePickerOpen(false);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  useEffect(() => {
    const evtSource = new EventSource('/api/stream');
    evtSource.onopen = () => setSseConnected(true);
    evtSource.onmessage = (event) => {
      try {
        const raw = JSON.parse(event.data);
        setSnapshot(sanitizeSnapshot(raw));
        setSseConnected(true);
      } catch (err) {
        console.error('Erreur SSE:', err);
      }
    };
    evtSource.onerror = () => setSseConnected(false);
    return () => evtSource.close();
  }, []);

  const alertsList = snapshot.alerts || [];

  const selectedAlert = useMemo(
    () => alertsList.find((a) => a.id === selectedAlertId) || null,
    [alertsList, selectedAlertId]
  );

  const highlightedComponent: ArchitectureComponent | null = selectedAlert
    ? selectedAlert.component
    : null;

  const visibleAlertsInTimeRange = useMemo(() => {
    return alertsList.filter((alert) => {
      if (alert.state === 'RESOLVED' && !isResolvedWithin(alert, timeRange.value, nowMs)) {
        return false;
      }
      return true;
    });
  }, [alertsList, timeRange, nowMs]);

  const componentStats = useMemo(() => {
    const stats: Record<ArchitectureComponent, { active: number; resolved: number; total: number }> = {
      otel: { active: 0, resolved: 0, total: 0 },
      kafka: { active: 0, resolved: 0, total: 0 },
      vector: { active: 0, resolved: 0, total: 0 },
      clickhouse: { active: 0, resolved: 0, total: 0 },
      elasticsearch: { active: 0, resolved: 0, total: 0 },
    };
    for (const a of visibleAlertsInTimeRange) {
      const comp = a.component || 'elasticsearch';
      if (!stats[comp]) continue;
      stats[comp].total++;
      if (a.state === 'ACTIVE') stats[comp].active++;
      if (a.state === 'RESOLVED') stats[comp].resolved++;
    }
    return stats;
  }, [visibleAlertsInTimeRange]);

  const kpiCounts = useMemo(() => {
    const base = visibleAlertsInTimeRange.filter(
      (a) =>
        (sourceFilter === 'ALL' || a.source === sourceFilter) &&
        (priorityFilter === 'ALL' || a.priority === priorityFilter) &&
        (!componentFilter || a.component === componentFilter) &&
        (!hideMuted || a.enabled)
    );
    return {
      total: base.length,
      actives: base.filter((a) => a.state === 'ACTIVE').length,
      resolved: base.filter((a) => a.state === 'RESOLVED').length,
      ok: base.filter((a) => a.state === 'OK' || a.state === 'MUTED').length,
      watchers: visibleAlertsInTimeRange.filter((a) => a.source === 'watcher').length,
      kibana: visibleAlertsInTimeRange.filter((a) => a.source === 'kibana_rule').length,
    };
  }, [visibleAlertsInTimeRange, sourceFilter, priorityFilter, componentFilter, hideMuted]);

  const filteredAlerts = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    return visibleAlertsInTimeRange.filter((alert) => {
      if (sourceFilter !== 'ALL' && alert.source !== sourceFilter) return false;
      if (priorityFilter !== 'ALL' && alert.priority !== priorityFilter) return false;
      if (componentFilter && alert.component !== componentFilter) return false;
      if (hideMuted && !alert.enabled) return false;

      if (windowFilter === 'ACTIVE' && alert.state !== 'ACTIVE') return false;
      if (windowFilter === 'RESOLVED' && alert.state !== 'RESOLVED') return false;
      if (windowFilter === 'OK' && alert.state !== 'OK' && alert.state !== 'MUTED') return false;

      if (q) {
        const hay = `${alert.priority} ${alert.component} ${alert.name} ${alert.id} ${
          alert.summaryReason || ''
        } ${(alert.tags || []).join(' ')}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
  }, [
    visibleAlertsInTimeRange,
    sourceFilter,
    priorityFilter,
    componentFilter,
    hideMuted,
    windowFilter,
    searchQuery,
  ]);

  const groupedAlerts = useMemo(() => {
    if (groupBy === 'none') {
      return [{ key: 'all', title: 'Toutes les alertes', order: 1, alerts: filteredAlerts }];
    }

    const groupsMap = new Map<string, UnifiedAlert[]>();

    for (const alert of filteredAlerts) {
      let key = 'elasticsearch';
      if (groupBy === 'component') {
        key = alert.component || 'elasticsearch';
      } else if (groupBy === 'priority') {
        key = alert.priority || 'P3';
      } else if (groupBy === 'state') {
        key = alert.state || 'OK';
      }
      if (!groupsMap.has(key)) {
        groupsMap.set(key, []);
      }
      groupsMap.get(key)!.push(alert);
    }

    const entries = Array.from(groupsMap.entries()).map(([key, list]) => {
      let title = key;
      let order = 99;
      if (groupBy === 'component') {
        const meta = COMPONENT_META[key as ArchitectureComponent];
        title = meta ? `${meta.title} (${meta.subtitle})` : key.toUpperCase();
        order = meta ? meta.order : 99;
      } else if (groupBy === 'priority') {
        const labels: Record<string, string> = {
          P1: 'Priorité P1 — Critique (Intervention Immédiate)',
          P2: 'Priorité P2 — Haute',
          P3: 'Priorité P3 — Moyenne',
          P4: 'Priorité P4 — Basse / Info',
        };
        title = labels[key] || key;
        order = ['P1', 'P2', 'P3', 'P4'].indexOf(key);
      } else if (groupBy === 'state') {
        const labels: Record<string, string> = {
          ACTIVE: 'Alertes Toujours Actives (Seuil Franchi)',
          RESOLVED: `Alertes Terminées (${timeRange.label})`,
          OK: 'Règles Saines (OK)',
          MUTED: 'Règles Désactivées (Muted)',
        };
        title = labels[key] || key;
        order = ['ACTIVE', 'RESOLVED', 'OK', 'MUTED'].indexOf(key);
      }
      return { key, title, order, alerts: list };
    });

    entries.sort((a, b) => a.order - b.order);
    return entries;
  }, [filteredAlerts, groupBy, timeRange.label]);

  const toggleGroupCollapse = (key: string) => {
    setCollapsedGroups((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const handleAlertClick = (alert: UnifiedAlert) => {
    if (selectedAlertId === alert.id) {
      setSelectedAlertId(null);
    } else {
      setSelectedAlertId(alert.id);
      setShowTopologyTab(true);
    }
  };

  const handleSimulate = async () => await fetch('/api/simulate', { method: 'POST' });
  const handleToggleMute = async (id: string) => {
    await fetch('/api/toggle-mute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id }),
    });
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 font-sans selection:bg-indigo-500/30 pb-14">
      {(!snapshot.esHealthy || !sseConnected) && (
        <div className="bg-rose-950/90 border-b border-rose-700 px-6 py-2.5 flex items-center justify-between text-xs text-rose-200">
          <div className="flex items-center gap-2 font-medium">
            <ServerCrash className="w-4 h-4 text-rose-400 shrink-0 animate-pulse" />
            <span>
              {!sseConnected
                ? 'Flux SSE déconnecté du backend Go...'
                : `Erreur de connexion Elasticsearch : ${snapshot.errorMessage}`}
            </span>
          </div>
          <span className="font-mono text-rose-400">DEAD-MAN-SWITCH</span>
        </div>
      )}

      {/* Header */}
      <header className="sticky top-0 z-20 backdrop-blur-md bg-slate-950/90 border-b border-slate-800/80 px-6 py-3">
        <div className="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-indigo-500/10 border border-indigo-500/30 flex items-center justify-center">
              <ShieldAlert className="w-4 h-4 text-indigo-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-sm font-semibold tracking-tight text-white">
                  Elastic Alert Console
                </h1>
                {snapshot.demoMode && (
                  <span className="inline-flex items-center gap-1 text-[10px] font-medium px-2 py-0.5 rounded-full bg-amber-500/10 text-amber-300 border border-amber-500/25">
                    <Sparkles className="w-3 h-3" /> Démo SRE
                  </span>
                )}
              </div>
              <p className="text-[11px] text-slate-400 font-mono">{snapshot.clusterName}</p>
            </div>
          </div>

          <div className="relative flex-1 max-w-md">
            <Search className="w-4 h-4 text-slate-500 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              ref={searchInputRef}
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Filtrer par brique (kafka, vector, clickhouse), P1..P4, nom... (⌘K)"
              className="w-full bg-slate-900/90 border border-slate-800 focus:border-indigo-500/60 rounded-lg pl-9 pr-8 py-1.5 text-xs text-slate-200 placeholder:text-slate-500 focus:outline-none transition"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery('')}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          <div className="flex items-center gap-2.5">
            <div className="relative">
              <button
                onClick={() => setIsTimePickerOpen(!isTimePickerOpen)}
                className="flex items-center gap-2 bg-slate-900 hover:bg-slate-800 border border-slate-700 px-3 py-1.5 rounded-lg text-xs font-medium text-slate-200 transition cursor-pointer"
              >
                <Calendar className="w-3.5 h-3.5 text-indigo-400" />
                <span>{timeRange.label}</span>
                <ChevronDown className="w-3.5 h-3.5 text-slate-500" />
              </button>

              {isTimePickerOpen && (
                <>
                  <div className="fixed inset-0 z-40" onClick={() => setIsTimePickerOpen(false)} />
                  <div className="absolute right-0 mt-2 w-52 bg-slate-900 border border-slate-700 rounded-xl shadow-2xl z-50 overflow-hidden py-1">
                    <div className="px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-slate-500 border-b border-slate-800">
                      Fenêtre des alertes terminées
                    </div>
                    {TIME_RANGES.map((tr) => (
                      <button
                        key={tr.short}
                        onClick={() => {
                          setTimeRange(tr);
                          setIsTimePickerOpen(false);
                        }}
                        className={`flex items-center justify-between w-full text-left px-3.5 py-2 text-xs transition cursor-pointer ${
                          timeRange.value === tr.value
                            ? 'bg-indigo-500/15 text-indigo-300 font-medium'
                            : 'text-slate-300 hover:bg-slate-800'
                        }`}
                      >
                        <span>{tr.label}</span>
                        <span className="font-mono text-[10px] text-slate-500">-{tr.short}</span>
                      </button>
                    ))}
                  </div>
                </>
              )}
            </div>

            {snapshot.demoMode && (
              <button
                onClick={handleSimulate}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-indigo-600/20 hover:bg-indigo-600/30 border border-indigo-500/40 text-indigo-200 text-xs font-medium transition cursor-pointer"
              >
                <Play className="w-3.5 h-3.5 fill-current" /> Simuler
              </button>
            )}
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-5 space-y-5">
        {/* Ruban KPI */}
        <section className="grid grid-cols-2 lg:grid-cols-4 gap-3.5">
          <KpiCard
            active={windowFilter === 'ACTIVE'}
            onClick={() => setWindowFilter(windowFilter === 'ACTIVE' ? 'ALL' : 'ACTIVE')}
            title="Toujours Actives"
            subtitle="Seuil franchi en cours"
            count={kpiCounts.actives}
            variant="danger"
            icon={<Flame className="w-4 h-4" />}
          />
          <KpiCard
            active={windowFilter === 'RESOLVED'}
            onClick={() => setWindowFilter(windowFilter === 'RESOLVED' ? 'ALL' : 'RESOLVED')}
            title={`Terminées (-${timeRange.short})`}
            subtitle={timeRange.label}
            count={kpiCounts.resolved}
            variant="emerald"
            icon={<CheckCircle2 className="w-4 h-4" />}
          />
          <KpiCard
            active={windowFilter === 'OK'}
            onClick={() => setWindowFilter(windowFilter === 'OK' ? 'ALL' : 'OK')}
            title="Règles Saines"
            subtitle="Aucun seuil atteint"
            count={kpiCounts.ok}
            variant="neutral"
            icon={<ShieldAlert className="w-4 h-4" />}
          />
          <KpiCard
            active={windowFilter === 'ALL'}
            onClick={() => setWindowFilter('ALL')}
            title="Inventaire Total"
            subtitle={`${kpiCounts.watchers} Watchers · ${kpiCounts.kibana} Kibana`}
            count={kpiCounts.total}
            variant="neutral"
            icon={<Layers className="w-4 h-4" />}
          />
        </section>

        {/* --- TAB TOPOLOGIE / ARCHITECTURE INTERACTIVE --- */}
        <section className="bg-slate-900/50 border border-slate-800/90 rounded-2xl overflow-hidden shadow-xl">
          <div className="px-5 py-3 bg-slate-900/90 border-b border-slate-800 flex flex-wrap items-center justify-between gap-3">
            <button
              onClick={() => setShowTopologyTab(!showTopologyTab)}
              className="flex items-center gap-2 text-xs font-semibold text-white cursor-pointer"
            >
              <Network className="w-4 h-4 text-indigo-400" />
              <span>Topologie & Architecture Pipeline (OTel ➔ Kafka ➔ Vector ➔ ClickHouse)</span>
              <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-400">
                {showTopologyTab ? 'Masquer' : 'Afficher'}
              </span>
            </button>

            <div className="flex items-center gap-3 text-[11px]">
              {selectedAlert ? (
                <div className="flex items-center gap-2 px-2.5 py-1 rounded-full bg-rose-500/20 border border-rose-500/50 text-rose-200">
                  <span className="w-2 h-2 rounded-full bg-rose-400 animate-ping" />
                  <span>
                    Alerte sélectionnée : <strong>{selectedAlert.name}</strong> ➔ Brique{' '}
                    <strong className="uppercase text-rose-300">
                      {COMPONENT_META[selectedAlert.component]?.title}
                    </strong>{' '}
                    en rouge
                  </span>
                  <button
                    onClick={() => setSelectedAlertId(null)}
                    className="ml-1 hover:text-white cursor-pointer"
                  >
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
              ) : (
                <span className="text-slate-400">
                  Cliquez sur une alerte ci-dessous pour illuminer en{' '}
                  <strong className="text-rose-400">rouge</strong> la brique impactée
                </span>
              )}
            </div>
          </div>

          {showTopologyTab && (
            <div className="p-5 space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-9 gap-2.5 items-center">
                <div className="md:col-span-2">
                  <ArchitectureBrick
                    comp="otel"
                    meta={COMPONENT_META.otel}
                    stats={componentStats.otel}
                    isHighlightedByAlert={highlightedComponent === 'otel'}
                    isFiltered={componentFilter === 'otel'}
                    selectedAlert={highlightedComponent === 'otel' ? selectedAlert : null}
                    onClickFilter={() =>
                      setComponentFilter(componentFilter === 'otel' ? null : 'otel')
                    }
                    icon={<Cpu className="w-4 h-4" />}
                  />
                </div>

                <PipelineConnector
                  danger={highlightedComponent === 'otel' || highlightedComponent === 'kafka'}
                  label="OTLP / gRPC"
                />

                <div className="md:col-span-2">
                  <ArchitectureBrick
                    comp="kafka"
                    meta={COMPONENT_META.kafka}
                    stats={componentStats.kafka}
                    isHighlightedByAlert={highlightedComponent === 'kafka'}
                    isFiltered={componentFilter === 'kafka'}
                    selectedAlert={highlightedComponent === 'kafka' ? selectedAlert : null}
                    onClickFilter={() =>
                      setComponentFilter(componentFilter === 'kafka' ? null : 'kafka')
                    }
                    icon={<Server className="w-4 h-4" />}
                  />
                </div>

                <PipelineConnector
                  danger={highlightedComponent === 'kafka' || highlightedComponent === 'vector'}
                  label="Consumer Group"
                />

                <div className="md:col-span-2">
                  <ArchitectureBrick
                    comp="vector"
                    meta={COMPONENT_META.vector}
                    stats={componentStats.vector}
                    isHighlightedByAlert={highlightedComponent === 'vector'}
                    isFiltered={componentFilter === 'vector'}
                    selectedAlert={highlightedComponent === 'vector' ? selectedAlert : null}
                    onClickFilter={() =>
                      setComponentFilter(componentFilter === 'vector' ? null : 'vector')
                    }
                    icon={<Activity className="w-4 h-4" />}
                  />
                </div>

                <PipelineConnector
                  danger={
                    highlightedComponent === 'vector' || highlightedComponent === 'clickhouse'
                  }
                  label="Native Batch Sink"
                />

                <div className="md:col-span-9 grid grid-cols-1 md:grid-cols-2 gap-3 pt-3 mt-1 border-t border-slate-800/70">
                  <ArchitectureBrick
                    comp="clickhouse"
                    meta={COMPONENT_META.clickhouse}
                    stats={componentStats.clickhouse}
                    isHighlightedByAlert={highlightedComponent === 'clickhouse'}
                    isFiltered={componentFilter === 'clickhouse'}
                    selectedAlert={
                      highlightedComponent === 'clickhouse' ? selectedAlert : null
                    }
                    onClickFilter={() =>
                      setComponentFilter(
                        componentFilter === 'clickhouse' ? null : 'clickhouse'
                      )
                    }
                    icon={<Database className="w-4 h-4" />}
                  />

                  <ArchitectureBrick
                    comp="elasticsearch"
                    meta={COMPONENT_META.elasticsearch}
                    stats={componentStats.elasticsearch}
                    isHighlightedByAlert={highlightedComponent === 'elasticsearch'}
                    isFiltered={componentFilter === 'elasticsearch'}
                    selectedAlert={
                      highlightedComponent === 'elasticsearch' ? selectedAlert : null
                    }
                    onClickFilter={() =>
                      setComponentFilter(
                        componentFilter === 'elasticsearch' ? null : 'elasticsearch'
                      )
                    }
                    icon={<ShieldAlert className="w-4 h-4" />}
                  />
                </div>
              </div>
            </div>
          )}
        </section>

        {/* --- BARRE D'OUTILS : REGROUPEMENT & FILTRES --- */}
        <section className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800/80 pb-3">
          <div className="flex flex-wrap items-center gap-2.5">
            <div className="flex items-center gap-1 bg-slate-900 p-1 rounded-lg border border-slate-800 text-xs">
              <span className="px-2 text-[11px] text-slate-400 flex items-center gap-1">
                <FolderKanban className="w-3.5 h-3.5 text-indigo-400" /> Grouper par :
              </span>
              {(
                [
                  { id: 'component', label: 'Brique Architecture' },
                  { id: 'priority', label: 'Priorité (P1–P4)' },
                  { id: 'state', label: 'État' },
                  { id: 'none', label: 'Aucun' },
                ] as { id: GroupByMode; label: string }[]
              ).map((g) => (
                <button
                  key={g.id}
                  onClick={() => setGroupBy(g.id)}
                  className={`px-2.5 py-1 rounded-md font-medium transition cursor-pointer ${
                    groupBy === g.id
                      ? 'bg-indigo-600 text-white shadow-xs'
                      : 'text-slate-400 hover:text-slate-200'
                  }`}
                >
                  {g.label}
                </button>
              ))}
            </div>

            <div className="flex items-center gap-1 bg-slate-900/90 p-1 rounded-lg border border-slate-800 text-xs">
              <button
                onClick={() => setSourceFilter('ALL')}
                className={`px-2.5 py-1 rounded-md font-medium cursor-pointer ${
                  sourceFilter === 'ALL' ? 'bg-slate-800 text-white' : 'text-slate-400'
                }`}
              >
                Toutes
              </button>
              <button
                onClick={() => setSourceFilter('watcher')}
                className={`px-2.5 py-1 rounded-md font-medium cursor-pointer ${
                  sourceFilter === 'watcher' ? 'bg-slate-800 text-amber-300' : 'text-slate-400'
                }`}
              >
                Watchers ({kpiCounts.watchers})
              </button>
              <button
                onClick={() => setSourceFilter('kibana_rule')}
                className={`px-2.5 py-1 rounded-md font-medium cursor-pointer ${
                  sourceFilter === 'kibana_rule' ? 'bg-slate-800 text-sky-300' : 'text-slate-400'
                }`}
              >
                Kibana ({kpiCounts.kibana})
              </button>
            </div>

            <div className="flex items-center gap-1 bg-slate-900/90 p-1 rounded-lg border border-slate-800 text-xs">
              {(['ALL', 'P1', 'P2', 'P3', 'P4'] as PriorityFilter[]).map((p) => (
                <button
                  key={p}
                  onClick={() => setPriorityFilter(p)}
                  className={`px-2 py-1 rounded-md font-mono font-semibold cursor-pointer ${
                    priorityFilter === p
                      ? 'bg-indigo-600/30 text-indigo-200 border border-indigo-500/40'
                      : 'text-slate-400'
                  }`}
                >
                  {p === 'ALL' ? 'P1–P4' : p}
                </button>
              ))}
            </div>
          </div>

          <div className="flex items-center gap-3">
            {componentFilter && (
              <button
                onClick={() => setComponentFilter(null)}
                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-indigo-500/15 border border-indigo-500/30 text-indigo-300 text-xs cursor-pointer"
              >
                <span>Brique : {COMPONENT_META[componentFilter]?.title}</span>
                <X className="w-3.5 h-3.5" />
              </button>
            )}

            <label className="flex items-center gap-2 text-xs text-slate-400 hover:text-slate-200 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={hideMuted}
                onChange={(e) => setHideMuted(e.target.checked)}
                className="rounded bg-slate-900 border-slate-700 text-indigo-500"
              />
              <EyeOff className="w-3.5 h-3.5" /> Masquer Muted
            </label>
          </div>
        </section>

        {/* --- LISTE DES ALERTES REGROUPÉES --- */}
        <section className="space-y-4">
          {groupedAlerts.length === 0 || filteredAlerts.length === 0 ? (
            <div className="bg-slate-900/40 border border-slate-800/80 rounded-xl py-12 text-center text-xs text-slate-500">
              Aucune alerte pour les critères et la fenêtre temporelle ({timeRange.label}).
            </div>
          ) : (
            groupedAlerts.map((group) => {
              const isCollapsed = !!collapsedGroups[group.key];
              const activeInGroup = group.alerts.filter((a) => a.state === 'ACTIVE').length;
              const isGroupHighlighted =
                groupBy === 'component' && highlightedComponent === group.key;

              return (
                <div
                  key={group.key}
                  className={`rounded-xl border overflow-hidden transition-all ${
                    isGroupHighlighted
                      ? 'bg-rose-950/15 border-rose-500/70 shadow-lg shadow-rose-950/30'
                      : 'bg-slate-900/40 border-slate-800/80'
                  }`}
                >
                  <div
                    onClick={() => toggleGroupCollapse(group.key)}
                    className="px-4 py-2.5 bg-slate-900/80 border-b border-slate-800/70 flex items-center justify-between cursor-pointer select-none hover:bg-slate-900"
                  >
                    <div className="flex items-center gap-2.5">
                      {isCollapsed ? (
                        <ChevronRight className="w-4 h-4 text-slate-400" />
                      ) : (
                        <ChevronDown className="w-4 h-4 text-slate-400" />
                      )}
                      <Boxes className="w-4 h-4 text-indigo-400" />
                      <span className="text-xs font-semibold text-white">{group.title}</span>
                      <span className="text-[11px] font-mono px-2 py-0.5 rounded-full bg-slate-800 text-slate-300">
                        {group.alerts.length} règle{group.alerts.length > 1 ? 's' : ''}
                      </span>
                      {activeInGroup > 0 && (
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-mono font-bold bg-rose-500/20 text-rose-300 border border-rose-500/30">
                          <span className="w-1.5 h-1.5 rounded-full bg-rose-400 animate-ping" />
                          {activeInGroup} active{activeInGroup > 1 ? 's' : ''}
                        </span>
                      )}
                    </div>
                    <span className="text-[11px] text-slate-500">
                      Cliquez sur une ligne pour cibler sa brique dans l'architecture
                    </span>
                  </div>

                  {!isCollapsed && (
                    <div className="overflow-x-auto">
                      <table className="w-full text-left border-collapse">
                        <thead>
                          <tr className="border-b border-slate-800/60 text-[10px] font-semibold uppercase tracking-wider text-slate-500 bg-slate-950/40">
                            <th className="py-2.5 px-4">État</th>
                            <th className="py-2.5 px-3">Priorité & Alerte</th>
                            <th className="py-2.5 px-3">Brique</th>
                            <th className="py-2.5 px-3">Moteur</th>
                            <th className="py-2.5 px-4">Activité (24h)</th>
                            <th className="py-2.5 px-4 text-right">Déclenchée</th>
                            <th className="py-2.5 px-4 text-right">Résolue</th>
                            <th className="py-2.5 px-3 text-right">Détails</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800/40 text-xs">
                          {group.alerts.map((alert) => {
                            const isSelected = selectedAlertId === alert.id;
                            return (
                              <tr
                                key={alert.id}
                                onClick={() => handleAlertClick(alert)}
                                className={`group cursor-pointer transition-colors ${
                                  isSelected
                                    ? 'bg-rose-500/20 hover:bg-rose-500/25 ring-1 ring-inset ring-rose-500/60'
                                    : alert.state === 'ACTIVE'
                                    ? 'bg-rose-950/15 hover:bg-rose-950/30'
                                    : 'hover:bg-slate-900/80'
                                }`}
                              >
                                <td className="py-3 px-4 whitespace-nowrap">
                                  <StateBadge state={alert.state} activeCount={alert.activeCount} />
                                </td>

                                <td className="py-3 px-3 max-w-md">
                                  <div className="flex items-center gap-2.5">
                                    <PriorityBadge
                                      priority={alert.priority}
                                      isActive={alert.state === 'ACTIVE' || isSelected}
                                    />
                                    <span
                                      className={`font-medium truncate ${
                                        isSelected
                                          ? 'text-rose-200 font-semibold'
                                          : 'text-slate-100 group-hover:text-white'
                                      }`}
                                    >
                                      {alert.name}
                                    </span>
                                  </div>
                                  {alert.summaryReason && (
                                    <div className="text-[11px] text-slate-400 truncate mt-1 pl-9">
                                      {alert.summaryReason}
                                    </div>
                                  )}
                                </td>

                                <td className="py-3 px-3 whitespace-nowrap">
                                  <span
                                    className={`inline-flex items-center gap-1 px-2 py-0.5 rounded font-mono text-[10px] uppercase border ${
                                      isSelected
                                        ? 'bg-rose-500 text-white border-rose-400 font-bold'
                                        : 'bg-slate-800/90 text-slate-300 border-slate-700'
                                    }`}
                                  >
                                    {alert.component}
                                  </span>
                                </td>

                                <td className="py-3 px-3 whitespace-nowrap">
                                  <span
                                    className={`inline-flex px-2 py-0.5 rounded font-mono text-[11px] border ${
                                      alert.source === 'watcher'
                                        ? 'bg-amber-500/10 text-amber-300 border-amber-500/20'
                                        : 'bg-sky-500/10 text-sky-300 border-sky-500/20'
                                    }`}
                                  >
                                    {alert.source === 'watcher' ? 'Watcher' : 'Kibana'}
                                  </span>
                                </td>

                                <td className="py-3 px-4 whitespace-nowrap">
                                  <div className="flex items-center gap-2">
                                    <Sparkline24h
                                      buckets={alert.hourlyBuckets}
                                      isActive={alert.state === 'ACTIVE'}
                                    />
                                    <span className="font-mono text-[11px] text-slate-400 w-7 text-right">
                                      {alert.fireCount24h > 0 ? `${alert.fireCount24h}×` : '0'}
                                    </span>
                                  </div>
                                </td>

                                <td className="py-3 px-4 text-right font-mono text-slate-300 whitespace-nowrap">
                                  {formatRelativeTime(alert.triggeredAt, nowMs)}
                                </td>

                                <td className="py-3 px-4 text-right font-mono whitespace-nowrap">
                                  {alert.state === 'ACTIVE' ? (
                                    <span className="text-rose-400 font-semibold">En cours</span>
                                  ) : alert.resolvedAt ? (
                                    <span className="text-emerald-400">
                                      {formatRelativeTime(alert.resolvedAt, nowMs)}
                                    </span>
                                  ) : (
                                    <span className="text-slate-600">—</span>
                                  )}
                                </td>

                                <td
                                  className="py-3 px-3 text-right whitespace-nowrap"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    setSelectedAlertId(alert.id);
                                    setShowInspectModal(true);
                                  }}
                                >
                                  <button className="px-2 py-1 rounded bg-slate-800/80 hover:bg-slate-700 text-[11px] text-slate-300 cursor-pointer">
                                    JSON
                                  </button>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              );
            })
          )}
        </section>
      </main>

      {showInspectModal && selectedAlert && (
        <div
          className="fixed inset-0 z-30 flex justify-end bg-black/60 backdrop-blur-xs"
          onClick={() => setShowInspectModal(false)}
        >
          <div
            className="w-full max-w-xl bg-slate-900 border-l border-slate-800 h-full p-6 overflow-y-auto flex flex-col justify-between shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="space-y-6">
              <div className="flex items-start justify-between gap-4 border-b border-slate-800 pb-4">
                <div>
                  <div className="flex items-center gap-2">
                    <PriorityBadge priority={selectedAlert.priority} isActive={true} />
                    <StateBadge state={selectedAlert.state} activeCount={selectedAlert.activeCount} />
                    <span className="text-[11px] font-mono uppercase px-2 py-0.5 rounded bg-rose-500/20 text-rose-300 border border-rose-500/30">
                      Brique : {selectedAlert.component}
                    </span>
                  </div>
                  <h2 className="text-base font-semibold text-white mt-2">{selectedAlert.name}</h2>
                  <p className="text-xs font-mono text-slate-400 mt-0.5">ID: {selectedAlert.id}</p>
                </div>
                <button
                  onClick={() => setShowInspectModal(false)}
                  className="p-1.5 bg-slate-800 rounded-lg text-slate-400 hover:text-white cursor-pointer"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>

              <div className="flex items-center justify-between p-3.5 rounded-xl bg-slate-950 border border-slate-800">
                <div className="text-xs">
                  <div className="font-medium text-slate-200">Activation de la règle</div>
                  <div className="text-slate-400 text-[11px]">
                    {selectedAlert.enabled
                      ? `Schedule: ${selectedAlert.schedule}`
                      : 'Règle en sourdine (Muted)'}
                  </div>
                </div>
                <button
                  onClick={() => handleToggleMute(selectedAlert.id)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium cursor-pointer border ${
                    selectedAlert.enabled
                      ? 'bg-slate-800 border-slate-700 text-slate-200'
                      : 'bg-emerald-600/20 border-emerald-500/40 text-emerald-300'
                  }`}
                >
                  {selectedAlert.enabled ? (
                    <>
                      <BellOff className="w-3.5 h-3.5 inline mr-1" /> Mute
                    </>
                  ) : (
                    <>
                      <Volume2 className="w-3.5 h-3.5 inline mr-1" /> Unmute
                    </>
                  )}
                </button>
              </div>

              {selectedAlert.conditionDsl && (
                <div className="space-y-1.5">
                  <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-400 flex items-center gap-1.5">
                    <Terminal className="w-3.5 h-3.5 text-amber-400" /> Condition / Seuil
                  </h3>
                  <pre className="p-3.5 rounded-lg bg-slate-950 border border-slate-800 text-[11px] font-mono text-amber-200/90 overflow-x-auto whitespace-pre-wrap">
                    {selectedAlert.conditionDsl}
                  </pre>
                </div>
              )}

              <pre className="p-3.5 rounded-lg bg-slate-950 border border-slate-800 text-[11px] font-mono text-slate-400 overflow-x-auto">
                {JSON.stringify(selectedAlert, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// --- Sous-composants Graphiques ---

function ArchitectureBrick({
  comp,
  meta,
  stats,
  isHighlightedByAlert,
  isFiltered,
  selectedAlert,
  onClickFilter,
  icon,
}: {
  comp: ArchitectureComponent;
  meta: { title: string; subtitle: string; role: string };
  stats: { active: number; resolved: number; total: number };
  isHighlightedByAlert: boolean;
  isFiltered: boolean;
  selectedAlert: UnifiedAlert | null;
  onClickFilter: () => void;
  icon: React.ReactNode;
}) {
  return (
    <div
      onClick={onClickFilter}
      className={`p-3.5 rounded-xl border transition-all cursor-pointer relative ${
        isHighlightedByAlert
          ? 'bg-rose-950/90 border-rose-400 ring-4 ring-rose-500/60 shadow-[0_0_30px_rgba(244,63,94,0.55)] scale-[1.02] z-10'
          : isFiltered
          ? 'bg-indigo-950/60 border-indigo-400 ring-2 ring-indigo-500/40'
          : stats.active > 0
          ? 'bg-rose-950/25 border-rose-800/80 hover:border-rose-600'
          : 'bg-slate-950/80 border-slate-800 hover:border-slate-700'
      }`}
    >
      {isHighlightedByAlert && (
        <div className="mb-2 px-2 py-0.5 rounded-full bg-rose-500 text-white text-[9px] font-mono font-bold uppercase tracking-wider shadow-md inline-flex items-center gap-1">
          <Flame className="w-3 h-3 fill-current" /> Brique Impactée ({selectedAlert?.priority})
        </div>
      )}

      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2.5">
          <div
            className={`p-2 rounded-lg border ${
              isHighlightedByAlert
                ? 'bg-rose-500 text-white border-rose-300'
                : stats.active > 0
                ? 'bg-rose-500/15 text-rose-400 border-rose-500/30'
                : 'bg-slate-900 text-indigo-400 border-slate-800'
            }`}
          >
            {icon}
          </div>
          <div>
            <h3 className="text-xs font-bold text-white flex items-center gap-1.5">
              <span>{meta.title}</span>
              <span className="text-[10px] font-mono uppercase text-slate-400">#{comp}</span>
            </h3>
            <p className="text-[10px] text-slate-400">{meta.subtitle}</p>
          </div>
        </div>

        <div className="text-right">
          {stats.active > 0 ? (
            <span
              className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-mono font-bold ${
                isHighlightedByAlert
                  ? 'bg-white text-rose-600'
                  : 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
              }`}
            >
              <span className="w-1.5 h-1.5 rounded-full bg-rose-400 animate-ping" />
              {stats.active} active{stats.active > 1 ? 's' : ''}
            </span>
          ) : (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-mono bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
              OK ({stats.total})
            </span>
          )}
        </div>
      </div>

      <div className="mt-2.5 pt-2 border-t border-slate-800/70 flex items-center justify-between text-[10px] text-slate-400">
        <span className="truncate">{meta.role}</span>
        <span className="font-mono text-slate-500 shrink-0 ml-2">
          {isFiltered ? 'Filtre actif' : 'Filtrer'}
        </span>
      </div>
    </div>
  );
}

function PipelineConnector({ danger, label }: { danger: boolean; label: string }) {
  return (
    <div className="hidden md:flex flex-col items-center justify-center px-1">
      <span
        className={`text-[9px] font-mono mb-1 ${
          danger ? 'text-rose-400 font-semibold' : 'text-slate-500'
        }`}
      >
        {label}
      </span>
      <div
        className={`w-full h-0.5 relative flex items-center ${
          danger ? 'bg-rose-500 shadow-[0_0_10px_rgba(244,63,94,0.8)]' : 'bg-slate-800'
        }`}
      >
        <div
          className={`w-2 h-2 rotate-45 border-t-2 border-r-2 absolute right-0 ${
            danger ? 'border-rose-400' : 'border-slate-600'
          }`}
        />
      </div>
    </div>
  );
}

function PriorityBadge({ priority, isActive }: { priority: string; isActive: boolean }) {
  const p = (priority || 'P3').toUpperCase();
  const styles: Record<string, string> = {
    P1: isActive
      ? 'bg-rose-500 text-white border-rose-400 shadow-xs shadow-rose-500/50'
      : 'bg-rose-500/15 text-rose-400 border-rose-500/30',
    P2: 'bg-orange-500/15 text-orange-400 border-orange-500/30',
    P3: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
    P4: 'bg-slate-800 text-slate-400 border-slate-700',
  };
  return (
    <span
      className={`inline-flex items-center justify-center w-7 py-0.5 rounded text-[10px] font-mono font-bold border shrink-0 ${
        styles[p] || styles.P3
      }`}
    >
      {p}
    </span>
  );
}

function KpiCard({ active, onClick, title, subtitle, count, variant, icon }: any) {
  const styles = {
    danger: active
      ? 'bg-rose-950/60 border-rose-500 text-rose-200'
      : 'bg-slate-900/50 border-slate-800/80 hover:border-slate-700',
    emerald: active
      ? 'bg-emerald-950/50 border-emerald-500 text-emerald-200'
      : 'bg-slate-900/50 border-slate-800/80 hover:border-slate-700',
    neutral: active
      ? 'bg-indigo-950/50 border-indigo-500 text-indigo-200'
      : 'bg-slate-900/50 border-slate-800/80 hover:border-slate-700',
  }[variant as string];
  return (
    <button
      onClick={onClick}
      className={`text-left p-3.5 rounded-xl border transition-all cursor-pointer ${styles}`}
    >
      <div className="flex justify-between text-slate-400">
        <span className="text-xs font-medium text-slate-200">{title}</span>
        {icon}
      </div>
      <div className="mt-2.5 flex justify-between gap-2 items-baseline">
        <span className="text-2xl font-bold font-mono text-white">{count}</span>
        <span className="text-[10px] text-slate-400 truncate">{subtitle}</span>
      </div>
    </button>
  );
}

function StateBadge({ state, activeCount }: { state: AlertState; activeCount: number }) {
  if (state === 'ACTIVE')
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-rose-500/15 text-rose-400 border border-rose-500/30">
        <span className="w-1.5 h-1.5 rounded-full bg-rose-400 animate-ping" />
        ACTIVE {activeCount > 1 ? `(${activeCount})` : ''}
      </span>
    );
  if (state === 'RESOLVED')
    return (
      <span className="inline-flex px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/25">
        RÉSOLUE
      </span>
    );
  if (state === 'MUTED')
    return (
      <span className="inline-flex px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-slate-800 text-slate-500">
        MUTED
      </span>
    );
  return (
    <span className="inline-flex px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-slate-800/60 text-slate-400 border border-slate-800">
      OK
    </span>
  );
}

function Sparkline24h({ buckets, isActive }: { buckets: number[]; isActive: boolean }) {
  const bars = Array.isArray(buckets) && buckets.length === 24 ? buckets : new Array(24).fill(0);
  const max = Math.max(...bars, 1);
  return (
    <div className="flex items-end gap-[2px] h-5 px-1.5 py-0.5 rounded bg-slate-950/80 border border-slate-800/80">
      {bars.map((val: number, idx: number) => (
        <div
          key={idx}
          style={{ height: `${val === 0 ? 15 : Math.max(30, Math.round((val / max) * 100))}%` }}
          className={`w-1 rounded-xs transition-all ${
            val === 0 ? 'bg-slate-800' : isActive && idx >= 21 ? 'bg-rose-400' : 'bg-indigo-400/80'
          }`}
        />
      ))}
    </div>
  );
}
