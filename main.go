package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

//go:embed all:web/dist
var staticFiles embed.FS

type AlertState string

const (
	StateActive   AlertState = "ACTIVE"
	StateResolved AlertState = "RESOLVED"
	StateOK       AlertState = "OK"
	StateMuted    AlertState = "MUTED"
)

type UnifiedAlert struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Source        string     `json:"source"`    // "watcher" | "kibana_rule"
	Component     string     `json:"component"` // "otel" | "kafka" | "vector" | "clickhouse" | "elasticsearch"
	Priority      string     `json:"priority"`  // "P1" | "P2" | "P3" | "P4"
	Severity      string     `json:"severity"`  // "critical" | "high" | "medium" | "low"
	Enabled       bool       `json:"enabled"`
	State         AlertState `json:"state"`
	Schedule      string     `json:"schedule"`
	ActiveCount   int        `json:"activeCount"`
	FireCount24h  int        `json:"fireCount24h"`
	HourlyBuckets []int      `json:"hourlyBuckets"`
	TriggeredAt   *time.Time `json:"triggeredAt,omitempty"`
	ResolvedAt    *time.Time `json:"resolvedAt,omitempty"`
	LastCheckedAt *time.Time `json:"lastCheckedAt,omitempty"`
	SummaryReason string     `json:"summaryReason,omitempty"`
	ConditionDSL  string     `json:"conditionDsl,omitempty"`
	Tags          []string   `json:"tags,omitempty"`
}

type ClusterSnapshot struct {
	UpdatedAt     time.Time      `json:"updatedAt"`
	QueryDuration int64          `json:"queryDurationMs"`
	ESHealthy     bool           `json:"esHealthy"`
	DemoMode      bool           `json:"demoMode"`
	ClusterName   string         `json:"clusterName"`
	ErrorMessage  string         `json:"errorMessage,omitempty"`
	Alerts        []UnifiedAlert `json:"alerts"`
}

// --- Structures _msearch Elasticsearch ---

type MSearchResponse struct {
	Responses []json.RawMessage `json:"responses"`
}

type WatchesSearchResp struct {
	Hits struct {
		Hits []struct {
			ID     string `json:"_id"`
			Source struct {
				Metadata map[string]interface{} `json:"metadata"`
				Trigger  struct {
					Schedule map[string]interface{} `json:"schedule"`
				} `json:"trigger"`
				Condition map[string]interface{} `json:"condition"`
				Status    struct {
					State struct {
						Active bool `json:"active"`
					} `json:"state"`
					LastChecked      *time.Time `json:"last_checked"`
					LastMetCondition *time.Time `json:"last_met_condition"`
				} `json:"_status"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type KibanaRulesResp struct {
	Hits struct {
		Hits []struct {
			ID     string `json:"_id"`
			Source struct {
				Alert struct {
					Name        string   `json:"name"`
					Enabled     bool     `json:"enabled"`
					AlertTypeID string   `json:"alertTypeId"`
					Tags        []string `json:"tags"`
					Schedule    struct {
						Interval string `json:"interval"`
					} `json:"schedule"`
				} `json:"alert"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type KibanaAlertsDataResp struct {
	Hits struct {
		Hits []struct {
			Source struct {
				Kibana struct {
					Alert struct {
						Status   string     `json:"status"`
						Severity string     `json:"severity"`
						Reason   string     `json:"reason"`
						Start    *time.Time `json:"start"`
						End      *time.Time `json:"end"`
						Rule     struct {
							UUID string `json:"uuid"`
							Name string `json:"name"`
						} `json:"rule"`
					} `json:"alert"`
				} `json:"kibana"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type WatcherHistoryResp struct {
	Aggregations struct {
		ByWatch struct {
			Buckets []struct {
				Key        string `json:"key"`
				RecentRuns struct {
					Hits struct {
						Hits []struct {
							Source struct {
								Result struct {
									ExecutionTime time.Time `json:"execution_time"`
									Condition     struct {
										Met bool `json:"met"`
									} `json:"condition"`
								} `json:"result"`
							} `json:"_source"`
						} `json:"hits"`
					} `json:"hits"`
				} `json:"recent_runs"`
				TriggeredRuns struct {
					DocCount    int `json:"doc_count"`
					LastFiredAt struct {
						ValueAsString *time.Time `json:"value_as_string"`
					} `json:"last_fired_at"`
				} `json:"triggered_runs"`
			} `json:"buckets"`
		} `json:"by_watch"`
	} `json:"aggregations"`
}

type AlertEngine struct {
	esURL      string
	esAPIKey   string
	esUsername string
	esPassword string
	demoMode   bool
	httpClient *http.Client

	mu       sync.RWMutex
	snapshot ClusterSnapshot

	clientsMu sync.Mutex
	clients   map[chan []byte]struct{}
}

func NewAlertEngine(esURL, esAPIKey, esUser, esPass string, demoMode, insecureTLS bool) *AlertEngine {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureTLS},
		MaxIdleConns:    10,
		IdleConnTimeout: 30 * time.Second,
	}
	eng := &AlertEngine{
		esURL:      strings.TrimRight(esURL, "/"),
		esAPIKey:   esAPIKey,
		esUsername: esUser,
		esPassword: esPass,
		demoMode:   demoMode,
		httpClient: &http.Client{Transport: tr, Timeout: 8 * time.Second},
		clients:    make(map[chan []byte]struct{}),
		// IMPORTANT : Toujours initialiser Alerts avec un slice vide non-nil
		snapshot: ClusterSnapshot{
			UpdatedAt:   time.Now().UTC(),
			ESHealthy:   true,
			DemoMode:    demoMode,
			ClusterName: esURL,
			Alerts:      make([]UnifiedAlert, 0),
		},
	}
	if demoMode {
		eng.initDemoState()
	}
	return eng
}

func timePtr(t time.Time) *time.Time { return &t }

func derivePriority(explicitPriority, severity string, tags []string) string {
	p := strings.ToUpper(strings.TrimSpace(explicitPriority))
	if p == "P1" || p == "P2" || p == "P3" || p == "P4" {
		return p
	}
	for _, t := range tags {
		tu := strings.ToUpper(strings.TrimSpace(t))
		if tu == "P1" || tu == "PRIORITY:P1" || tu == "SEV1" {
			return "P1"
		}
		if tu == "P2" || tu == "PRIORITY:P2" || tu == "SEV2" {
			return "P2"
		}
		if tu == "P3" || tu == "PRIORITY:P3" || tu == "SEV3" {
			return "P3"
		}
		if tu == "P4" || tu == "PRIORITY:P4" || tu == "SEV4" {
			return "P4"
		}
	}
	switch strings.ToLower(severity) {
	case "critical":
		return "P1"
	case "high":
		return "P2"
	case "medium":
		return "P3"
	default:
		return "P4"
	}
}

// Déduit automatiquement la brique d'architecture impactée (otel, kafka, vector, clickhouse, elasticsearch)
func deriveComponent(explicitComp, name, id string, tags []string) string {
	c := strings.ToLower(strings.TrimSpace(explicitComp))
	if c == "otel" || c == "kafka" || c == "vector" || c == "clickhouse" || c == "elasticsearch" {
		return c
	}
	hay := strings.ToLower(name + " " + id + " " + strings.Join(tags, " "))
	switch {
	case strings.Contains(hay, "kafka") || strings.Contains(hay, "consumer") || strings.Contains(hay, "broker") || strings.Contains(hay, "topic"):
		return "kafka"
	case strings.Contains(hay, "vector") || strings.Contains(hay, "vrl") || strings.Contains(hay, "transform"):
		return "vector"
	case strings.Contains(hay, "clickhouse") || strings.Contains(hay, "mergetree") || strings.Contains(hay, "olap"):
		return "clickhouse"
	case strings.Contains(hay, "otel") || strings.Contains(hay, "opentelemetry") || strings.Contains(hay, "envoy") || strings.Contains(hay, "collector") || strings.Contains(hay, "span"):
		return "otel"
	default:
		return "elasticsearch"
	}
}

func (e *AlertEngine) initDemoState() {
	now := time.Now().UTC()
	demoAlerts := []UnifiedAlert{
		// --- BRIQUE : KAFKA ---
		{
			ID:            "kib-kafka-consumer-lag",
			Name:          "Kafka Consumer Group Lag > 50k msgs (topic: otel-logs-prod)",
			Source:        "kibana_rule",
			Component:     "kafka",
			Priority:      "P1",
			Severity:      "critical",
			Enabled:       true,
			State:         StateActive,
			Schedule:      "1m",
			ActiveCount:   2,
			FireCount24h:  11,
			HourlyBuckets: []int{0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 3, 3},
			TriggeredAt:   timePtr(now.Add(-8 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-10 * time.Second)),
			SummaryReason: "Consumer group 'vector-clickhouse-sink' accuse un retard de 148 200 messages sur les partitions 0..11.",
			ConditionDSL:  `alertTypeId: "metrics.alert.threshold" • kafka.consumer.records_lag_max > 50000 FOR 5m`,
			Tags:          []string{"P1", "kafka", "lag", "production"},
		},
		{
			ID:            "watch-kafka-under-replicated",
			Name:          "Kafka Under-Replicated Partitions > 0 (broker-02)",
			Source:        "watcher",
			Component:     "kafka",
			Priority:      "P2",
			Severity:      "high",
			Enabled:       true,
			State:         StateResolved,
			Schedule:      `{"interval":"30s"}`,
			ActiveCount:   0,
			FireCount24h:  3,
			HourlyBuckets: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0},
			TriggeredAt:   timePtr(now.Add(-24 * time.Minute)),
			ResolvedAt:    timePtr(now.Add(-6 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-5 * time.Second)),
			SummaryReason: "ISR resynchronisé sur les 64 partitions après redémarrage réseau de kafka-broker-02.",
			ConditionDSL:  `"condition": { "compare": { "ctx.payload.under_replicated": { "gt": 0 } } }`,
			Tags:          []string{"P2", "kafka", "isr"},
		},

		// --- BRIQUE : VECTOR ---
		{
			ID:            "watch-vector-buffer-drop",
			Name:          "Vector Sink Buffer Utilization >= 95% (Dropped Events)",
			Source:        "watcher",
			Component:     "vector",
			Priority:      "P1",
			Severity:      "critical",
			Enabled:       true,
			State:         StateActive,
			Schedule:      `{"interval":"30s"}`,
			ActiveCount:   1,
			FireCount24h:  7,
			HourlyBuckets: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 2, 3},
			TriggeredAt:   timePtr(now.Add(-12 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-7 * time.Second)),
			SummaryReason: "vector_buffer_byte_size dépasse 95% sur le sink 'clickhouse_logs' (backpressure aval détectée).",
			ConditionDSL:  `"condition": { "compare": { "ctx.payload.vector_buffer_pct": { "gte": 95 } } }`,
			Tags:          []string{"P1", "vector", "backpressure", "pipeline"},
		},
		{
			ID:            "kib-vector-vrl-parse-err",
			Name:          "Vector VRL Remap Parse Error Rate > 1%",
			Source:        "kibana_rule",
			Component:     "vector",
			Priority:      "P3",
			Severity:      "medium",
			Enabled:       true,
			State:         StateResolved,
			Schedule:      "2m",
			ActiveCount:   0,
			FireCount24h:  4,
			HourlyBuckets: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0},
			TriggeredAt:   timePtr(now.Add(-75 * time.Minute)),
			ResolvedAt:    timePtr(now.Add(-38 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-15 * time.Second)),
			SummaryReason: "Correction du schéma JSON d'entrée déployée sur le transform VRL 'parse_envoy_access'.",
			ConditionDSL:  `alertTypeId: "metrics.alert.threshold" • vector.component_errors_total > 100`,
			Tags:          []string{"P3", "vector", "vrl"},
		},

		// --- BRIQUE : CLICKHOUSE ---
		{
			ID:            "watch-clickhouse-mergetree-parts",
			Name:          "ClickHouse Max Part Count for Partition > 300 (Too Many Parts)",
			Source:        "watcher",
			Component:     "clickhouse",
			Priority:      "P2",
			Severity:      "high",
			Enabled:       true,
			State:         StateActive,
			Schedule:      `{"interval":"1m"}`,
			ActiveCount:   1,
			FireCount24h:  6,
			HourlyBuckets: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2},
			TriggeredAt:   timePtr(now.Add(-19 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-9 * time.Second)),
			SummaryReason: "Table 'otel.otel_logs' sur ch-shard-01 compte 318 parts actives (ralentissement des background merges).",
			ConditionDSL:  `"condition": { "compare": { "ctx.payload.ch_max_parts": { "gt": 300 } } }`,
			Tags:          []string{"P2", "clickhouse", "mergetree", "storage"},
		},
		{
			ID:            "kib-clickhouse-Slow-queries",
			Name:          "ClickHouse Query Duration p99 > 5s (Grafana Dashboards)",
			Source:        "kibana_rule",
			Component:     "clickhouse",
			Priority:      "P3",
			Severity:      "medium",
			Enabled:       true,
			State:         StateResolved,
			Schedule:      "1m",
			ActiveCount:   0,
			FireCount24h:  2,
			HourlyBuckets: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0},
			TriggeredAt:   timePtr(now.Add(-145 * time.Minute)),
			ResolvedAt:    timePtr(now.Add(-125 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-20 * time.Second)),
			SummaryReason: "Latence p99 redescendue à 320ms après matérialisation de la colonne trace_id.",
			ConditionDSL:  `alertTypeId: "metrics.alert.threshold" • clickhouse.query.duration_p99 > 5000`,
			Tags:          []string{"P3", "clickhouse", "performance"},
		},

		// --- BRIQUE : OTEL / INGRESS ---
		{
			ID:            "watch-otel-gateway-dropped-spans",
			Name:          "OTel Collector Gateway Export Queue Saturation",
			Source:        "watcher",
			Component:     "otel",
			Priority:      "P2",
			Severity:      "high",
			Enabled:       true,
			State:         StateResolved,
			Schedule:      `{"interval":"30s"}`,
			ActiveCount:   0,
			FireCount24h:  5,
			HourlyBuckets: []int{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0},
			TriggeredAt:   timePtr(now.Add(-22 * time.Minute)),
			ResolvedAt:    timePtr(now.Add(-9 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-6 * time.Second)),
			SummaryReason: "File d'export OTel Collector revenue à 18% après scale-up HPA.",
			ConditionDSL:  `"condition": { "compare": { "ctx.payload.queue_size_ratio": { "gt": 0.90 } } }`,
			Tags:          []string{"P2", "otel", "collector", "tracing"},
		},

		// --- BRIQUE : ELASTICSEARCH ---
		{
			ID:            "watch-es-jvm-heap-pressure",
			Name:          "Elasticsearch JVM Heap Pressure >= 88% (es-hot-02)",
			Source:        "watcher",
			Component:     "elasticsearch",
			Priority:      "P1",
			Severity:      "critical",
			Enabled:       true,
			State:         StateActive,
			Schedule:      `{"interval":"30s"}`,
			ActiveCount:   2,
			FireCount24h:  14,
			HourlyBuckets: []int{0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 2, 3, 3},
			TriggeredAt:   timePtr(now.Add(-16 * time.Minute)),
			LastCheckedAt: timePtr(now.Add(-8 * time.Second)),
			SummaryReason: "es-hot-02 maintient un JVM Heap à 91.8% pendant les agrégations terms lourdes.",
			ConditionDSL:  `"condition": { "compare": { "ctx.payload.aggregations.max_heap.value": { "gte": 88 } } }`,
			Tags:          []string{"P1", "elasticsearch", "jvm"},
		},
		{
			ID:            "watch-es-cluster-status-red",
			Name:          "Elasticsearch Cluster Status == RED",
			Source:        "watcher",
			Component:     "elasticsearch",
			Priority:      "P1",
			Severity:      "critical",
			Enabled:       true,
			State:         StateOK,
			Schedule:      `{"interval":"10s"}`,
			ActiveCount:   0,
			FireCount24h:  0,
			HourlyBuckets: make([]int, 24),
			LastCheckedAt: timePtr(now.Add(-3 * time.Second)),
			SummaryReason: "Cluster status GREEN (0 unassigned shards).",
			Tags:          []string{"P1", "elasticsearch", "availability"},
		},
	}

	e.sortAlerts(demoAlerts)
	e.snapshot = ClusterSnapshot{
		UpdatedAt:     now,
		QueryDuration: 12,
		ESHealthy:     true,
		DemoMode:      true,
		ClusterName:   "es-obs-prod-eu-west (Mode Démo SRE)",
		Alerts:        demoAlerts,
	}
}

func (e *AlertEngine) StartPoller(ctx context.Context, interval time.Duration) {
	if !e.demoMode {
		e.pollOnceES(ctx)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.demoMode {
				e.mu.Lock()
				e.snapshot.UpdatedAt = time.Now().UTC()
				e.snapshot.QueryDuration = int64(10 + rand.Intn(8))
				snap := e.snapshot
				e.mu.Unlock()
				e.broadcast(snap)
			} else {
				e.pollOnceES(ctx)
			}
		}
	}
}

func (e *AlertEngine) buildMSearchPayload() []byte {
	queries := []string{
		`{"index":".watches","ignore_unavailable":true}`,
		`{"size":1000,"_source":["metadata","trigger.schedule","condition","_status.state.active","_status.last_checked","_status.last_met_condition"]}`,
		`{"index":".kibana*","ignore_unavailable":true}`,
		`{"size":1000,"query":{"term":{"type":"alert"}},"_source":["alert.name","alert.enabled","alert.alertTypeId","alert.schedule.interval","alert.tags"]}`,
		`{"index":".internal.alerts-*","ignore_unavailable":true}`,
		`{"size":3000,"sort":[{"kibana.alert.start":{"order":"desc"}}],"query":{"bool":{"should":[{"term":{"kibana.alert.status":"active"}},{"bool":{"must":[{"term":{"kibana.alert.status":"recovered"}},{"range":{"kibana.alert.end":{"gte":"now-7d"}}}]}}],"minimum_should_match":1}},"_source":["kibana.alert.rule.uuid","kibana.alert.rule.name","kibana.alert.status","kibana.alert.severity","kibana.alert.start","kibana.alert.end","kibana.alert.reason"]}`,
		`{"index":".watcher-history-*","ignore_unavailable":true}`,
		`{"size":0,"query":{"range":{"result.execution_time":{"gte":"now-7d"}}},"aggs":{"by_watch":{"terms":{"field":"watch_id","size":1000},"aggs":{"recent_runs":{"top_hits":{"size":100,"sort":[{"result.execution_time":{"order":"desc"}}],"_source":["result.execution_time","result.condition.met"]}},"triggered_runs":{"filter":{"term":{"result.condition.met":true}},"aggs":{"last_fired_at":{"max":{"field":"result.execution_time"}}}}}}}}`,
	}
	return []byte(strings.Join(queries, "\n") + "\n")
}

func (e *AlertEngine) pollOnceES(ctx context.Context) {
	start := time.Now()
	payload := e.buildMSearchPayload()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.esURL+"/_msearch", bytes.NewReader(payload))
	if err != nil {
		e.recordError(err)
		return
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if e.esAPIKey != "" {
		req.Header.Set("Authorization", "ApiKey "+e.esAPIKey)
	} else if e.esUsername != "" {
		req.SetBasicAuth(e.esUsername, e.esPassword)
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		e.recordError(fmt.Errorf("connexion Elasticsearch impossible (%s): %w", e.esURL, err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		e.recordError(fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes)))
		return
	}

	var msearch MSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&msearch); err != nil || len(msearch.Responses) < 4 {
		e.recordError(fmt.Errorf("réponse _msearch invalide: %v", err))
		return
	}

	alerts := e.reconcile(msearch.Responses)
	e.sortAlerts(alerts)

	snap := ClusterSnapshot{
		UpdatedAt:     time.Now().UTC(),
		QueryDuration: time.Since(start).Milliseconds(),
		ESHealthy:     true,
		DemoMode:      false,
		ClusterName:   e.esURL,
		Alerts:        alerts,
	}

	e.mu.Lock()
	e.snapshot = snap
	e.mu.Unlock()
	e.broadcast(snap)
}

func (e *AlertEngine) recordError(err error) {
	log.Printf("[WARN] Poll Elasticsearch: %v", err)
	e.mu.Lock()
	e.snapshot.UpdatedAt = time.Now().UTC()
	e.snapshot.ESHealthy = false
	e.snapshot.ErrorMessage = err.Error()
	snap := e.snapshot
	e.mu.Unlock()
	e.broadcast(snap)
}

func (e *AlertEngine) reconcile(raw []json.RawMessage) []UnifiedAlert {
	var watchesInv WatchesSearchResp
	var kibanaInv KibanaRulesResp
	var kibanaData KibanaAlertsDataResp
	var watcherHist WatcherHistoryResp

	_ = json.Unmarshal(raw[0], &watchesInv)
	_ = json.Unmarshal(raw[1], &kibanaInv)
	_ = json.Unmarshal(raw[2], &kibanaData)
	_ = json.Unmarshal(raw[3], &watcherHist)

	unifiedMap := make(map[string]*UnifiedAlert)

	for _, hit := range watchesInv.Hits.Hits {
		name := hit.ID
		severity := "medium"
		explicitPrio := ""
		explicitComp := ""
		if n, ok := hit.Source.Metadata["name"].(string); ok && n != "" {
			name = n
		}
		if s, ok := hit.Source.Metadata["severity"].(string); ok && s != "" {
			severity = strings.ToLower(s)
		}
		if p, ok := hit.Source.Metadata["priority"].(string); ok && p != "" {
			explicitPrio = p
		}
		if c, ok := hit.Source.Metadata["component"].(string); ok && c != "" {
			explicitComp = c
		}
		schedBytes, _ := json.Marshal(hit.Source.Trigger.Schedule)
		condBytes, _ := json.Marshal(hit.Source.Condition)
		state := StateOK
		if !hit.Source.Status.State.Active {
			state = StateMuted
		}

		unifiedMap["watcher:"+hit.ID] = &UnifiedAlert{
			ID:            hit.ID,
			Name:          name,
			Source:        "watcher",
			Component:     deriveComponent(explicitComp, name, hit.ID, nil),
			Priority:      derivePriority(explicitPrio, severity, nil),
			Severity:      severity,
			Enabled:       hit.Source.Status.State.Active,
			State:         state,
			Schedule:      string(schedBytes),
			HourlyBuckets: make([]int, 24),
			LastCheckedAt: hit.Source.Status.LastChecked,
			TriggeredAt:   hit.Source.Status.LastMetCondition,
			ConditionDSL:  string(condBytes),
		}
	}

	for _, b := range watcherHist.Aggregations.ByWatch.Buckets {
		key := "watcher:" + b.Key
		item, exists := unifiedMap[key]
		if !exists {
			item = &UnifiedAlert{
				ID:            b.Key,
				Name:          b.Key,
				Source:        "watcher",
				Component:     deriveComponent("", b.Key, b.Key, nil),
				Priority:      "P3",
				Severity:      "medium",
				Enabled:       true,
				State:         StateOK,
				HourlyBuckets: make([]int, 24),
			}
			unifiedMap[key] = item
		}

		item.FireCount24h = b.TriggeredRuns.DocCount
		runs := b.RecentRuns.Hits.Hits
		if len(runs) == 0 || !item.Enabled {
			continue
		}

		now := time.Now().UTC()
		for _, r := range runs {
			if r.Source.Result.Condition.Met {
				hoursAgo := int(now.Sub(r.Source.Result.ExecutionTime).Hours())
				if hoursAgo >= 0 && hoursAgo < 24 {
					item.HourlyBuckets[23-hoursAgo]++
				}
			}
		}

		latest := runs[0].Source.Result
		item.LastCheckedAt = &latest.ExecutionTime

		if latest.Condition.Met {
			item.State = StateActive
			item.ActiveCount = 1
			tStart := latest.ExecutionTime
			for _, r := range runs {
				if r.Source.Result.Condition.Met {
					tStart = r.Source.Result.ExecutionTime
				} else {
					break
				}
			}
			item.TriggeredAt = &tStart
			item.SummaryReason = fmt.Sprintf("Condition franchie (%d exécutions positives sur la période)", item.FireCount24h)
		} else if b.TriggeredRuns.DocCount > 0 {
			item.State = StateResolved
			item.TriggeredAt = b.TriggeredRuns.LastFiredAt.ValueAsString
			resolvedTime := latest.ExecutionTime
			for i := 0; i < len(runs)-1; i++ {
				if !runs[i].Source.Result.Condition.Met && runs[i+1].Source.Result.Condition.Met {
					resolvedTime = runs[i].Source.Result.ExecutionTime
					tFired := runs[i+1].Source.Result.ExecutionTime
					item.TriggeredAt = &tFired
					break
				}
			}
			item.ResolvedAt = &resolvedTime
			item.SummaryReason = "Condition revenue à la normale"
		}
	}

	for _, hit := range kibanaInv.Hits.Hits {
		ruleUUID := strings.TrimPrefix(hit.ID, "alert:")
		state := StateOK
		if !hit.Source.Alert.Enabled {
			state = StateMuted
		}
		tags := hit.Source.Alert.Tags
		name := hit.Source.Alert.Name
		unifiedMap["kibana:"+ruleUUID] = &UnifiedAlert{
			ID:            ruleUUID,
			Name:          name,
			Source:        "kibana_rule",
			Component:     deriveComponent("", name, ruleUUID, tags),
			Priority:      derivePriority("", "medium", tags),
			Severity:      "medium",
			Enabled:       hit.Source.Alert.Enabled,
			State:         state,
			Schedule:      hit.Source.Alert.Schedule.Interval,
			HourlyBuckets: make([]int, 24),
			ConditionDSL:  fmt.Sprintf("alertTypeId: %s", hit.Source.Alert.AlertTypeID),
			Tags:          tags,
		}
	}

	now := time.Now().UTC()
	for _, hit := range kibanaData.Hits.Hits {
		al := hit.Source.Kibana.Alert
		if al.Rule.UUID == "" {
			continue
		}
		key := "kibana:" + al.Rule.UUID
		item, exists := unifiedMap[key]
		if !exists {
			item = &UnifiedAlert{
				ID:            al.Rule.UUID,
				Name:          al.Rule.Name,
				Source:        "kibana_rule",
				Component:     deriveComponent("", al.Rule.Name, al.Rule.UUID, nil),
				Priority:      derivePriority("", al.Severity, nil),
				Severity:      al.Severity,
				Enabled:       true,
				State:         StateOK,
				HourlyBuckets: make([]int, 24),
			}
			unifiedMap[key] = item
		}

		item.FireCount24h++
		if al.Start != nil {
			hoursAgo := int(now.Sub(*al.Start).Hours())
			if hoursAgo >= 0 && hoursAgo < 24 {
				item.HourlyBuckets[23-hoursAgo]++
			}
		}
		if al.Severity != "" {
			item.Severity = strings.ToLower(al.Severity)
			item.Priority = derivePriority("", item.Severity, item.Tags)
		}

		switch al.Status {
		case "active":
			item.State = StateActive
			item.ActiveCount++
			if item.TriggeredAt == nil || (al.Start != nil && al.Start.Before(*item.TriggeredAt)) {
				item.TriggeredAt = al.Start
			}
			if al.Reason != "" {
				item.SummaryReason = al.Reason
			}
		case "recovered":
			if item.State != StateActive && item.Enabled {
				item.State = StateResolved
				if item.ResolvedAt == nil || (al.End != nil && al.End.After(*item.ResolvedAt)) {
					item.ResolvedAt = al.End
					item.TriggeredAt = al.Start
					item.SummaryReason = al.Reason
				}
			}
		}
	}

	result := make([]UnifiedAlert, 0, len(unifiedMap))
	for _, v := range unifiedMap {
		result = append(result, *v)
	}
	return result
}

func (e *AlertEngine) sortAlerts(result []UnifiedAlert) {
	stateRank := map[AlertState]int{StateActive: 0, StateResolved: 1, StateOK: 2, StateMuted: 3}
	prioRank := map[string]int{"P1": 0, "P2": 1, "P3": 2, "P4": 3}

	sort.Slice(result, func(i, j int) bool {
		if stateRank[result[i].State] != stateRank[result[j].State] {
			return stateRank[result[i].State] < stateRank[result[j].State]
		}
		if prioRank[result[i].Priority] != prioRank[result[j].Priority] {
			return prioRank[result[i].Priority] < prioRank[result[j].Priority]
		}
		if result[i].State == StateResolved && result[i].ResolvedAt != nil && result[j].ResolvedAt != nil {
			return result[i].ResolvedAt.After(*result[j].ResolvedAt)
		}
		return result[i].Name < result[j].Name
	})
}

func (e *AlertEngine) handleToggleMute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	e.mu.Lock()
	for i := range e.snapshot.Alerts {
		if e.snapshot.Alerts[i].ID == req.ID {
			a := &e.snapshot.Alerts[i]
			a.Enabled = !a.Enabled
			if !a.Enabled {
				a.State = StateMuted
				a.ActiveCount = 0
			} else {
				a.State = StateOK
			}
			break
		}
	}
	e.sortAlerts(e.snapshot.Alerts)
	snap := e.snapshot
	e.mu.Unlock()

	e.broadcast(snap)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snap)
}

func (e *AlertEngine) handleSimulateEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	now := time.Now().UTC()
	e.mu.Lock()
	resolvedOne := false
	for i := range e.snapshot.Alerts {
		a := &e.snapshot.Alerts[i]
		if a.State == StateActive {
			a.State = StateResolved
			a.ActiveCount = 0
			a.ResolvedAt = &now
			a.SummaryReason = "Résolution automatique confirmée à l'instant par le poller Elasticsearch."
			resolvedOne = true
			break
		}
	}
	if !resolvedOne {
		for i := range e.snapshot.Alerts {
			a := &e.snapshot.Alerts[i]
			if a.State == StateOK || a.State == StateResolved {
				a.State = StateActive
				a.Enabled = true
				a.ActiveCount = 2
				a.FireCount24h++
				a.HourlyBuckets[23]++
				a.TriggeredAt = &now
				a.ResolvedAt = nil
				a.SummaryReason = "Seuil critique franchi en direct (Simulation d'incident SRE)."
				break
			}
		}
	}
	e.snapshot.UpdatedAt = now
	e.sortAlerts(e.snapshot.Alerts)
	snap := e.snapshot
	e.mu.Unlock()

	e.broadcast(snap)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snap)
}

func (e *AlertEngine) broadcast(snap ClusterSnapshot) {
	data, err := json.Marshal(snap)
	if err != nil {
		return
	}
	e.clientsMu.Lock()
	defer e.clientsMu.Unlock()
	for ch := range e.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

func (e *AlertEngine) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE non supporté", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := make(chan []byte, 4)
	e.clientsMu.Lock()
	e.clients[ch] = struct{}{}
	e.clientsMu.Unlock()

	defer func() {
		e.clientsMu.Lock()
		delete(e.clients, ch)
		e.clientsMu.Unlock()
	}()

	e.mu.RLock()
	initialBytes, _ := json.Marshal(e.snapshot)
	e.mu.RUnlock()
	fmt.Fprintf(w, "data: %s\n\n", initialBytes)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (e *AlertEngine) handleState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	e.mu.RLock()
	defer e.mu.RUnlock()
	_ = json.NewEncoder(w).Encode(e.snapshot)
}

func main() {
	esURL := os.Getenv("ES_URL")
	esAPIKey := os.Getenv("ES_API_KEY")
	esUser := os.Getenv("ES_USERNAME")
	esPass := os.Getenv("ES_PASSWORD")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	insecureTLS := os.Getenv("ES_INSECURE_TLS") == "true"
	demoMode := esURL == "" || os.Getenv("DEMO_MODE") == "true"

	engine := NewAlertEngine(esURL, esAPIKey, esUser, esPass, demoMode, insecureTLS)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go engine.StartPoller(ctx, 10*time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/stream", engine.handleSSE)
	mux.HandleFunc("/api/state", engine.handleState)
	mux.HandleFunc("/api/toggle-mute", engine.handleToggleMute)
	mux.HandleFunc("/api/simulate", engine.handleSimulateEvent)

	distFS, err := fs.Sub(staticFiles, "web/dist")
	if err == nil {
		mux.Handle("/", http.FileServer(http.FS(distFS)))
	}

	log.Printf("🚀 Elastic Alert Console prête sur http://localhost:%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
