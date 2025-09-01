package analytics

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AnalyticsEngine struct {
	requestLogs *mongo.Collection
}

type RequestData struct {
	ClientID    string    `bson:"client_id"`
	Timestamp   time.Time `bson:"timestamp"`
	Path        string    `bson:"path"`
	Method      string    `bson:"method"`
	StatusCode  int       `bson:"status_code"`
	LatencyMs   int64     `bson:"response_latency_ms"`
	IsThrottled bool      `bson:"is_throttled"`
	IsAnomalous bool      `bson:"is_anomalous"`
}

type ClientAnalytics struct {
	ClientID         string                 `json:"client_id"`
	TimeRange        TimeRange              `json:"time_range"`
	RequestMetrics   RequestMetrics         `json:"request_metrics"`
	LatencyMetrics   LatencyMetrics         `json:"latency_metrics"`
	BehaviorPatterns BehaviorPatterns       `json:"behavior_patterns"`
	AnomalyAnalysis  AnomalyAnalysis        `json:"anomaly_analysis"`
	PathAnalysis     map[string]PathMetrics `json:"path_analysis"`
	CalculatedAt     time.Time              `json:"calculated_at"`
}

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type RequestMetrics struct {
	TotalRequests      int64   `json:"total_requests"`
	SuccessfulRequests int64   `json:"successful_requests"`
	ThrottledRequests  int64   `json:"throttled_requests"`
	AnomalousRequests  int64   `json:"anomalous_requests"`
	ErrorRequests      int64   `json:"error_requests"`
	RequestsPerMinute  float64 `json:"requests_per_minute"`
	SuccessRate        float64 `json:"success_rate"`
	ThrottleRate       float64 `json:"throttle_rate"`
	AnomalyRate        float64 `json:"anomaly_rate"`
}

type LatencyMetrics struct {
	Average float64 `json:"average"`
	Median  float64 `json:"median"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
	Min     int64   `json:"min"`
	Max     int64   `json:"max"`
	StdDev  float64 `json:"std_dev"`
}

type BehaviorPatterns struct {
	IsRegular           bool     `json:"is_regular"`
	HasBursts           bool     `json:"has_bursts"`
	BurstCount          int      `json:"burst_count"`
	PeakHours           []int    `json:"peak_hours"`
	RequestDistribution string   `json:"request_distribution"`
	Observations        []string `json:"observations"`
}

type AnomalyAnalysis struct {
	AnomalyPeriods   []AnomalyPeriod `json:"anomaly_periods"`
	TotalAnomalyTime time.Duration   `json:"total_anomaly_time"`
	AnomalyTriggers  []string        `json:"anomaly_triggers"`
	RecoveryPatterns []string        `json:"recovery_patterns"`
}

type AnomalyPeriod struct {
	Start     time.Time     `json:"start"`
	End       time.Time     `json:"end"`
	Duration  time.Duration `json:"duration"`
	Intensity string        `json:"intensity"`
	Trigger   string        `json:"trigger"`
}

type PathMetrics struct {
	Path           string  `json:"path"`
	RequestCount   int64   `json:"request_count"`
	AverageLatency float64 `json:"average_latency"`
	ErrorRate      float64 `json:"error_rate"`
	ThrottleRate   float64 `json:"throttle_rate"`
}

func NewAnalyticsEngine(requestLogs *mongo.Collection) *AnalyticsEngine {
	return &AnalyticsEngine{
		requestLogs: requestLogs,
	}
}

func (ae *AnalyticsEngine) AnalyzeClient(ctx context.Context, clientID string, since time.Time) (*ClientAnalytics, error) {
	// Simple MongoDB query - get all client requests
	filter := bson.M{
		"client_id": clientID,
		"timestamp": bson.M{"$gte": since},
	}

	cursor, err := ae.requestLogs.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Load data into memory for Go processing
	var requests []RequestData
	for cursor.Next(ctx) {
		var req RequestData
		if err := cursor.Decode(&req); err != nil {
			continue
		}
		requests = append(requests, req)
	}

	if len(requests) == 0 {
		return &ClientAnalytics{
			ClientID:     clientID,
			TimeRange:    TimeRange{Start: since, End: time.Now()},
			CalculatedAt: time.Now(),
		}, nil
	}

	// Calculate all analytics in Go
	analytics := &ClientAnalytics{
		ClientID: clientID,
		TimeRange: TimeRange{
			Start: since,
			End:   time.Now(),
		},
		CalculatedAt: time.Now(),
	}

	analytics.RequestMetrics = ae.calculateRequestMetrics(requests, since)
	analytics.LatencyMetrics = ae.calculateLatencyMetrics(requests)
	analytics.BehaviorPatterns = ae.analyzeBehaviorPatterns(requests)
	analytics.AnomalyAnalysis = ae.analyzeAnomalyPatterns(requests)
	analytics.PathAnalysis = ae.analyzePathMetrics(requests)

	return analytics, nil
}

func (ae *AnalyticsEngine) calculateRequestMetrics(requests []RequestData, since time.Time) RequestMetrics {
	metrics := RequestMetrics{}

	for _, req := range requests {
		metrics.TotalRequests++

		if req.StatusCode < 400 {
			metrics.SuccessfulRequests++
		} else {
			metrics.ErrorRequests++
		}

		if req.IsThrottled {
			metrics.ThrottledRequests++
		}

		if req.IsAnomalous {
			metrics.AnomalousRequests++
		}
	}

	// Calculate rates
	timeSpan := time.Since(since).Minutes()
	if timeSpan > 0 {
		metrics.RequestsPerMinute = float64(metrics.TotalRequests) / timeSpan
	}

	if metrics.TotalRequests > 0 {
		metrics.SuccessRate = float64(metrics.SuccessfulRequests) / float64(metrics.TotalRequests) * 100
		metrics.ThrottleRate = float64(metrics.ThrottledRequests) / float64(metrics.TotalRequests) * 100
		metrics.AnomalyRate = float64(metrics.AnomalousRequests) / float64(metrics.TotalRequests) * 100
	}

	return metrics
}

func (ae *AnalyticsEngine) calculateLatencyMetrics(requests []RequestData) LatencyMetrics {
	if len(requests) == 0 {
		return LatencyMetrics{}
	}

	latencies := make([]int64, len(requests))
	sum := int64(0)
	min := requests[0].LatencyMs
	max := requests[0].LatencyMs

	for i, req := range requests {
		latencies[i] = req.LatencyMs
		sum += req.LatencyMs

		if req.LatencyMs < min {
			min = req.LatencyMs
		}
		if req.LatencyMs > max {
			max = req.LatencyMs
		}
	}

	// Sort for percentile calculations
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	average := float64(sum) / float64(len(latencies))

	// Calculate standard deviation
	variance := float64(0)
	for _, latency := range latencies {
		variance += math.Pow(float64(latency)-average, 2)
	}
	variance /= float64(len(latencies))
	stdDev := math.Sqrt(variance)

	return LatencyMetrics{
		Average: average,
		Median:  float64(latencies[len(latencies)/2]),
		P95:     float64(latencies[int(float64(len(latencies))*0.95)]),
		P99:     float64(latencies[int(float64(len(latencies))*0.99)]),
		Min:     min,
		Max:     max,
		StdDev:  stdDev,
	}
}

func (ae *AnalyticsEngine) analyzeBehaviorPatterns(requests []RequestData) BehaviorPatterns {
	patterns := BehaviorPatterns{
		Observations: make([]string, 0),
		PeakHours:    make([]int, 0),
	}

	if len(requests) < 10 {
		patterns.RequestDistribution = "insufficient_data"
		return patterns
	}

	// Analyze request distribution by hour
	hourCounts := make(map[int]int)
	for _, req := range requests {
		hour := req.Timestamp.Hour()
		hourCounts[hour]++
	}

	// Find peak hours
	maxRequests := 0
	for _, count := range hourCounts {
		if count > maxRequests {
			maxRequests = count
		}
	}

	for hour, count := range hourCounts {
		if count >= maxRequests*8/10 { // Within 80% of peak
			patterns.PeakHours = append(patterns.PeakHours, hour)
		}
	}

	// Detect burst patterns
	patterns.BurstCount = ae.detectBursts(requests)
	patterns.HasBursts = patterns.BurstCount > 0

	// Analyze regularity
	patterns.IsRegular = ae.isRegularPattern(requests)

	// Add observations
	if patterns.IsRegular {
		patterns.Observations = append(patterns.Observations, "Regular traffic pattern detected")
	}
	if patterns.HasBursts {
		patterns.Observations = append(patterns.Observations, fmt.Sprintf("Detected %d traffic bursts", patterns.BurstCount))
	}
	if len(patterns.PeakHours) > 0 {
		patterns.Observations = append(patterns.Observations, fmt.Sprintf("Peak activity hours: %v", patterns.PeakHours))
	}

	return patterns
}

func (ae *AnalyticsEngine) detectBursts(requests []RequestData) int {
	if len(requests) < 20 {
		return 0
	}

	burstCount := 0
	windowSize := 30 * time.Second
	burstThreshold := 15

	i := 0
	for i < len(requests)-burstThreshold {
		windowStart := requests[i].Timestamp
		requestsInWindow := 0
		j := i

		for j < len(requests) && requests[j].Timestamp.Sub(windowStart) <= windowSize {
			requestsInWindow++
			j++
		}

		if requestsInWindow >= burstThreshold {
			burstCount++
			i = j // Skip past this burst
		} else {
			i++
		}
	}

	return burstCount
}

func (ae *AnalyticsEngine) isRegularPattern(requests []RequestData) bool {
	if len(requests) < 60 {
		return false
	}

	// Calculate inter-request intervals
	intervals := make([]float64, len(requests)-1)
	for i := 1; i < len(requests); i++ {
		interval := requests[i].Timestamp.Sub(requests[i-1].Timestamp).Seconds()
		intervals = append(intervals, interval)
	}

	// Calculate coefficient of variation for regularity
	if len(intervals) == 0 {
		return false
	}

	// Mean interval
	sum := 0.0
	for _, interval := range intervals {
		sum += interval
	}
	mean := sum / float64(len(intervals))

	// Standard deviation
	variance := 0.0
	for _, interval := range intervals {
		variance += math.Pow(interval-mean, 2)
	}
	variance /= float64(len(intervals))
	stdDev := math.Sqrt(variance)

	// Coefficient of variation
	cv := stdDev / mean

	// Regular pattern if CV < 0.5 (low variability)
	return cv < 0.5
}

func (ae *AnalyticsEngine) analyzeAnomalyPatterns(requests []RequestData) AnomalyAnalysis {
	analysis := AnomalyAnalysis{
		AnomalyPeriods:   make([]AnomalyPeriod, 0),
		AnomalyTriggers:  make([]string, 0),
		RecoveryPatterns: make([]string, 0),
	}

	// Find anomaly periods
	var currentPeriod *AnomalyPeriod
	for i, req := range requests {
		if req.IsAnomalous {
			if currentPeriod == nil {
				// Start new anomaly period
				currentPeriod = &AnomalyPeriod{
					Start: req.Timestamp,
					End:   req.Timestamp,
				}
			} else {
				// Extend current period
				currentPeriod.End = req.Timestamp
			}
		} else if currentPeriod != nil {
			// End current anomaly period
			currentPeriod.Duration = currentPeriod.End.Sub(currentPeriod.Start)
			currentPeriod.Intensity = ae.calculateAnomalyIntensity(requests, i-10, i)
			currentPeriod.Trigger = ae.identifyAnomalyTrigger(requests, i-20, i-10)

			analysis.AnomalyPeriods = append(analysis.AnomalyPeriods, *currentPeriod)
			analysis.TotalAnomalyTime += currentPeriod.Duration
			currentPeriod = nil
		}
	}

	// Close any ongoing anomaly period
	if currentPeriod != nil {
		currentPeriod.Duration = currentPeriod.End.Sub(currentPeriod.Start)
		analysis.AnomalyPeriods = append(analysis.AnomalyPeriods, *currentPeriod)
		analysis.TotalAnomalyTime += currentPeriod.Duration
	}

	// Analyze triggers and patterns
	analysis.AnomalyTriggers = ae.identifyCommonTriggers(requests)
	analysis.RecoveryPatterns = ae.identifyRecoveryPatterns(requests)

	return analysis
}

func (ae *AnalyticsEngine) calculateAnomalyIntensity(requests []RequestData, start, end int) string {
	if start < 0 || end >= len(requests) || start >= end {
		return "unknown"
	}

	anomalyCount := 0
	totalCount := end - start

	for i := start; i < end; i++ {
		if requests[i].IsAnomalous {
			anomalyCount++
		}
	}

	ratio := float64(anomalyCount) / float64(totalCount)

	if ratio > 0.8 {
		return "high"
	} else if ratio > 0.5 {
		return "medium"
	} else {
		return "low"
	}
}

func (ae *AnalyticsEngine) identifyAnomalyTrigger(requests []RequestData, start, end int) string {
	if start < 0 || end >= len(requests) || start >= end {
		return "unknown"
	}

	// Look for patterns before anomaly
	requestCounts := make(map[time.Time]int)
	for i := start; i < end; i++ {
		minute := requests[i].Timestamp.Truncate(time.Minute)
		requestCounts[minute]++
	}

	// Find sudden spikes
	var counts []int
	for _, count := range requestCounts {
		counts = append(counts, count)
	}

	if len(counts) > 1 {
		maxCount := 0
		for _, count := range counts {
			if count > maxCount {
				maxCount = count
			}
		}

		avgCount := 0
		for _, count := range counts {
			avgCount += count
		}
		avgCount /= len(counts)

		if maxCount > avgCount*3 {
			return "traffic_spike"
		}
	}

	return "gradual_increase"
}

func (ae *AnalyticsEngine) identifyCommonTriggers(requests []RequestData) []string {
	triggers := make([]string, 0)

	// Analyze request patterns
	pathCounts := make(map[string]int)
	methodCounts := make(map[string]int)

	for _, req := range requests {
		if req.IsAnomalous {
			pathCounts[req.Path]++
			methodCounts[req.Method]++
		}
	}

	// Find dominant patterns
	totalAnomalous := 0
	for _, count := range pathCounts {
		totalAnomalous += count
	}

	for path, count := range pathCounts {
		if count > totalAnomalous/2 {
			triggers = append(triggers, fmt.Sprintf("High traffic to %s", path))
		}
	}

	for method, count := range methodCounts {
		if count > totalAnomalous*7/10 {
			triggers = append(triggers, fmt.Sprintf("Dominated by %s requests", method))
		}
	}

	if len(triggers) == 0 {
		triggers = append(triggers, "Mixed traffic patterns")
	}

	return triggers
}

func (ae *AnalyticsEngine) identifyRecoveryPatterns(requests []RequestData) []string {
	patterns := make([]string, 0)

	// Simple recovery analysis
	anomalyPeriods := 0
	recoveryTimes := make([]time.Duration, 0)

	var anomalyStart *time.Time
	for _, req := range requests {
		if req.IsAnomalous && anomalyStart == nil {
			anomalyStart = &req.Timestamp
		} else if !req.IsAnomalous && anomalyStart != nil {
			recovery := req.Timestamp.Sub(*anomalyStart)
			recoveryTimes = append(recoveryTimes, recovery)
			anomalyPeriods++
			anomalyStart = nil
		}
	}

	if len(recoveryTimes) > 0 {
		totalRecovery := time.Duration(0)
		for _, rt := range recoveryTimes {
			totalRecovery += rt
		}
		avgRecovery := totalRecovery / time.Duration(len(recoveryTimes))

		patterns = append(patterns, fmt.Sprintf("Average recovery time: %v", avgRecovery.Round(time.Second)))

		if avgRecovery < 5*time.Minute {
			patterns = append(patterns, "Fast recovery pattern")
		} else if avgRecovery > 30*time.Minute {
			patterns = append(patterns, "Slow recovery pattern")
		} else {
			patterns = append(patterns, "Normal recovery pattern")
		}
	}

	return patterns
}

func (ae *AnalyticsEngine) analyzePathMetrics(requests []RequestData) map[string]PathMetrics {
	pathStats := make(map[string]*PathMetrics)

	// Collect per-path statistics
	for _, req := range requests {
		metrics, exists := pathStats[req.Path]
		if !exists {
			metrics = &PathMetrics{
				Path: req.Path,
			}
			pathStats[req.Path] = metrics
		}

		metrics.RequestCount++

		if req.IsThrottled {
			// Count for throttle rate calculation
		}

		// Update average latency (running average)
		if metrics.RequestCount == 1 {
			metrics.AverageLatency = float64(req.LatencyMs)
		} else {
			metrics.AverageLatency = (metrics.AverageLatency*float64(metrics.RequestCount-1) + float64(req.LatencyMs)) / float64(metrics.RequestCount)
		}
	}

	// Convert to final format and calculate rates
	result := make(map[string]PathMetrics)
	for path, metrics := range pathStats {
		// Calculate final rates
		throttleCount := int64(0)
		errorCount := int64(0)

		for _, req := range requests {
			if req.Path == path {
				if req.IsThrottled {
					throttleCount++
				}
				if req.StatusCode >= 400 {
					errorCount++
				}
			}
		}

		metrics.ThrottleRate = float64(throttleCount) / float64(metrics.RequestCount) * 100
		metrics.ErrorRate = float64(errorCount) / float64(metrics.RequestCount) * 100

		result[path] = *metrics
	}

	return result
}

// Calculate baseline statistics using simple Go code
func (ae *AnalyticsEngine) CalculateBaseline(ctx context.Context, clientID string, windowHours int) (*BaselineStats, error) {
	since := time.Now().Add(-time.Duration(windowHours) * time.Hour)

	// Simple query to get request timestamps
	filter := bson.M{
		"client_id":    clientID,
		"timestamp":    bson.M{"$gte": since},
		"is_throttled": false, // Exclude throttled requests from baseline
	}

	cursor, err := ae.requestLogs.Find(ctx, filter, options.Find().SetProjection(bson.M{"timestamp": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Collect timestamps
	var timestamps []time.Time
	for cursor.Next(ctx) {
		var doc struct {
			Timestamp time.Time `bson:"timestamp"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		timestamps = append(timestamps, doc.Timestamp)
	}

	if len(timestamps) < 50 {
		return nil, fmt.Errorf("insufficient data: need at least 50 requests, got %d", len(timestamps))
	}

	// Group by minute and calculate rates
	minuteRates := ae.calculateMinuteRatesFromTimestamps(timestamps)

	if len(minuteRates) < 10 {
		return nil, fmt.Errorf("insufficient time coverage: need at least 10 minutes of data")
	}

	// Calculate statistics
	mean := ae.calculateMean(minuteRates)
	stdDev := ae.calculateStandardDeviation(minuteRates, mean)

	return &BaselineStats{
		ClientID:          clientID,
		AvgRequestsPerMin: mean,
		StdDeviation:      stdDev,
		SampleCount:       len(minuteRates),
		WindowHours:       windowHours,
		CalculatedAt:      time.Now(),
	}, nil
}

type BaselineStats struct {
	ClientID          string    `json:"client_id"`
	AvgRequestsPerMin float64   `json:"avg_requests_per_minute"`
	StdDeviation      float64   `json:"std_deviation"`
	SampleCount       int       `json:"sample_count"`
	WindowHours       int       `json:"window_hours"`
	CalculatedAt      time.Time `json:"calculated_at"`
}

func (ae *AnalyticsEngine) calculateMinuteRatesFromTimestamps(timestamps []time.Time) []float64 {
	// Group by minute
	minuteCounts := make(map[int64]int)

	for _, ts := range timestamps {
		minute := ts.Unix() / 60
		minuteCounts[minute]++
	}

	// Convert to rates
	rates := make([]float64, 0, len(minuteCounts))
	for _, count := range minuteCounts {
		rates = append(rates, float64(count))
	}

	return rates
}

func (ae *AnalyticsEngine) calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func (ae *AnalyticsEngine) calculateStandardDeviation(values []float64, mean float64) float64 {
	if len(values) <= 1 {
		return 0
	}

	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	variance /= float64(len(values) - 1) // Sample standard deviation

	return math.Sqrt(variance)
}
