# Neko Oracle RWA - Aggregator Service

Price aggregation, normalization, and consensus calculation service for the Oracle RWA system. This service implements a complete pipeline for processing raw price data into a reliable consensus price with circuit breaker protection and comprehensive metrics.

## 📋 Overview

The Aggregator service is responsible for:

1. **Price Reception** - Accept validated price data from sources
2. **Normalization** - Convert prices to standard format with precision rules
3. **Outlier Detection** - Identify and filter anomalous prices
4. **Aggregation** - Calculate consensus price from valid sources
5. **Storage** - Persist aggregated results for historical analysis

### Pipeline Flow

```
Price Input (PriceInputDto)
        ↓
[Reception] → PriceReceivedEvent
        ↓
[Normalization] → PriceNormalizedEvent
        ↓
[Outlier Detection] → filtered prices
        ↓
[Aggregation] → PriceAggregatedEvent
        ↓
[Storage] → persistent records
```

## ✨ Features

### Pipeline Architecture

- **Event-Driven Design**: Each stage emits events for observability and integration
- **Circuit Breaker Protection**: Automatic failure detection and recovery
- **Error Resilience**: Non-blocking pipeline with graceful degradation
- **Dual Processing Modes**: Real-time and batch aggregation
- **Comprehensive Metrics**: Execution timing and quality assessment

### Aggregation Methods

#### Weighted Average
- **Formula**: `Σ(price_i × weight_i) / Σ(weight_i)`
- **Best For**: Trusted data providers, stable markets
- **Complexity**: O(n)

#### Median
- **Formula**: Middle value after sorting
- **Best For**: Volatile markets, outlier resistance
- **Complexity**: O(n log n)

#### Trimmed Mean
- **Formula**: Average after removing extreme values
- **Best For**: Balanced approach, occasional outliers
- **Complexity**: O(n log n)

### Source Weighting

Trust-based source reliability configuration:

```typescript
{
  'Bloomberg': 2.0,      // Premium sources
  'Reuters': 2.0,
  'AlphaVantage': 1.5,   // High reliability
  'YahooFinance': 1.2,   // Standard reliability
  'Finnhub': 1.2,
  'default': 1.0,        // Fallback weight
}
```

### Quality Metrics

Each aggregated result includes:

- **Standard Deviation**: Price variance across sources
- **Coefficient of Variation**: Relative price spread
- **Source Count**: Number of valid sources used
- **Confidence Score**: Quality assessment (0-100)
- **Execution Time**: Stage-by-stage performance metrics

## 🏗️ Architecture

```
apps/aggregator/
├── src/
│   ├── controllers/
│   │   └── aggregator-orchestration.controller.ts   # REST API endpoints
│   │
│   ├── modules/
│   │   └── aggregator-orchestration.module.ts       # Main module
│   │
│   ├── services/
│   │   ├── orchestration.service.ts                 # Pipeline coordinator
│   │   ├── data-reception.service.ts                # Price validation
│   │   ├── normalization.service.ts                 # Format standardization
│   │   ├── outlier-detection.service.ts             # Anomaly filtering
│   │   ├── aggregation.service.ts                   # Consensus calculation
│   │   └── data-storage.service.ts                  # Persistence
│   │
│   ├── events/
│   │   └── price.events.ts                          # Event definitions
│   │       ├── PriceReceivedEvent
│   │       ├── PriceNormalizedEvent
│   │       └── PriceAggregatedEvent
│   │
│   ├── middleware/
│   │   └── circuit-breaker.middleware.ts            # Failure protection
│   │
│   ├── strategies/aggregators/
│   │   ├── weighted-average.aggregator.ts
│   │   ├── median.aggregator.ts
│   │   └── trimmed-mean.aggregator.ts
│   │
│   ├── config/
│   │   └── source-weights.config.ts                 # Weight configuration
│   │
│   ├── dto/
│   │   └── price-input.dto.ts                       # Input validation
│   │
│   └── app.module.ts                                # Application root
│
└── .env.example                                      # Environment template
```

## 🚀 Getting Started

### Prerequisites

- Node.js >= 18
- npm >= 9
- NestJS >= 10

### Installation

```bash
# Install dependencies
npm install

# Copy environment file
cp .env.example .env

# Configure environment variables
# Edit .env with your settings
```

### Development

```bash
# Start in development mode
npm run start:dev

# The service will start on http://localhost:3001
```

### Production

```bash
# Build the application
npm run build

# Start production service
npm start
```

## ⚙️ Configuration

### Environment Variables

Create `.env` file from `.env.example`:

```bash
# Server Configuration
PORT=3001
NODE_ENV=development

# Processing Modes
AGGREGATION_REALTIME_ENABLED=true
AGGREGATION_BATCH_ENABLED=false
AGGREGATION_BATCH_INTERVAL_MS=5000
AGGREGATION_BATCH_MIN_PRICES=3

# Aggregation Strategy
AGGREGATION_METHOD=WEIGHTED_MEDIAN
AGGREGATION_MIN_SOURCES=2
AGGREGATION_MAX_SOURCES=50

# Outlier Detection
OUTLIER_METHOD=MODIFIED_Z_SCORE
OUTLIER_THRESHOLD=0.03

# Normalization
NORMALIZATION_ENABLED=true

# Quality Thresholds
MIN_CONFIDENCE=0.7
MAX_CV=0.05

# Timeouts (milliseconds)
NORMALIZATION_TIMEOUT_MS=5000
OUTLIER_DETECTION_TIMEOUT_MS=3000
AGGREGATION_TIMEOUT_MS=5000
STORAGE_TIMEOUT_MS=10000

# Circuit Breaker
CB_FAILURE_THRESHOLD=5
CB_SUCCESS_THRESHOLD=2
CB_TIMEOUT_MS=30000
CB_HALF_OPEN_MAX_REQUESTS=3
CB_ENABLE_LOGGING=true

# Storage
STORAGE_ENABLED=true
STORAGE_RETENTION_DAYS=30

# Metrics & Logging
METRICS_ENABLED=true
METRICS_INTERVAL_MS=60000
VERBOSE_LOGGING=false
```

### Source Weights Configuration

Edit `src/config/source-weights.config.ts`:

```typescript
export const SOURCE_WEIGHTS: Record<string, number> = {
  'Bloomberg': 2.0,      // Premium (highest reliability)
  'Reuters': 2.0,
  'AlphaVantage': 1.5,   // High reliability
  'YahooFinance': 1.2,   // Standard reliability
  'Finnhub': 1.2,
  'IEX Cloud': 1.2,
  'Polygon': 1.0,        // Baseline reliability
  'default': 1.0,        // Unknown sources
};
```

**Weight Guidelines:**
- `2.0`: Premium institutional sources
- `1.2-1.5`: Trusted commercial APIs
- `1.0`: Standard baseline
- `0.5-0.8`: Lower priority sources
- `0.0`: Disabled/excluded

## 🔄 Pipeline Stages

### Stage 1: Reception
```typescript
PriceInputDto
  ├─ symbol: string
  ├─ price: number
  ├─ source: string
  └─ timestamp: ISO string
           ↓
    PriceReceivedEvent
```
**Responsibility**: Validate input and create source-weighted event

### Stage 2: Normalization
```typescript
PriceReceivedEvent
  ├─ Convert to decimal format
  ├─ Apply precision rules
  ├─ Validate price ranges
  └─ Standardize units
           ↓
    PriceNormalizedEvent
```
**Responsibility**: Standardize prices across sources

### Stage 3: Outlier Detection
```typescript
PriceNormalizedEvent
  ├─ Apply detection algorithm
  ├─ Identify anomalies
  └─ Filter invalid prices
           ↓
    filtered_prices[]
```
**Responsibility**: Remove suspicious data points

### Stage 4: Aggregation
```typescript
filtered_prices[]
  ├─ Apply aggregation method
  ├─ Calculate confidence
  ├─ Compute statistics
  └─ Assess quality
           ↓
    PriceAggregatedEvent
```
**Responsibility**: Calculate consensus price

### Stage 5: Storage
```typescript
PriceAggregatedEvent
  ├─ Validate result
  ├─ Serialize data
  └─ Persist to database
           ↓
    Storage confirmed
```
**Responsibility**: Store historical data

## 📊 Usage Examples

### Basic Price Processing

```typescript
import { OrchestrationService } from './services/orchestration.service';
import { PriceInputDto } from './dto/price-input.dto';

constructor(private orchestration: OrchestrationService) {}

async processPriceData(dto: PriceInputDto) {
  const result = await this.orchestration.processPriceData(dto);
  
  if (result.success) {
    console.log(`✓ Price: ${result.event.aggregatedPrice}`);
    console.log(`✓ Quality: ${result.event.qualityScore}`);
    console.log(`✓ Sources: ${result.event.sourceCount}/${result.event.totalSourceCount}`);
  } else {
    console.error(`✗ Failed at: ${result.failedStage}`);
    console.error(`✗ Error: ${result.error.message}`);
  }
  
  return result;
}
```

### Health Monitoring

```typescript
// Get circuit breaker health status
const health = this.orchestration.getHealthStatus();

if (health.healthy) {
  console.log('✓ All stages healthy');
} else {
  console.warn('⚠ Some stages degraded:');
  for (const [stage, status] of Object.entries(health.stages)) {
    console.log(`  ${stage}: ${status.state}`);
  }
}
```

### Recovery Management

```typescript
// Manually reset a circuit breaker after fixing an issue
this.orchestration.resetCircuitBreaker('PRICE_NORMALIZED');

// Emergency stop (open circuit)
this.circuitBreaker.open('DATA_STORAGE');
```

### REST API Endpoints

```bash
# Process a single price
POST /api/prices
Content-Type: application/json

{
  "symbol": "EUR/USD",
  "price": 1.0850,
  "source": "Bloomberg",
  "timestamp": "2026-02-02T16:34:24Z"
}

# Get system health
GET /api/health

# Get aggregated price for symbol
GET /api/prices/:symbol

# Get pipeline metrics
GET /api/metrics
```

## 🧪 Testing

### Run Test Suite

```bash
# Run all tests
npm test

# Run specific test file
npm test orchestration.service.spec

# Watch mode (auto-rerun on changes)
npm run test:watch

# Coverage report
npm run test:cov
```

### Test Coverage

- ✅ **Orchestration Service**: 11+ test cases
  - Happy path (full pipeline)
  - Error handling (all stages)
  - Health monitoring
  - Edge cases
  - Quality assessment

- ✅ **Circuit Breaker**: Comprehensive state machine testing
- ✅ **Events**: Proper event creation and propagation
- ✅ **Integration**: Full pipeline flow validation

**Target Coverage**: >75% code coverage

## 🛡️ Error Handling

### Circuit Breaker States

```
CLOSED ──(failures exceed threshold)──→ OPEN
  ↑                                        │
  │                                        │
  └─── HALF_OPEN ←(timeout expires)─────┘
       │
       ├(success threshold met)→ CLOSED
       │
       └(any failure)→ OPEN
```

### Error Recovery

The pipeline automatically handles errors:

1. **Stage Failure**: Logs error and stops pipeline
2. **Circuit Open**: Uses fallback strategy if available
3. **Timeout**: Configured per-stage (5-10 seconds)
4. **Storage Failure**: Non-blocking (doesn't fail pipeline)

### Common Errors

```typescript
// Insufficient sources
"Insufficient sources for EUR/USD. Required: 2, Found: 1"

// Invalid price data
"All prices must be for symbol EUR/USD"

// Configuration issues
"Unknown aggregation method: xyz"

// Outlier filtered everything
"All prices identified as outliers"
```

## 📈 Monitoring & Metrics

### Key Metrics

- **Success Rate**: Percentage of successful aggregations
- **Pipeline Latency**: Total execution time per stage
- **Source Contribution**: Weight and reliability per source
- **Quality Score**: Confidence in aggregated result
- **Circuit Status**: State of failure protection

### Event Emissions

The service emits events throughout the pipeline:

```typescript
// Price received
'price-received' → PriceReceivedEvent

// Price normalized
'price-normalized' → PriceNormalizedEvent

// Outliers detected
'outliers-detected' → detection results

// Price aggregated
'price-aggregated' → PriceAggregatedEvent

// Circuit breaker state change
'circuit-breaker.{STAGE}.state-changed' → state update
```

## 🔌 Integration Points

### Upstream Services
- **Ingestor**: Provides raw price data
- **Data Sources**: Multiple price feed APIs

### Downstream Services
- **Transactor**: Consumes aggregated prices
- **Storage**: Historical price records
- **Monitoring**: Metrics and alerts

## 🚀 Performance Considerations

### Optimization Tips

1. **Batch Processing**: Enable for high-volume scenarios
   - Reduces processing overhead
   - Smoother consensus calculations

2. **Time Windows**: Adjust based on market conditions
   - Real-time: 10-30 seconds
   - Stable: 1-5 minutes

3. **Source Count**: Balance coverage vs. latency
   - Minimum 2-3 sources
   - Maximum 10-20 sources

4. **Aggregation Method**:
   - **Weighted Average**: Fastest, good for stable markets
   - **Median**: Slowest, best for volatile markets
   - **Trimmed Mean**: Balanced approach

### Complexity Analysis

| Method | Time | Space | Outlier Resistant |
|--------|------|-------|-------------------|
| Weighted Avg | O(n) | O(1) | ❌ |
| Median | O(n log n) | O(n) | ✅✅✅ |
| Trimmed Mean | O(n log n) | O(n) | ✅✅ |

## 🤝 Contributing

1. **Code Quality**: Follow existing patterns
2. **Testing**: >75% coverage required
3. **Documentation**: Update README for new features
4. **Type Safety**: Use strict TypeScript settings
5. **Commits**: Descriptive commit messages

## 📝 Roadmap

- [ ] VWAP aggregation method
- [ ] Machine learning outlier detection
- [ ] Adaptive source weighting
- [ ] Real-time streaming aggregation
- [ ] Custom aggregation strategy plugins
- [ ] Historical backtesting tools

## 🐛 Troubleshooting

### Circuit Breaker Open

**Problem**: Service returns "Circuit breaker open for stage: X"

**Solution**:
```bash
# Check service logs for root cause
# Fix the underlying issue
# Reset circuit breaker manually
curl -X POST http://localhost:3001/api/circuit-breaker/reset?stage=PRICE_NORMALIZED
```

### Low Confidence Scores

**Problem**: Aggregated prices have confidence < 70%

**Causes**:
- Insufficient sources
- High price variance
- Outlier filtering too aggressive

**Solutions**:
- Add more reliable sources
- Adjust outlier threshold
- Increase time window

### Slow Processing

**Problem**: Pipeline takes >5 seconds

**Solutions**:
- Check network latency to sources
- Reduce number of sources
- Use weighted average instead of median
- Enable batch processing

## 📚 Additional Resources

- [NestJS Documentation](https://docs.nestjs.com)
- [CircuitBreaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Statistical Analysis](https://en.wikipedia.org/wiki/Descriptive_statistics)

## 📄 License

MIT

---

**Last Updated**: February 2026  
**Version**: 1.0.0  
**Status**: Production Ready