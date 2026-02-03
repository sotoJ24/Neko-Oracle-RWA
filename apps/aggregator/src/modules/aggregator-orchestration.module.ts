import { Module, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { ConfigModule, ConfigService } from '@nestjs/config';

import { OrchestrationService } from './orchestration.service';
import { CircuitBreakerMiddleware, CircuitBreakerState } from '../middleware/circuit-breaker.middleware';

import { AggregatorOrchestrationController } from '../controllers/aggregator-orchestration.controller';

import { DataReceptionService } from '../services/data-reception.service';
import { NormalizationService } from '../services/normalization.service';
import { OutlierDetectionService } from '../services/outlier-detection.service';
import { AggregationService } from '../services/aggregation.service';
import { DataStorageService } from '../services/data-storage.service';

import {
  PriceReceivedEvent,
  PriceNormalizedEvent,
  PriceAggregatedEvent,
} from '../events/price.events';

/**
 * Aggregator Orchestration Module
 *
 * This module implements the complete price aggregation pipeline with:
 * - Event-driven architecture for each processing stage
 * - Circuit breaker pattern to prevent cascading failures
 * - Comprehensive metrics and observability
 * - Error handling and recovery mechanisms
 * - Support for both real-time and batch processing
 *
 * Pipeline Flow:
 * Reception (PriceReceivedEvent)
 *   ↓
 * Normalization (PriceNormalizedEvent)
 *   ↓
 * Outlier Detection (filtered)
 *   ↓
 * Aggregation (PriceAggregatedEvent)
 *   ↓
 * Storage (persistence)
 *
 * @example
 * ```typescript
 * // In AppModule
 * @Module({
 *   imports: [
 *     AggregatorOrchestrationModule
 *   ]
 * })
 * export class AppModule {}
 *
 * // Inject and use the orchestration service
 * @Injectable()
 * export class MyService {
 *   constructor(private orchestration: OrchestrationService) {}
 *
 *   async processPrice(dto: PriceInputDto) {
 *     return this.orchestration.processPriceData(dto);
 *   }
 * }
 * ```
 */
@Module({
  imports: [
    EventEmitterModule.forRoot({
      wildcard: true,
      delimiter: '.',
      newListener: false,
      removeListener: false,
      maxListeners: 20,
      verboseMemoryLeak: true,
      ignoreErrors: false,
    }),
    ConfigModule.forFeature(() => ({
      aggregation: {
        // Real-time processing
        realtimeEnabled: process.env.AGGREGATION_REALTIME_ENABLED === 'true',

        // Batch processing
        batchEnabled: process.env.AGGREGATION_BATCH_ENABLED === 'true',
        batchIntervalMs: parseInt(process.env.AGGREGATION_BATCH_INTERVAL_MS || '5000', 10),
        batchMinPrices: parseInt(process.env.AGGREGATION_BATCH_MIN_PRICES || '3', 10),

        // Aggregation strategy
        aggregationMethod: process.env.AGGREGATION_METHOD || 'WEIGHTED_MEDIAN',
        minSourceCount: parseInt(process.env.AGGREGATION_MIN_SOURCES || '2', 10),
        maxSourceCount: parseInt(process.env.AGGREGATION_MAX_SOURCES || '50', 10),

        // Outlier detection
        outlierThreshold: parseFloat(process.env.OUTLIER_THRESHOLD || '0.03'),
        outlierMethod: process.env.OUTLIER_METHOD || 'MODIFIED_Z_SCORE',

        // Normalization
        enableNormalization: process.env.NORMALIZATION_ENABLED !== 'false',
        precisionBySymbol: {
          'EUR/USD': 6,
          'GBP/USD': 6,
          'BTC/USD': 2,
          'ETH/USD': 4,
        },

        // Quality thresholds
        minConfidence: parseFloat(process.env.MIN_CONFIDENCE || '0.7'),
        maxCoefficientOfVariation: parseFloat(process.env.MAX_CV || '0.05'),

        // Pipeline timeouts
        normalizationTimeoutMs: parseInt(process.env.NORMALIZATION_TIMEOUT_MS || '5000', 10),
        outlierDetectionTimeoutMs: parseInt(
          process.env.OUTLIER_DETECTION_TIMEOUT_MS || '3000',
          10
        ),
        aggregationTimeoutMs: parseInt(process.env.AGGREGATION_TIMEOUT_MS || '5000', 10),
        storageTimeoutMs: parseInt(process.env.STORAGE_TIMEOUT_MS || '10000', 10),

        // Circuit breaker defaults
        circuitBreaker: {
          failureThreshold: parseInt(process.env.CB_FAILURE_THRESHOLD || '5', 10),
          successThreshold: parseInt(process.env.CB_SUCCESS_THRESHOLD || '2', 10),
          timeoutMs: parseInt(process.env.CB_TIMEOUT_MS || '30000', 10),
          halfOpenMaxRequests: parseInt(process.env.CB_HALF_OPEN_MAX_REQUESTS || '3', 10),
          enableLogging: process.env.CB_ENABLE_LOGGING !== 'false',
        },

        // Storage
        enableStorage: process.env.STORAGE_ENABLED !== 'false',
        storageRetentionDays: parseInt(process.env.STORAGE_RETENTION_DAYS || '30', 10),

        // Metrics
        enableMetrics: process.env.METRICS_ENABLED !== 'false',
        metricsInterval: parseInt(process.env.METRICS_INTERVAL_MS || '60000', 10),

        // Logging
        verboseLogging: process.env.VERBOSE_LOGGING === 'true',
      },
    })),
  ],
  controllers: [AggregatorOrchestrationController],
  providers: [
    // Core Services
    OrchestrationService,
    CircuitBreakerMiddleware,

    // Pipeline Services
    DataReceptionService,
    NormalizationService,
    OutlierDetectionService,
    AggregationService,
    DataStorageService,
  ],
  exports: [
    OrchestrationService,
    CircuitBreakerMiddleware,
    DataReceptionService,
    NormalizationService,
    OutlierDetectionService,
    AggregationService,
    DataStorageService,
  ],
})
export class AggregatorOrchestrationModule implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(AggregatorOrchestrationModule.name);

  constructor(
    private configService: ConfigService,
    private orchestrationService: OrchestrationService,
    private circuitBreaker: CircuitBreakerMiddleware,
    private eventEmitter: any // EventEmitter2
  ) {}

  /**
   * Initialize the orchestration module
   * - Register circuit breakers for each pipeline stage
   * - Set up event listeners
   * - Initialize metrics collection
   * - Log pipeline configuration
   */
  async onModuleInit(): Promise<void> {
    this.logger.log('Initializing Aggregator Orchestration Module...');

    const config = this.configService.get('aggregation');
    this.validateConfiguration(config);
    this.registerCircuitBreakers(config);
    this.setupEventListeners(config);
    this.logPipelineConfiguration(config);

    this.logger.log('✓ Aggregator Orchestration Module initialized successfully');
  }

  /**
   * Cleanup resources on module destroy
   */
  onModuleDestroy(): void {
    this.logger.log('Destroying Aggregator Orchestration Module...');
    // Circuit breaker cleanup is handled automatically
  }

  /**
   * Validate the aggregation configuration
   */
  private validateConfiguration(config: any): void {
    const errors: string[] = [];

    // Validate at least one processing mode is enabled
    if (!config.realtimeEnabled && !config.batchEnabled) {
      errors.push(
        'At least one of AGGREGATION_REALTIME_ENABLED or AGGREGATION_BATCH_ENABLED must be true'
      );
    }

    // Validate aggregation method
    const validMethods = ['WEIGHTED_MEDIAN', 'WEIGHTED_MEAN', 'TRIMMED_MEAN'];
    if (!validMethods.includes(config.aggregationMethod)) {
      errors.push(
        `Invalid aggregation method: ${config.aggregationMethod}. Must be one of: ${validMethods.join(', ')}`
      );
    }

    // Validate outlier detection method
    const validOutlierMethods = ['MODIFIED_Z_SCORE', 'IQR', 'MEDIAN_ABSOLUTE_DEVIATION'];
    if (!validOutlierMethods.includes(config.outlierMethod)) {
      errors.push(
        `Invalid outlier method: ${config.outlierMethod}. Must be one of: ${validOutlierMethods.join(', ')}`
      );
    }

    // Validate numeric ranges
    if (config.outlierThreshold <= 0 || config.outlierThreshold > 1) {
      errors.push('OUTLIER_THRESHOLD must be between 0 and 1');
    }

    if (config.minSourceCount < 1) {
      errors.push('AGGREGATION_MIN_SOURCES must be at least 1');
    }

    if (config.maxSourceCount <= config.minSourceCount) {
      errors.push('AGGREGATION_MAX_SOURCES must be greater than AGGREGATION_MIN_SOURCES');
    }

    if (errors.length > 0) {
      this.logger.error('Configuration validation failed:');
      errors.forEach((error) => this.logger.error(`  ✗ ${error}`));
      throw new Error(`Invalid aggregation configuration: ${errors.join(', ')}`);
    }

    this.logger.log('✓ Configuration validation passed');
  }

  /**
   * Register circuit breakers for each pipeline stage
   */
  private registerCircuitBreakers(config: any): void {
    const stages = [
      'PRICE_RECEIVED',
      'PRICE_NORMALIZED',
      'OUTLIER_DETECTION',
      'PRICE_AGGREGATED',
      'DATA_STORAGE',
    ];

    const cbConfig = config.circuitBreaker;

    for (const stage of stages) {
      // Stage-specific configurations can override defaults
      const stageConfig = {
        ...cbConfig,
        // Normalization might be faster, so use shorter timeouts
        ...(stage === 'PRICE_NORMALIZED' && {
          timeout: Math.min(cbConfig.timeoutMs, 20000),
        }),
        // Aggregation might need more time
        ...(stage === 'PRICE_AGGREGATED' && {
          timeout: Math.max(cbConfig.timeoutMs, 30000),
        }),
      };

      this.circuitBreaker.registerStage(stage, stageConfig);
      this.logger.log(
        `Registered circuit breaker for stage: ${stage} (threshold: ${stageConfig.failureThreshold}, timeout: ${stageConfig.timeout}ms)`
      );
    }
  }

  /**
   * Setup event listeners for observability
   */
  private setupEventListeners(config: any): void {
    if (!config.enableMetrics) {
      return;
    }

    // Listen to circuit breaker state changes
    this.eventEmitter.on('circuit-breaker.*.state-changed', (data: any) => {
      const { stage, newState, metrics } = data;

      if (newState === CircuitBreakerState.OPEN) {
        this.logger.error(
          `🔴 Circuit breaker OPENED: ${stage} | Failures: ${metrics.failureCount} | Last error: ${metrics.lastError}`
        );
      } else if (newState === CircuitBreakerState.HALF_OPEN) {
        this.logger.warn(
          `🟡 Circuit breaker HALF_OPEN: ${stage} | Attempting recovery... (Success threshold: ${metrics.currentSuccessCount})`
        );
      } else if (newState === CircuitBreakerState.CLOSED) {
        this.logger.log(
          `🟢 Circuit breaker CLOSED: ${stage} | Service recovered | Success rate: ${metrics.successRate.toFixed(2)}%`
        );
      }
    });

    // Listen to price events for logging
    this.eventEmitter.on('price-received', (event: PriceReceivedEvent) => {
      if (config.verboseLogging) {
        this.logger.debug(
          `[${event.traceId}] Price received: ${event.symbol} @ ${event.price} from ${event.source}`
        );
      }
    });

    this.eventEmitter.on('price-normalized', (event: PriceNormalizedEvent) => {
      if (config.verboseLogging) {
        this.logger.debug(
          `[${event.traceId}] Price normalized: ${event.symbol} ${event.originalPrice} -> ${event.normalizedPrice} (${event.executionTime}ms)`
        );
      }
    });

    this.eventEmitter.on('price-aggregated', (event: PriceAggregatedEvent) => {
      const assessment = event.getQualityAssessment();
      const logLevel = assessment.isHighQuality ? 'log' : 'warn';

      this.logger[logLevel](
        `[${event.traceId}] Price aggregated: ${event.symbol} @ ${event.aggregatedPrice} | Sources: ${event.sourceCount}/${event.totalSourceCount} | Quality: ${assessment.qualityLevel} | CV: ${event.coefficientOfVariation.toFixed(4)}`
      );
    });
  }

  /**
   * Log the complete pipeline configuration
   */
  private logPipelineConfiguration(config: any): void {
    this.logger.log('Pipeline Configuration:');
    this.logger.log(`  Processing Modes:`);
    this.logger.log(`    - Real-time: ${config.realtimeEnabled ? '✓ Enabled' : '✗ Disabled'}`);
    this.logger.log(`    - Batch: ${config.batchEnabled ? '✓ Enabled' : '✗ Disabled'}`);
    if (config.batchEnabled) {
      this.logger.log(
        `      • Interval: ${config.batchIntervalMs}ms | Min prices: ${config.batchMinPrices}`
      );
    }

    this.logger.log(`  Aggregation:`);
    this.logger.log(`    - Method: ${config.aggregationMethod}`);
    this.logger.log(
      `    - Sources: ${config.minSourceCount}-${config.maxSourceCount} | Min confidence: ${config.minConfidence}`
    );
    this.logger.log(`    - Max CV threshold: ${config.maxCoefficientOfVariation}`);

    this.logger.log(`  Outlier Detection:`);
    this.logger.log(`    - Method: ${config.outlierMethod}`);
    this.logger.log(`    - Threshold: ${config.outlierThreshold}`);

    this.logger.log(`  Timeouts:`);
    this.logger.log(`    - Normalization: ${config.normalizationTimeoutMs}ms`);
    this.logger.log(`    - Outlier Detection: ${config.outlierDetectionTimeoutMs}ms`);
    this.logger.log(`    - Aggregation: ${config.aggregationTimeoutMs}ms`);
    this.logger.log(`    - Storage: ${config.storageTimeoutMs}ms`);

    this.logger.log(`  Circuit Breaker:`);
    this.logger.log(
      `    - Threshold: ${config.circuitBreaker.failureThreshold} failures | Recovery: ${config.circuitBreaker.timeoutMs}ms`
    );
    this.logger.log(
      `    - Success threshold: ${config.circuitBreaker.successThreshold} | Half-open max: ${config.circuitBreaker.halfOpenMaxRequests}`
    );

    this.logger.log(`  Storage: ${config.enableStorage ? '✓ Enabled' : '✗ Disabled'}`);
    if (config.enableStorage) {
      this.logger.log(`    - Retention: ${config.storageRetentionDays} days`);
    }

    this.logger.log(`  Metrics: ${config.enableMetrics ? '✓ Enabled' : '✗ Disabled'}`);
    if (config.enableMetrics) {
      this.logger.log(`    - Interval: ${config.metricsInterval}ms`);
    }
  }
}