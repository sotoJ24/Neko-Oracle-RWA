import { Injectable, Logger, Inject } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';

import { CircuitBreakerMiddleware, CircuitBreakerOpenError } from '../middleware/circuit-breaker.middleware';

import { DataReceptionService } from './data-reception.service';
import { NormalizationService } from './normalization.service';
import { OutlierDetectionService } from './outlier-detection.service';
import { AggregationService } from './aggregation.service';
import { DataStorageService } from './data-storage.service';

import {
  PriceReceivedEvent,
  PriceNormalizedEvent,
  PriceAggregatedEvent,
} from '../events/price.events';

import { PriceInputDto } from '../dto/price-input.dto';

/**
 * Pipeline processing result
 */
export interface PipelineResult {
  /**
   * Whether the entire pipeline completed successfully
   */
  success: boolean;

  /**
   * Final aggregated event (if successful)
   */
  event?: PriceAggregatedEvent;

  /**
   * Error if pipeline failed
   */
  error?: Error;

  /**
   * Stage where failure occurred
   */
  failedStage?: string;

  /**
   * Execution times per stage in milliseconds
   */
  executionTimes: Record<string, number>;

  /**
   * Total pipeline execution time in milliseconds
   */
  totalExecutionTime: number;

  /**
   * Trace ID for tracking
   */
  traceId: string;

  /**
   * Pipeline status details
   */
  status: {
    priceReceived: boolean;
    priceNormalized: boolean;
    outliersDetected: boolean;
    priceAggregated: boolean;
    dataStored: boolean;
  };
}

/**
 * Orchestration Service
 *
 * Coordinates the complete price aggregation pipeline:
 * 1. Reception: Validate and create PriceReceivedEvent
 * 2. Normalization: Convert prices to standard format
 * 3. Outlier Detection: Identify and filter anomalies
 * 4. Aggregation: Calculate consensus price from valid sources
 * 5. Storage: Persist aggregated results
 *
 * Features:
 * - Circuit breaker protection on each stage
 * - Error handling without stopping the pipeline
 * - Detailed execution metrics per stage
 * - Event emission for observability
 * - Fallback strategies
 * - Trace ID propagation
 *
 * @example
 * ```typescript
 * @Injectable()
 * export class PriceController {
 *   constructor(private orchestration: OrchestrationService) {}
 *
 *   @Post('prices')
 *   async processPriceData(@Body() dto: PriceInputDto): Promise<PipelineResult> {
 *     return this.orchestration.processPriceData(dto);
 *   }
 * }
 * ```
 */
@Injectable()
export class OrchestrationService {
  private readonly logger = new Logger(OrchestrationService.name);

  /**
   * Cache for batch processing
   */
  private batchCache = new Map<string, PriceReceivedEvent[]>();

  /**
   * Batch processing timers
   */
  private batchTimers = new Map<string, NodeJS.Timeout>();

  constructor(
    private configService: ConfigService,
    private eventEmitter: EventEmitter2,
    private circuitBreaker: CircuitBreakerMiddleware,
    private receptionService: DataReceptionService,
    private normalizationService: NormalizationService,
    private outlierDetectionService: OutlierDetectionService,
    private aggregationService: AggregationService,
    private storageService: DataStorageService
  ) {}

  /**
   * Process a single price input through the complete pipeline
   *
   * This is the main entry point for price data. It processes through:
   * Reception → Normalization → Outlier Detection → Aggregation → Storage
   *
   * @param dto - Price input data
   * @returns Pipeline execution result
   */
  async processPriceData(dto: PriceInputDto): Promise<PipelineResult> {
    const startTime = Date.now();
    const config = this.configService.get('aggregation');
    const executionTimes: Record<string, number> = {};
    const result: PipelineResult = {
      success: false,
      executionTimes,
      totalExecutionTime: 0,
      traceId: '',
      status: {
        priceReceived: false,
        priceNormalized: false,
        outliersDetected: false,
        priceAggregated: false,
        dataStored: false,
      },
    };

    try {
      // Stage 1: Reception
      const receptionStart = Date.now();
      const priceReceivedEvent = await this.executeReception(dto);
      executionTimes['PRICE_RECEIVED'] = Date.now() - receptionStart;
      result.status.priceReceived = true;
      result.traceId = priceReceivedEvent.traceId;

      // Stage 2: Normalization
      const normalizationStart = Date.now();
      const priceNormalizedEvent = await this.executeNormalization(
        priceReceivedEvent,
        config.normalizationTimeoutMs
      );
      executionTimes['PRICE_NORMALIZED'] = Date.now() - normalizationStart;
      result.status.priceNormalized = true;

      // Handle batch processing if enabled
      if (config.batchEnabled) {
        this.addToBatch(priceNormalizedEvent);
        result.event = undefined; // No aggregation yet in batch mode
        result.success = true; // Queued successfully
        result.totalExecutionTime = Date.now() - startTime;

        this.logger.debug(
          `[${result.traceId}] Price queued for batch processing: ${priceReceivedEvent.symbol}`
        );

        return result;
      }

      // Real-time processing continues...
      if (!config.realtimeEnabled) {
        this.logger.warn(
          `[${result.traceId}] Real-time processing disabled, price will be processed in batch`
        );
        result.success = true;
        result.totalExecutionTime = Date.now() - startTime;
        return result;
      }

      // Stage 3: Outlier Detection (aggregate with other recent prices)
      const outlierStart = Date.now();
      const aggregatedPrices = await this.executeOutlierDetection(
        [priceNormalizedEvent],
        config.outlierDetectionTimeoutMs
      );
      executionTimes['OUTLIER_DETECTION'] = Date.now() - outlierStart;
      result.status.outliersDetected = true;

      // If outlier detection filtered this price, still mark success but no aggregation
      if (aggregatedPrices.length === 0) {
        this.logger.warn(
          `[${result.traceId}] Price identified as outlier: ${priceReceivedEvent.symbol}`
        );
        result.success = true;
        result.totalExecutionTime = Date.now() - startTime;
        return result;
      }

      // Stage 4: Aggregation
      const aggregationStart = Date.now();
      const priceAggregatedEvent = await this.executeAggregation(
        [priceNormalizedEvent],
        aggregatedPrices,
        config.aggregationTimeoutMs
      );
      executionTimes['PRICE_AGGREGATED'] = Date.now() - aggregationStart;
      result.status.priceAggregated = true;
      result.event = priceAggregatedEvent;

      // Validate aggregation quality
      const assessment = priceAggregatedEvent.getQualityAssessment();
      if (!assessment.isHighQuality) {
        this.logger.warn(
          `[${result.traceId}] Low quality aggregation: ${priceAggregatedEvent.symbol} | Issues: ${assessment.issues.join(', ')}`
        );
      }

      // Stage 5: Storage
      const storageStart = Date.now();
      await this.executeStorage(priceAggregatedEvent, config.storageTimeoutMs);
      executionTimes['DATA_STORAGE'] = Date.now() - storageStart;
      result.status.dataStored = true;

      result.success = true;
      result.totalExecutionTime = Date.now() - startTime;

      this.logger.log(
        `[${result.traceId}] ✓ Pipeline completed successfully: ${priceReceivedEvent.symbol} @ ${priceAggregatedEvent.aggregatedPrice} (${result.totalExecutionTime}ms)`
      );

      return result;
    } catch (error) {
      result.totalExecutionTime = Date.now() - startTime;
      result.error = error as Error;

      // Determine which stage failed
      if (!result.status.priceReceived) {
        result.failedStage = 'PRICE_RECEIVED';
      } else if (!result.status.priceNormalized) {
        result.failedStage = 'PRICE_NORMALIZED';
      } else if (!result.status.outliersDetected) {
        result.failedStage = 'OUTLIER_DETECTION';
      } else if (!result.status.priceAggregated) {
        result.failedStage = 'PRICE_AGGREGATED';
      } else if (!result.status.dataStored) {
        result.failedStage = 'DATA_STORAGE';
      }

      this.logger.error(
        `[${result.traceId || 'unknown'}] ✗ Pipeline failed at stage: ${result.failedStage} | Error: ${error instanceof Error ? error.message : String(error)}`
      );

      return result;
    }
  }

  /**
   * Process batch of accumulated prices
   * Called periodically when batch processing is enabled
   */
  async processBatch(symbol: string): Promise<PipelineResult | null> {
    const batchKey = `batch:${symbol}`;
    const batch = this.batchCache.get(batchKey);

    if (!batch || batch.length === 0) {
      return null;
    }

    const config = this.configService.get('aggregation');
    const startTime = Date.now();
    const executionTimes: Record<string, number> = {};
    const traceId = batch[0].traceId;

    this.logger.debug(`[${traceId}] Processing batch for ${symbol} with ${batch.length} prices`);

    try {
      // Execute outlier detection on entire batch
      const outlierStart = Date.now();
      const validPrices = await this.executeOutlierDetection(
        batch,
        config.outlierDetectionTimeoutMs
      );
      executionTimes['OUTLIER_DETECTION'] = Date.now() - outlierStart;

      if (validPrices.length < config.minSourceCount) {
        this.logger.warn(
          `[${traceId}] Insufficient valid prices for batch aggregation: ${validPrices.length}/${config.minSourceCount}`
        );
        this.batchCache.delete(batchKey);
        return null;
      }

      // Aggregate all valid prices
      const aggregationStart = Date.now();
      const priceAggregatedEvent = await this.executeAggregation(
        batch,
        validPrices,
        config.aggregationTimeoutMs
      );
      executionTimes['PRICE_AGGREGATED'] = Date.now() - aggregationStart;

      // Store aggregated result
      const storageStart = Date.now();
      await this.executeStorage(priceAggregatedEvent, config.storageTimeoutMs);
      executionTimes['DATA_STORAGE'] = Date.now() - storageStart;

      // Clear batch
      this.batchCache.delete(batchKey);

      const result: PipelineResult = {
        success: true,
        event: priceAggregatedEvent,
        executionTimes,
        totalExecutionTime: Date.now() - startTime,
        traceId,
        status: {
          priceReceived: true,
          priceNormalized: true,
          outliersDetected: true,
          priceAggregated: true,
          dataStored: true,
        },
      };

      this.logger.log(
        `[${traceId}] ✓ Batch processed: ${symbol} | ${batch.length} prices aggregated @ ${priceAggregatedEvent.aggregatedPrice}`
      );

      return result;
    } catch (error) {
      this.logger.error(
        `[${traceId}] ✗ Batch processing failed for ${symbol}: ${error instanceof Error ? error.message : String(error)}`
      );
      return null;
    }
  }

  /**
   * Get health status of all circuit breakers
   */
  getHealthStatus(): Record<string, any> {
    const allMetrics = this.circuitBreaker.getAllMetrics();
    const health: Record<string, any> = {
      timestamp: new Date(),
      healthy: true,
      stages: {},
    };

    for (const [stage, data] of Object.entries(allMetrics)) {
      const isHealthy = data.state === 'CLOSED';
      health.stages[stage] = {
        state: data.state,
        healthy: isHealthy,
        successRate: data.metrics.successRate.toFixed(2) + '%',
        totalRequests: data.metrics.totalRequests,
        failures: data.metrics.failureCount,
        averageResponseTime: data.metrics.averageResponseTime.toFixed(2) + 'ms',
      };

      if (!isHealthy) {
        health.healthy = false;
      }
    }

    return health;
  }

  /**
   * Reset a specific circuit breaker
   */
  resetCircuitBreaker(stage: string): void {
    this.circuitBreaker.reset(stage);
    this.logger.warn(`Circuit breaker reset for stage: ${stage}`);
  }

  // ============================================================================
  // Private Stage Execution Methods
  // ============================================================================

  /**
   * Execute reception stage
   */
  private async executeReception(dto: PriceInputDto): Promise<PriceReceivedEvent> {
    return this.circuitBreaker.execute(
      'PRICE_RECEIVED',
      () => this.receptionService.receivePrice(dto),
      {
        timeout: 2000,
        onError: (error, stage) => {
          this.logger.error(`Reception stage error: ${error.message}`);
        },
      }
    );
  }

  /**
   * Execute normalization stage
   */
  private async executeNormalization(
    event: PriceReceivedEvent,
    timeout: number
  ): Promise<PriceNormalizedEvent> {
    return this.circuitBreaker.execute(
      'PRICE_NORMALIZED',
      () => this.normalizationService.normalize(event),
      {
        timeout,
        fallback: async (error) => {
          this.logger.warn(
            `Normalization failed, using raw price as fallback: ${error.message}`
          );
          // Fallback: use raw price without normalization
          return this.normalizationService.createFallbackNormalization(event);
        },
        onError: (error, stage) => {
          this.logger.error(`Normalization stage error: ${error.message}`);
        },
      }
    );
  }

  /**
   * Execute outlier detection stage
   */
  private async executeOutlierDetection(
    events: PriceNormalizedEvent[],
    timeout: number
  ): Promise<PriceNormalizedEvent[]> {
    if (events.length === 0) {
      return [];
    }

    return this.circuitBreaker.execute(
      'OUTLIER_DETECTION',
      () => this.outlierDetectionService.detectOutliers(events),
      {
        timeout,
        fallback: async () => {
          // Fallback: accept all prices if detection fails
          this.logger.warn('Outlier detection failed, accepting all prices');
          return events;
        },
        onError: (error, stage) => {
          this.logger.error(`Outlier detection stage error: ${error.message}`);
        },
      }
    );
  }

  /**
   * Execute aggregation stage
   */
  private async executeAggregation(
    sourceEvents: PriceNormalizedEvent[],
    validPrices: PriceNormalizedEvent[],
    timeout: number
  ): Promise<PriceAggregatedEvent> {
    return this.circuitBreaker.execute(
      'PRICE_AGGREGATED',
      () => this.aggregationService.aggregate(sourceEvents, validPrices),
      {
        timeout,
        onError: (error, stage) => {
          this.logger.error(`Aggregation stage error: ${error.message}`);
        },
      }
    );
  }

  /**
   * Execute storage stage
   */
  private async executeStorage(event: PriceAggregatedEvent, timeout: number): Promise<void> {
    const config = this.configService.get('aggregation');

    if (!config.enableStorage) {
      return;
    }

    return this.circuitBreaker.execute(
      'DATA_STORAGE',
      () => this.storageService.store(event),
      {
        timeout,
        fallback: async (error) => {
          this.logger.warn(`Storage failed, price data not persisted: ${error.message}`);
          // Still consider pipeline successful even if storage fails
          return undefined;
        },
        onError: (error, stage) => {
          this.logger.error(`Storage stage error: ${error.message}`);
        },
      }
    );
  }

  /**
   * Add price to batch cache
   */
  private addToBatch(event: PriceNormalizedEvent): void {
    const config = this.configService.get('aggregation');
    const batchKey = `batch:${event.symbol}`;

    if (!this.batchCache.has(batchKey)) {
      this.batchCache.set(batchKey, []);
    }

    const batch = this.batchCache.get(batchKey)!;
    batch.push(event);

    // Check if batch is full
    if (batch.length >= config.batchMinPrices) {
      this.processBatchImmediate(event.symbol);
      return;
    }

    // Schedule batch processing if timer not already set
    if (!this.batchTimers.has(batchKey)) {
      const timer = setTimeout(() => {
        this.processBatchImmediate(event.symbol);
      }, config.batchIntervalMs);

      this.batchTimers.set(batchKey, timer);
    }
  }

  /**
   * Process batch immediately and clean up timer
   */
  private async processBatchImmediate(symbol: string): Promise<void> {
    const batchKey = `batch:${symbol}`;
    const timer = this.batchTimers.get(batchKey);

    if (timer) {
      clearTimeout(timer);
      this.batchTimers.delete(batchKey);
    }

    await this.processBatch(symbol);
  }
}