import { Injectable, Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';

/**
 * Circuit breaker state enumeration
 */
export enum CircuitBreakerState {
  /**
   * Circuit is closed - requests pass through normally
   */
  CLOSED = 'CLOSED',

  /**
   * Circuit is open - requests fail immediately without attempting the operation
   */
  OPEN = 'OPEN',

  /**
   * Circuit is half-open - limited requests allowed to test if service recovered
   */
  HALF_OPEN = 'HALF_OPEN',
}

/**
 * Circuit breaker configuration interface
 */
export interface CircuitBreakerConfig {
  /**
   * Failure threshold (number of failures) before opening the circuit
   */
  failureThreshold: number;

  /**
   * Success threshold (number of successes) to close the circuit from HALF_OPEN state
   */
  successThreshold: number;

  /**
   * Timeout in milliseconds before attempting to recover (transition from OPEN to HALF_OPEN)
   */
  timeout: number;

  /**
   * Maximum number of concurrent requests allowed in HALF_OPEN state
   */
  halfOpenMaxRequests: number;

  /**
   * Enable detailed logging
   */
  enableLogging: boolean;

  /**
   * Custom metrics callback for monitoring
   */
  onStateChange?: (stage: string, newState: CircuitBreakerState, metrics: CircuitBreakerMetrics) => void;
}

/**
 * Circuit breaker metrics
 */
export interface CircuitBreakerMetrics {
  /**
   * Total number of requests processed
   */
  totalRequests: number;

  /**
   * Number of successful requests
   */
  successCount: number;

  /**
   * Number of failed requests
   */
  failureCount: number;

  /**
   * Current failure count (resets on success or state change)
   */
  currentFailureCount: number;

  /**
   * Current success count (used in HALF_OPEN state)
   */
  currentSuccessCount: number;

  /**
   * Success rate percentage (0-100)
   */
  successRate: number;

  /**
   * Average response time in milliseconds
   */
  averageResponseTime: number;

  /**
   * Last error message
   */
  lastError?: string;

  /**
   * Timestamp of last failure
   */
  lastFailureTime?: Date;

  /**
   * Timestamp when circuit was opened
   */
  openedAt?: Date;

  /**
   * Estimated time until recovery attempt in milliseconds
   */
  recoveryEstimateMs?: number;
}

/**
 * Error response from circuit breaker when circuit is open
 */
export class CircuitBreakerOpenError extends Error {
  constructor(
    public readonly stage: string,
    public readonly metrics: CircuitBreakerMetrics,
    message?: string
  ) {
    super(
      message ||
        `Circuit breaker open for stage: ${stage}. Failure count: ${metrics.currentFailureCount}/${metrics.failureThreshold}`
    );
    this.name = 'CircuitBreakerOpenError';
  }
}

/**
 * Circuit Breaker Middleware
 *
 * Implements the circuit breaker pattern to prevent cascading failures
 * in the orchestration pipeline. Each pipeline stage has its own circuit breaker.
 *
 * State transitions:
 * CLOSED -> OPEN (after failureThreshold failures)
 * OPEN -> HALF_OPEN (after timeout milliseconds)
 * HALF_OPEN -> CLOSED (after successThreshold successes)
 * HALF_OPEN -> OPEN (on first failure)
 *
 * @example
 * ```
 * const circuitBreaker = new CircuitBreakerMiddleware(eventEmitter, {
 *   failureThreshold: 5,
 *   successThreshold: 2,
 *   timeout: 30000,
 *   halfOpenMaxRequests: 3,
 *   enableLogging: true
 * });
 *
 * // Wrap a stage handler
 * const result = await circuitBreaker.execute(
 *   'NORMALIZATION',
 *   () => normalizationService.normalize(event),
 *   {
 *     timeout: 5000,
 *     fallback: () => event // optional fallback
 *   }
 * );
 * ```
 */
@Injectable()
export class CircuitBreakerMiddleware {
  private readonly logger = new Logger(CircuitBreakerMiddleware.name);

  /**
   * Circuit breaker state per stage
   */
  private states = new Map<string, CircuitBreakerState>();

  /**
   * Metrics per stage
   */
  private metrics = new Map<string, CircuitBreakerMetrics>();

  /**
   * Timestamps for OPEN state transitions
   */
  private openedAt = new Map<string, Date>();

  /**
   * Active requests counter for HALF_OPEN state
   */
  private activeRequests = new Map<string, number>();

  /**
   * Circuit breaker configurations per stage
   */
  private configs = new Map<string, CircuitBreakerConfig>();

  /**
   * Recovery timers (to clear on state change)
   */
  private recoveryTimers = new Map<string, NodeJS.Timeout>();

  constructor(
    private eventEmitter: EventEmitter2,
    private defaultConfig: CircuitBreakerConfig = {
      failureThreshold: 5,
      successThreshold: 2,
      timeout: 30000, // 30 seconds
      halfOpenMaxRequests: 3,
      enableLogging: true,
    }
  ) {}

  /**
   * Register a custom configuration for a specific stage
   */
  registerStage(stage: string, config: Partial<CircuitBreakerConfig>): void {
    const mergedConfig = { ...this.defaultConfig, ...config };
    this.configs.set(stage, mergedConfig);
    this.initializeStage(stage);
  }

  /**
   * Initialize metrics for a stage
   */
  private initializeStage(stage: string): void {
    if (!this.states.has(stage)) {
      this.states.set(stage, CircuitBreakerState.CLOSED);
      this.metrics.set(stage, {
        totalRequests: 0,
        successCount: 0,
        failureCount: 0,
        currentFailureCount: 0,
        currentSuccessCount: 0,
        successRate: 100,
        averageResponseTime: 0,
      });
      this.activeRequests.set(stage, 0);
    }
  }

  /**
   * Execute a function with circuit breaker protection
   *
   * @param stage - Pipeline stage identifier
   * @param handler - Function to execute
   * @param options - Execution options (timeout, fallback)
   * @returns Result from handler or fallback
   * @throws CircuitBreakerOpenError if circuit is open and no fallback provided
   */
  async execute<T>(
    stage: string,
    handler: () => Promise<T>,
    options: {
      timeout?: number;
      fallback?: (error: Error, metrics: CircuitBreakerMetrics) => Promise<T> | T;
      onError?: (error: Error, stage: string) => void;
    } = {}
  ): Promise<T> {
    this.initializeStage(stage);

    const config = this.configs.get(stage) || this.defaultConfig;
    const state = this.states.get(stage)!;
    const metrics = this.metrics.get(stage)!;

    // Check if circuit is open
    if (state === CircuitBreakerState.OPEN) {
      return this.handleOpenCircuit(stage, config, metrics, options);
    }

    // Check if we can accept more requests in HALF_OPEN state
    if (state === CircuitBreakerState.HALF_OPEN) {
      const active = this.activeRequests.get(stage) || 0;
      if (active >= config.halfOpenMaxRequests) {
        const error = new CircuitBreakerOpenError(
          stage,
          metrics,
          `Circuit breaker HALF_OPEN state with max concurrent requests (${config.halfOpenMaxRequests})`
        );
        options.onError?.(error, stage);

        if (options.fallback) {
          return options.fallback(error, metrics);
        }
        throw error;
      }
    }

    // Execute the handler with timeout
    const startTime = Date.now();
    try {
      this.activeRequests.set(stage, (this.activeRequests.get(stage) || 0) + 1);

      const result = await this.executeWithTimeout(
        handler,
        options.timeout || 30000
      );

      this.recordSuccess(stage, config, startTime);
      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.recordFailure(stage, config, errorMessage);
      options.onError?.(error as Error, stage);

      if (options.fallback) {
        return options.fallback(error as Error, metrics);
      }
      throw error;
    } finally {
      this.activeRequests.set(stage, (this.activeRequests.get(stage) || 0) - 1);
    }
  }

  /**
   * Execute a synchronous function with circuit breaker protection
   */
  executeSync<T>(
    stage: string,
    handler: () => T,
    options: {
      fallback?: (error: Error, metrics: CircuitBreakerMetrics) => T;
      onError?: (error: Error, stage: string) => void;
    } = {}
  ): T {
    this.initializeStage(stage);

    const config = this.configs.get(stage) || this.defaultConfig;
    const state = this.states.get(stage)!;
    const metrics = this.metrics.get(stage)!;

    // Check if circuit is open
    if (state === CircuitBreakerState.OPEN) {
      const error = new CircuitBreakerOpenError(stage, metrics);
      options.onError?.(error, stage);

      if (options.fallback) {
        return options.fallback(error, metrics);
      }
      throw error;
    }

    const startTime = Date.now();
    try {
      const result = handler();
      this.recordSuccess(stage, config, startTime);
      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.recordFailure(stage, config, errorMessage);
      options.onError?.(error as Error, stage);

      if (options.fallback) {
        return options.fallback(error as Error, metrics);
      }
      throw error;
    }
  }

  /**
   * Get current state of a circuit breaker
   */
  getState(stage: string): CircuitBreakerState {
    this.initializeStage(stage);
    return this.states.get(stage) || CircuitBreakerState.CLOSED;
  }

  /**
   * Get metrics for a circuit breaker
   */
  getMetrics(stage: string): CircuitBreakerMetrics {
    this.initializeStage(stage);
    return { ...this.metrics.get(stage)! };
  }

  /**
   * Get all circuit breaker states and metrics
   */
  getAllMetrics(): Record<string, { state: CircuitBreakerState; metrics: CircuitBreakerMetrics }> {
    const result: Record<string, { state: CircuitBreakerState; metrics: CircuitBreakerMetrics }> = {};

    for (const [stage, state] of this.states.entries()) {
      result[stage] = {
        state,
        metrics: { ...this.metrics.get(stage)! },
      };
    }

    return result;
  }

  /**
   * Manually reset a circuit breaker to CLOSED state
   */
  reset(stage: string): void {
    this.logger.warn(`Manually resetting circuit breaker for stage: ${stage}`);

    const timer = this.recoveryTimers.get(stage);
    if (timer) {
      clearTimeout(timer);
      this.recoveryTimers.delete(stage);
    }

    this.states.set(stage, CircuitBreakerState.CLOSED);
    const metrics = this.metrics.get(stage);
    if (metrics) {
      metrics.currentFailureCount = 0;
      metrics.currentSuccessCount = 0;
    }

    this.emitStateChange(stage, CircuitBreakerState.CLOSED);
  }

  /**
   * Manually open a circuit breaker (emergency stop)
   */
  open(stage: string): void {
    this.logger.error(`Manually opening circuit breaker for stage: ${stage}`);

    this.states.set(stage, CircuitBreakerState.OPEN);
    this.openedAt.set(stage, new Date());

    const metrics = this.metrics.get(stage);
    if (metrics) {
      metrics.openedAt = new Date();
    }

    this.emitStateChange(stage, CircuitBreakerState.OPEN);
  }

  /**
   * Handle circuit open - either use fallback or throw error
   */
  private async handleOpenCircuit<T>(
    stage: string,
    config: CircuitBreakerConfig,
    metrics: CircuitBreakerMetrics,
    options: {
      fallback?: (error: Error, metrics: CircuitBreakerMetrics) => Promise<T> | T;
      onError?: (error: Error, stage: string) => void;
    }
  ): Promise<T> {
    const openedAt = this.openedAt.get(stage);
    const timeSinceOpen = openedAt ? Date.now() - openedAt.getTime() : 0;

    if (timeSinceOpen >= config.timeout) {
      // Attempt recovery
      this.transitionToHalfOpen(stage);
      
      if (config.enableLogging) {
        this.logger.log(
          `Circuit breaker transitioned to HALF_OPEN for stage: ${stage}. Attempting recovery...`
        );
      }

      // Recursively call execute to process in HALF_OPEN state
      return this.execute(stage, () => Promise.reject(new Error('Recovery attempt')), options);
    }

    const error = new CircuitBreakerOpenError(stage, metrics);
    options.onError?.(error, stage);

    metrics.recoveryEstimateMs = config.timeout - timeSinceOpen;

    if (options.fallback) {
      return options.fallback(error, metrics);
    }

    throw error;
  }

  /**
   * Record a successful request
   */
  private recordSuccess(stage: string, config: CircuitBreakerConfig, startTime: number): void {
    const metrics = this.metrics.get(stage)!;
    const state = this.states.get(stage)!;

    metrics.totalRequests++;
    metrics.successCount++;
    metrics.currentFailureCount = 0;

    // Update average response time
    const responseTime = Date.now() - startTime;
    metrics.averageResponseTime =
      (metrics.averageResponseTime * (metrics.successCount - 1) + responseTime) /
      metrics.successCount;

    metrics.successRate = (metrics.successCount / metrics.totalRequests) * 100;

    if (state === CircuitBreakerState.HALF_OPEN) {
      metrics.currentSuccessCount++;

      if (metrics.currentSuccessCount >= config.successThreshold) {
        this.transitionToClosed(stage);
      }
    }

    if (config.enableLogging) {
      this.logger.debug(
        `Stage ${stage}: Success recorded. Response time: ${responseTime}ms, Success rate: ${metrics.successRate.toFixed(2)}%`
      );
    }
  }

  /**
   * Record a failed request
   */
  private recordFailure(stage: string, config: CircuitBreakerConfig, errorMessage: string): void {
    const metrics = this.metrics.get(stage)!;
    const state = this.states.get(stage)!;

    metrics.totalRequests++;
    metrics.failureCount++;
    metrics.currentFailureCount++;
    metrics.lastError = errorMessage;
    metrics.lastFailureTime = new Date();
    metrics.successRate = (metrics.successCount / metrics.totalRequests) * 100;

    if (state === CircuitBreakerState.HALF_OPEN) {
      // Immediate transition back to OPEN on first failure in HALF_OPEN
      this.transitionToOpen(stage, config);
    } else if (state === CircuitBreakerState.CLOSED) {
      // Check if failure threshold exceeded
      if (metrics.currentFailureCount >= config.failureThreshold) {
        this.transitionToOpen(stage, config);
      }
    }

    if (config.enableLogging) {
      this.logger.warn(
        `Stage ${stage}: Failure recorded. Count: ${metrics.currentFailureCount}/${config.failureThreshold}. Error: ${errorMessage}`
      );
    }
  }

  /**
   * Transition circuit to OPEN state
   */
  private transitionToOpen(stage: string, config: CircuitBreakerConfig): void {
    const metrics = this.metrics.get(stage)!;
    this.states.set(stage, CircuitBreakerState.OPEN);
    this.openedAt.set(stage, new Date());
    metrics.openedAt = new Date();
    metrics.currentSuccessCount = 0;

    if (config.enableLogging) {
      this.logger.error(`Circuit breaker OPENED for stage: ${stage}`);
    }

    // Schedule recovery attempt
    const timer = setTimeout(() => {
      this.transitionToHalfOpen(stage);
      if (config.enableLogging) {
        this.logger.log(
          `Circuit breaker timeout expired for stage: ${stage}. Transitioning to HALF_OPEN...`
        );
      }
    }, config.timeout);

    this.recoveryTimers.set(stage, timer);

    this.emitStateChange(stage, CircuitBreakerState.OPEN);
  }

  /**
   * Transition circuit to HALF_OPEN state
   */
  private transitionToHalfOpen(stage: string): void {
    const timer = this.recoveryTimers.get(stage);
    if (timer) {
      clearTimeout(timer);
      this.recoveryTimers.delete(stage);
    }

    this.states.set(stage, CircuitBreakerState.HALF_OPEN);
    const metrics = this.metrics.get(stage)!;
    metrics.currentSuccessCount = 0;
    metrics.currentFailureCount = 0;

    const config = this.configs.get(stage) || this.defaultConfig;
    if (config.enableLogging) {
      this.logger.log(`Circuit breaker HALF_OPEN for stage: ${stage}`);
    }

    this.emitStateChange(stage, CircuitBreakerState.HALF_OPEN);
  }

  /**
   * Transition circuit to CLOSED state
   */
  private transitionToClosed(stage: string): void {
    const timer = this.recoveryTimers.get(stage);
    if (timer) {
      clearTimeout(timer);
      this.recoveryTimers.delete(stage);
    }

    this.states.set(stage, CircuitBreakerState.CLOSED);
    const metrics = this.metrics.get(stage)!;
    metrics.currentFailureCount = 0;
    metrics.currentSuccessCount = 0;

    const config = this.configs.get(stage) || this.defaultConfig;
    if (config.enableLogging) {
      this.logger.log(`Circuit breaker CLOSED for stage: ${stage} - Service recovered`);
    }

    this.emitStateChange(stage, CircuitBreakerState.CLOSED);
  }

  /**
   * Execute function with timeout
   */
  private executeWithTimeout<T>(
    handler: () => Promise<T>,
    timeoutMs: number
  ): Promise<T> {
    return Promise.race([
      handler(),
      new Promise<T>((_, reject) =>
        setTimeout(
          () => reject(new Error(`Operation timeout after ${timeoutMs}ms`)),
          timeoutMs
        )
      ),
    ]);
  }

  /**
   * Emit state change event
   */
  private emitStateChange(stage: string, newState: CircuitBreakerState): void {
    const config = this.configs.get(stage) || this.defaultConfig;
    const metrics = this.metrics.get(stage)!;

    this.eventEmitter.emit(`circuit-breaker.${stage}.state-changed`, {
      stage,
      newState,
      metrics: { ...metrics },
      timestamp: new Date(),
    });

    config.onStateChange?.(stage, newState, { ...metrics });
  }

  /**
   * Cleanup resources
   */
  onModuleDestroy(): void {
    for (const [, timer] of this.recoveryTimers.entries()) {
      clearTimeout(timer);
    }
    this.recoveryTimers.clear();
  }
}