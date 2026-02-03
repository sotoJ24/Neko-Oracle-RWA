import { IEvent } from '@nestjs/cqrs';
// import { PriceReceivedEvent } from './price-aggregated.event';
import { PriceReceivedEvent } from './price-received.event';

/**
 * Event emitted after a price has been normalized to a standard format.
 * Normalization includes:
 * - Converting to decimal format with specified precision
 * - Applying precision rules based on symbol and asset type
 * - Standardizing units across different sources
 * - Validating price ranges
 *
 * @example
 * ```
 * new PriceNormalizedEvent(
 *   priceReceivedEvent,
 *   {
 *     normalizedPrice: 1.085000,
 *     precision: 6,
 *     currency: 'EUR',
 *     baseCurrency: 'USD',
 *     normalizationMethod: 'DECIMAL_PRECISION'
 *   },
 *   executionTime
 * )
 * ```
 */
export class PriceNormalizedEvent implements IEvent {
  /**
   * Unique identifier for this event instance
   */
  readonly eventId: string;

  /**
   * Timestamp when the event was created
   */
  readonly eventTimestamp: Date;

  /**
   * Reference to the original PriceReceivedEvent
   */
  readonly originalEvent: PriceReceivedEvent;

  /**
   * Asset symbol (inherited from original event)
   */
  readonly symbol: string;

  /**
   * Price after normalization
   */
  readonly normalizedPrice: number;

  /**
   * Decimal precision used for normalization
   * E.g., 6 means 6 decimal places for EUR/USD
   */
  readonly precision: number;

  /**
   * Primary currency of the price pair
   */
  readonly currency: string;

  /**
   * Base currency for the pair (e.g., USD in EUR/USD)
   */
  readonly baseCurrency: string;

  /**
   * Original raw price before normalization
   */
  readonly originalPrice: number;

  /**
   * Data source identifier (inherited from original event)
   */
  readonly source: string;

  /**
   * Source weight (inherited from original event)
   */
  readonly sourceWeight: number;

  /**
   * Confidence score carried forward from reception stage
   */
  readonly confidence: number;

  /**
   * Normalization method applied
   * Examples: 'DECIMAL_PRECISION', 'RANGE_VALIDATION', 'UNIT_CONVERSION'
   */
  readonly normalizationMethod: string;

  /**
   * Trace ID for pipeline tracking (inherited from original event)
   */
  readonly traceId: string;

  /**
   * Stage execution time in milliseconds
   */
  readonly executionTime: number;

  /**
   * Whether normalization was successful
   */
  readonly isValid: boolean;

  /**
   * Validation error message if isValid is false
   */
  readonly validationError?: string;

  /**
   * Additional metadata about normalization
   */
  readonly metadata?: Record<string, any>;

  constructor(
    originalEvent: PriceReceivedEvent,
    normalizationData: {
      normalizedPrice: number;
      precision: number;
      currency: string;
      baseCurrency: string;
      normalizationMethod: string;
      isValid?: boolean;
      validationError?: string;
      metadata?: Record<string, any>;
    },
    executionTime: number
  ) {
    this.originalEvent = originalEvent;
    this.symbol = originalEvent.symbol;
    this.normalizedPrice = normalizationData.normalizedPrice;
    this.precision = normalizationData.precision;
    this.currency = normalizationData.currency;
    this.baseCurrency = normalizationData.baseCurrency;
    this.originalPrice = originalEvent.price;
    this.source = originalEvent.source;
    this.sourceWeight = originalEvent.sourceWeight;
    this.confidence = originalEvent.confidence;
    this.normalizationMethod = normalizationData.normalizationMethod;
    this.executionTime = executionTime;
    this.isValid = normalizationData.isValid ?? true;
    this.validationError = normalizationData.validationError;
    this.metadata = normalizationData.metadata;
    this.traceId = originalEvent.traceId;
    this.eventId = `pn-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    this.eventTimestamp = new Date();
  }

  /**
   * Get stage identifier for logging and metrics
   */
  getStage(): string {
    return 'PRICE_NORMALIZED';
  }

  /**
   * Calculate normalization deviation from original price
   */
  getNormalizationDeviation(): number {
    return Math.abs(this.normalizedPrice - this.originalPrice) / this.originalPrice;
  }
}