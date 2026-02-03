import { IEvent } from '@nestjs/cqrs';
import { PriceInputDto } from '../dto/price-input.dto';
import { getSourceWeight } from '../config/source-weights.config';

/**
 * Event emitted when a price is received from a data source.
 * This marks the entry point of the aggregation pipeline.
 *
 * Created from PriceInputDto and enriched with source weight information
 * and trace tracking for observability across the pipeline.
 *
 * @example
 * ```
 * const dto = new PriceInputDto();
 * dto.symbol = 'EUR/USD';
 * dto.price = 1.0850;
 * dto.source = 'Bloomberg';
 * dto.timestamp = '2026-02-02T16:34:24Z';
 *
 * new PriceReceivedEvent(dto);
 * ```
 */
export class PriceReceivedEvent implements IEvent {
  /**
   * Unique identifier for this event instance
   */
  readonly eventId: string;

  /**
   * Timestamp when the event was created (event processing time)
   */
  readonly eventTimestamp: Date;

  /**
   * Asset symbol (e.g., 'EUR/USD', 'BTC/USD')
   */
  readonly symbol: string;

  /**
   * Raw price value received from the source
   */
  readonly price: number;

  /**
   * Data source identifier (e.g., 'Bloomberg', 'Reuters', 'AlphaVantage')
   */
  readonly source: string;

  /**
   * Timestamp from the source when the price was recorded
   */
  readonly sourceTimestamp: Date;

  /**
   * Source weight from SOURCE_WEIGHTS configuration
   * Higher weight = more trusted source
   */
  readonly sourceWeight: number;

  /**
   * Initial confidence score based on source weight
   * Calculated as: sourceWeight / 2.0 (normalized to 0-1 range)
   */
  readonly confidence: number;

  /**
   * Unique trace ID for tracking through the entire pipeline
   * Propagated through all subsequent events
   */
  readonly traceId: string;

  /**
   * Additional metadata from the source
   */
  readonly metadata?: Record<string, any>;

  constructor(priceInput: PriceInputDto, traceId?: string) {
    this.symbol = priceInput.symbol;
    this.price = priceInput.price;
    this.source = priceInput.source;
    this.sourceTimestamp = new Date(priceInput.timestamp);
    this.sourceWeight = getSourceWeight(priceInput.source);
    // Normalize weight to confidence score (0-1 range)
    // Assuming max weight is 2.0, divide by 2.0 to normalize
    this.confidence = Math.min(this.sourceWeight / 2.0, 1.0);
    this.eventId = `pr-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    this.eventTimestamp = new Date();
    this.traceId = traceId || this.generateTraceId();
  }

  /**
   * Get stage identifier for logging and metrics
   */
  getStage(): string {
    return 'PRICE_RECEIVED';
  }

  private generateTraceId(): string {
    return `trace-${Date.now()}-${Math.random().toString(36).substr(2, 12)}`;
  }
}