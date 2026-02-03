import { IEvent } from '@nestjs/cqrs';
import { PriceNormalizedEvent } from './price-normalized.event';

/**
 * Represents an individual source price in the aggregated result
 */
export interface AggregatedPriceSource {
  /**
   * Data source identifier
   */
  source: string;

  /**
   * Normalized price from this source
   */
  price: number;

  /**
   * Source weight from SOURCE_WEIGHTS configuration
   */
  sourceWeight: number;

  /**
   * Confidence score from this source
   */
  confidence: number;

  /**
   * Weight applied to this source in the aggregation
   * (may differ from sourceWeight if source is excluded)
   */
  aggregationWeight: number;

  /**
   * Whether this price was used in the final aggregation
   */
  included: boolean;

  /**
   * Reason if price was excluded (e.g., 'OUTLIER', 'LOW_CONFIDENCE', 'FAILED_VALIDATION')
   */
  exclusionReason?: string;

  /**
   * Deviation from aggregated price as percentage
   */
  deviationPercent: number;
}

/**
 * Event emitted after prices from multiple sources have been aggregated
 * into a final consensus price using weighted aggregation methods.
 *
 * This event represents the result of processing multiple normalized prices
 * with outlier detection and source weighting based on SOURCE_WEIGHTS configuration.
 * The aggregation produces statistical measures of consensus quality.
 *
 * @example
 * ```
 * new PriceAggregatedEvent(
 *   [normalizedEventBitcoin, normalizedEventKraken],
 *   {
 *     symbol: 'EUR/USD',
 *     aggregatedPrice: 1.0852,
 *     aggregationMethod: 'WEIGHTED_MEDIAN',
 *     sources: [
 *       {
 *         source: 'Bloomberg',
 *         price: 1.0850,
 *         sourceWeight: 2.0,
 *         confidence: 1.0,
 *         aggregationWeight: 0.5,
 *         included: true,
 *         deviationPercent: 0.02
 *       }
 *     ]
 *   },
 *   executionTime
 * )
 * ```
 */
export class PriceAggregatedEvent implements IEvent {
  /**
   * Unique identifier for this event instance
   */
  readonly eventId: string;

  /**
   * Timestamp when the event was created
   */
  readonly eventTimestamp: Date;

  /**
   * References to the normalized price events that were aggregated
   */
  readonly sourceEvents: PriceNormalizedEvent[];

  /**
   * Asset symbol being aggregated
   */
  readonly symbol: string;

  /**
   * Final aggregated price after consensus calculation
   */
  readonly aggregatedPrice: number;

  /**
   * Aggregation method used
   * Examples: 'WEIGHTED_MEDIAN', 'WEIGHTED_MEAN', 'TRIMMED_MEAN'
   */
  readonly aggregationMethod: string;

  /**
   * Individual source prices included in the aggregation
   * with detailed metrics and weighting information
   */
  readonly sources: AggregatedPriceSource[];

  /**
   * Number of sources used in the final aggregation (included sources)
   */
  readonly sourceCount: number;

  /**
   * Total number of sources that provided prices
   */
  readonly totalSourceCount: number;

  /**
   * Number of prices excluded as outliers or invalid
   */
  readonly excludedCount: number;

  /**
   * Breakdown of exclusion reasons
   */
  readonly exclusionBreakdown: Record<string, number>;

  /**
   * Standard deviation of the included prices
   */
  readonly standardDeviation: number;

  /**
   * Coefficient of variation (CV = stdDev / mean)
   * Values closer to 0 indicate better consensus
   */
  readonly coefficientOfVariation: number;

  /**
   * Minimum price from included sources
   */
  readonly minPrice: number;

  /**
   * Maximum price from included sources
   */
  readonly maxPrice: number;

  /**
   * Average confidence score weighted by source weights
   */
  readonly weightedConfidence: number;

  /**
   * Overall quality score (0-1) based on statistical measures
   * Factors: source count, CV, confidence
   */
  readonly qualityScore: number;

  /**
   * Trace ID for pipeline tracking (inherited from source events)
   */
  readonly traceId: string;

  /**
   * Stage execution time in milliseconds
   */
  readonly executionTime: number;

  /**
   * Aggregation interval identifier (for batch processing)
   */
  readonly intervalId?: string;

  /**
   * Additional metadata about aggregation
   */
  readonly metadata?: Record<string, any>;

  constructor(
    sourceEvents: PriceNormalizedEvent[],
    aggregationData: {
      symbol: string;
      aggregatedPrice: number;
      aggregationMethod: string;
      sources: AggregatedPriceSource[];
      standardDeviation: number;
      coefficientOfVariation: number;
      minPrice: number;
      maxPrice: number;
      weightedConfidence: number;
      qualityScore: number;
      intervalId?: string;
      metadata?: Record<string, any>;
    },
    executionTime: number
  ) {
    this.sourceEvents = sourceEvents;
    this.symbol = aggregationData.symbol;
    this.aggregatedPrice = aggregationData.aggregatedPrice;
    this.aggregationMethod = aggregationData.aggregationMethod;
    this.sources = aggregationData.sources;
    this.sourceCount = aggregationData.sources.filter((s) => s.included).length;
    this.totalSourceCount = aggregationData.sources.length;
    this.excludedCount = this.totalSourceCount - this.sourceCount;
    this.standardDeviation = aggregationData.standardDeviation;
    this.coefficientOfVariation = aggregationData.coefficientOfVariation;
    this.minPrice = aggregationData.minPrice;
    this.maxPrice = aggregationData.maxPrice;
    this.weightedConfidence = aggregationData.weightedConfidence;
    this.qualityScore = aggregationData.qualityScore;
    this.executionTime = executionTime;
    this.intervalId = aggregationData.intervalId;
    this.metadata = aggregationData.metadata;
    this.traceId = sourceEvents[0]?.traceId || this.generateTraceId();
    this.eventId = `pa-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    this.eventTimestamp = new Date();
    this.exclusionBreakdown = this.calculateExclusionBreakdown();
  }

  /**
   * Get stage identifier for logging and metrics
   */
  getStage(): string {
    return 'PRICE_AGGREGATED';
  }

  /**
   * Calculate the price spread as a percentage
   * (maxPrice - minPrice) / aggregatedPrice * 100
   */
  getPriceSpreadPercentage(): number {
    if (this.aggregatedPrice === 0) return 0;
    return ((this.maxPrice - this.minPrice) / this.aggregatedPrice) * 100;
  }

  /**
   * Check if aggregation is of high quality based on thresholds
   * Requirements: minimum sources, low CV, high confidence
   */
  isHighQuality(
    minSourceCount: number = 2,
    maxCVThreshold: number = 0.05,
    minConfidenceThreshold: number = 0.85
  ): boolean {
    return (
      this.sourceCount >= minSourceCount &&
      this.coefficientOfVariation <= maxCVThreshold &&
      this.weightedConfidence >= minConfidenceThreshold
    );
  }

  /**
   * Check if consensus is strong (low variance)
   */
  hasStrongConsensus(cvThreshold: number = 0.03): boolean {
    return this.coefficientOfVariation <= cvThreshold;
  }

  /**
   * Get detailed quality assessment
   */
  getQualityAssessment(): {
    isHighQuality: boolean;
    hasStrongConsensus: boolean;
    qualityLevel: 'EXCELLENT' | 'GOOD' | 'FAIR' | 'POOR';
    issues: string[];
  } {
    const issues: string[] = [];
    let qualityLevel: 'EXCELLENT' | 'GOOD' | 'FAIR' | 'POOR' = 'EXCELLENT';

    if (this.sourceCount < 2) {
      issues.push('Insufficient sources');
      qualityLevel = 'POOR';
    }

    if (this.coefficientOfVariation > 0.1) {
      issues.push('High price variance');
      if (qualityLevel === 'EXCELLENT') qualityLevel = 'FAIR';
    } else if (this.coefficientOfVariation > 0.05) {
      issues.push('Moderate price variance');
      if (qualityLevel === 'EXCELLENT') qualityLevel = 'GOOD';
    }

    if (this.weightedConfidence < 0.7) {
      issues.push('Low confidence');
      qualityLevel = 'POOR';
    } else if (this.weightedConfidence < 0.85) {
      issues.push('Moderate confidence');
      if (qualityLevel === 'EXCELLENT') qualityLevel = 'GOOD';
    }

    if (this.excludedCount > this.sourceCount) {
      issues.push('More sources excluded than included');
      qualityLevel = 'POOR';
    }

    return {
      isHighQuality: this.isHighQuality(),
      hasStrongConsensus: this.hasStrongConsensus(),
      qualityLevel,
      issues,
    };
  }

  private calculateExclusionBreakdown(): Record<string, number> {
    const breakdown: Record<string, number> = {};

    for (const source of this.sources) {
      if (!source.included && source.exclusionReason) {
        breakdown[source.exclusionReason] = (breakdown[source.exclusionReason] || 0) + 1;
      }
    }

    return breakdown;
  }

  private generateTraceId(): string {
    return `trace-${Date.now()}-${Math.random().toString(36).substr(2, 12)}`;
  }
}