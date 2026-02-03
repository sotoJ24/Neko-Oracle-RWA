/**
 * Normalized price data structure from various sources
 * This is the input to the aggregation engine
 */
export interface NormalizedPrice {
  symbol: string;
  price: number;
  timestamp: number;
  source: string;
  weight?: number; // Optional weight override for this specific price
}
