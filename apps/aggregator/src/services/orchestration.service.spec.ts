import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';

import { OrchestrationService, PipelineResult } from './orchestration.service';
import { CircuitBreakerMiddleware, CircuitBreakerState, CircuitBreakerOpenError } from '../middleware/circuit-breaker.middleware';

import { DataReceptionService } from './data-reception.service';
import { NormalizationService } from './normalization.service';
import { OutlierDetectionService } from './outlier-detection.service';
import { AggregationService } from './aggregation.service';
import { DataStorageService } from './data-storage.service';

import { PriceInputDto } from '../dto/price-input.dto';
import {
  PriceReceivedEvent,
  PriceNormalizedEvent,
  PriceAggregatedEvent,
  AggregatedPriceSource,
} from '../events/price.events';

describe('OrchestrationService', () => {
  let service: OrchestrationService;
  let configService: ConfigService;
  let eventEmitter: EventEmitter2;
  let circuitBreaker: CircuitBreakerMiddleware;
  let receptionService: DataReceptionService;
  let normalizationService: NormalizationService;
  let outlierDetectionService: OutlierDetectionService;
  let aggregationService: AggregationService;
  let storageService: DataStorageService;

  const mockConfig = {
    aggregation: {
      realtimeEnabled: true,
      batchEnabled: false,
      batchIntervalMs: 5000,
      batchMinPrices: 3,
      aggregationMethod: 'WEIGHTED_MEDIAN',
      minSourceCount: 2,
      maxSourceCount: 50,
      outlierThreshold: 0.03,
      outlierMethod: 'MODIFIED_Z_SCORE',
      enableNormalization: true,
      precisionBySymbol: {
        'EUR/USD': 6,
        'BTC/USD': 2,
      },
      minConfidence: 0.7,
      maxCoefficientOfVariation: 0.05,
      normalizationTimeoutMs: 5000,
      outlierDetectionTimeoutMs: 3000,
      aggregationTimeoutMs: 5000,
      storageTimeoutMs: 10000,
      circuitBreaker: {
        failureThreshold: 5,
        successThreshold: 2,
        timeoutMs: 30000,
        halfOpenMaxRequests: 3,
        enableLogging: false,
      },
      enableStorage: true,
      storageRetentionDays: 30,
      enableMetrics: true,
      metricsInterval: 60000,
      verboseLogging: false,
    },
  };

  beforeEach(async () => {
    // Mock services
    const mockReceptionService = {
      receivePrice: jest.fn(),
    };

    const mockNormalizationService = {
      normalize: jest.fn(),
      createFallbackNormalization: jest.fn(),
    };

    const mockOutlierDetectionService = {
      detectOutliers: jest.fn(),
    };

    const mockAggregationService = {
      aggregate: jest.fn(),
    };

    const mockStorageService = {
      store: jest.fn(),
    };

    const mockConfigService = {
      get: jest.fn((key) => mockConfig[key]),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrchestrationService,
        {
          provide: ConfigService,
          useValue: mockConfigService,
        },
        {
          provide: EventEmitter2,
          useValue: {
            emit: jest.fn(),
            on: jest.fn(),
            off: jest.fn(),
          },
        },
        {
          provide: CircuitBreakerMiddleware,
          useValue: {
            execute: jest.fn(),
            executeSync: jest.fn(),
            registerStage: jest.fn(),
            getState: jest.fn(),
            getMetrics: jest.fn(),
            getAllMetrics: jest.fn(),
            reset: jest.fn(),
            open: jest.fn(),
          },
        },
        {
          provide: DataReceptionService,
          useValue: mockReceptionService,
        },
        {
          provide: NormalizationService,
          useValue: mockNormalizationService,
        },
        {
          provide: OutlierDetectionService,
          useValue: mockOutlierDetectionService,
        },
        {
          provide: AggregationService,
          useValue: mockAggregationService,
        },
        {
          provide: DataStorageService,
          useValue: mockStorageService,
        },
      ],
    }).compile();

    service = module.get<OrchestrationService>(OrchestrationService);
    configService = module.get<ConfigService>(ConfigService);
    eventEmitter = module.get<EventEmitter2>(EventEmitter2);
    circuitBreaker = module.get<CircuitBreakerMiddleware>(CircuitBreakerMiddleware);
    receptionService = module.get<DataReceptionService>(DataReceptionService);
    normalizationService = module.get<NormalizationService>(NormalizationService);
    outlierDetectionService = module.get<OutlierDetectionService>(OutlierDetectionService);
    aggregationService = module.get<AggregationService>(AggregationService);
    storageService = module.get<DataStorageService>(DataStorageService);

    // Setup default mocks for circuit breaker to pass through handlers
    (circuitBreaker.execute as jest.Mock).mockImplementation(
      (stage, handler) => handler()
    );
  });

  describe('processPriceData - Happy Path', () => {
    it('should successfully process a price through the complete real-time pipeline', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
          isValid: true,
          metadata: {},
        },
        15
      );

      const aggregatedSource: AggregatedPriceSource = {
        source: 'Bloomberg',
        price: 1.085000,
        sourceWeight: 2.0,
        confidence: 1.0,
        aggregationWeight: 0.5,
        included: true,
        deviationPercent: 0,
      };

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.085000,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [aggregatedSource],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 1.085000,
          maxPrice: 1.085000,
          weightedConfidence: 1.0,
          qualityScore: 1.0,
        },
        25
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.event).toEqual(priceAggregatedEvent);
      expect(result.traceId).toBe(priceReceivedEvent.traceId);
      expect(result.status.priceReceived).toBe(true);
      expect(result.status.priceNormalized).toBe(true);
      expect(result.status.outliersDetected).toBe(true);
      expect(result.status.priceAggregated).toBe(true);
      expect(result.status.dataStored).toBe(true);
      expect(result.totalExecutionTime).toBeGreaterThan(0);
      expect(Object.keys(result.executionTimes).length).toBe(5);

      // Verify services were called in order
      expect(receptionService.receivePrice).toHaveBeenCalledWith(priceDto);
      expect(normalizationService.normalize).toHaveBeenCalledWith(priceReceivedEvent);
      expect(outlierDetectionService.detectOutliers).toHaveBeenCalled();
      expect(aggregationService.aggregate).toHaveBeenCalled();
      expect(storageService.store).toHaveBeenCalledWith(priceAggregatedEvent);
    });

    it('should complete successfully with timing metrics', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.085000,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 1.085000,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 1.085000,
          maxPrice: 1.085000,
          weightedConfidence: 1.0,
          qualityScore: 1.0,
        },
        20
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.executionTimes['PRICE_RECEIVED']).toBeGreaterThanOrEqual(0);
      expect(result.executionTimes['PRICE_NORMALIZED']).toBeGreaterThanOrEqual(0);
      expect(result.executionTimes['OUTLIER_DETECTION']).toBeGreaterThanOrEqual(0);
      expect(result.executionTimes['PRICE_AGGREGATED']).toBeGreaterThanOrEqual(0);
      expect(result.executionTimes['DATA_STORAGE']).toBeGreaterThanOrEqual(0);
      expect(result.totalExecutionTime).toBeGreaterThan(0);
    });
  });

  describe('processPriceData - Batch Processing', () => {
    beforeEach(() => {
      (configService.get as jest.Mock).mockImplementation((key) => ({
        ...mockConfig,
        aggregation: {
          ...mockConfig.aggregation,
          realtimeEnabled: false,
          batchEnabled: true,
        },
      })[key]);
    });

    it('should queue price for batch processing when batch is enabled', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.event).toBeUndefined(); // No aggregation in batch mode yet
      expect(result.status.priceReceived).toBe(true);
      expect(result.status.priceNormalized).toBe(true);
      expect(result.status.outliersDetected).toBe(false);
      expect(result.status.priceAggregated).toBe(false);
      expect(result.status.dataStored).toBe(false);
    });

    it('should not process if real-time is disabled and batch is disabled', async () => {
      // Arrange
      (configService.get as jest.Mock).mockImplementation((key) => ({
        ...mockConfig,
        aggregation: {
          ...mockConfig.aggregation,
          realtimeEnabled: false,
          batchEnabled: false,
        },
      })[key]);

      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.event).toBeUndefined();
    });
  });

  describe('processPriceData - Error Handling', () => {
    it('should handle reception stage failure gracefully', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const error = new Error('Reception service failed');
      jest.spyOn(receptionService, 'receivePrice').mockRejectedValue(error);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(false);
      expect(result.error).toEqual(error);
      expect(result.failedStage).toBe('PRICE_RECEIVED');
      expect(result.status.priceReceived).toBe(false);
      expect(result.totalExecutionTime).toBeGreaterThan(0);
    });

    it('should handle normalization stage failure with fallback', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const fallbackNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.0850, // Raw price as fallback
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'FALLBACK',
        },
        5
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest
        .spyOn(normalizationService, 'normalize')
        .mockRejectedValue(new Error('Normalization failed'));
      jest
        .spyOn(normalizationService, 'createFallbackNormalization')
        .mockResolvedValue(fallbackNormalizedEvent);

      // Mock circuit breaker to use fallback
      (circuitBreaker.execute as jest.Mock).mockImplementation(
        async (stage, handler, options) => {
          if (stage === 'PRICE_NORMALIZED') {
            try {
              return await handler();
            } catch (err) {
              return options.fallback?.(err);
            }
          }
          return handler();
        }
      );

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [fallbackNormalizedEvent],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.0850,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 1.0850,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 1.0850,
          maxPrice: 1.0850,
          weightedConfidence: 1.0,
          qualityScore: 0.9,
        },
        20
      );

      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([fallbackNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert - Pipeline should recover with fallback
      expect(result.success).toBe(true);
      expect(result.event).toBeDefined();
    });

    it('should handle outlier detection filtering out the price', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([]); // Filtered out

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.event).toBeUndefined(); // No aggregation without valid prices
      expect(result.status.outliersDetected).toBe(true);
      expect(result.status.priceAggregated).toBe(false);
    });

    it('should handle aggregation stage failure', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const error = new Error('Aggregation service failed');

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockRejectedValue(error);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(false);
      expect(result.error).toEqual(error);
      expect(result.failedStage).toBe('PRICE_AGGREGATED');
      expect(result.status.priceAggregated).toBe(false);
    });

    it('should handle storage stage failure gracefully (non-blocking)', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.085000,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 1.085000,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 1.085000,
          maxPrice: 1.085000,
          weightedConfidence: 1.0,
          qualityScore: 1.0,
        },
        20
      );

      const storageError = new Error('Storage service failed');

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockRejectedValue(storageError);

      // Mock circuit breaker to use fallback for storage
      (circuitBreaker.execute as jest.Mock).mockImplementation(
        async (stage, handler, options) => {
          if (stage === 'DATA_STORAGE') {
            try {
              return await handler();
            } catch (err) {
              return options.fallback?.(err);
            }
          }
          return handler();
        }
      );

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert - Pipeline should succeed even though storage failed
      expect(result.success).toBe(true);
      expect(result.event).toEqual(priceAggregatedEvent);
      expect(result.status.priceAggregated).toBe(true);
      expect(result.status.dataStored).toBe(true); // Marked as successful despite storage error
    });
  });

  describe('processPriceData - Quality Assessment', () => {
    it('should process low quality aggregation successfully but log warning', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const lowQualityAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.085000,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 1.085000,
              sourceWeight: 1.0,
              confidence: 0.5, // Low confidence
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0.2, // High standard deviation
          coefficientOfVariation: 0.15, // Exceeds threshold
          minPrice: 1.085000,
          maxPrice: 1.085000,
          weightedConfidence: 0.5,
          qualityScore: 0.3,
        },
        20
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(lowQualityAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.event?.qualityScore).toBe(0.3);
      const assessment = result.event?.getQualityAssessment();
      expect(assessment?.isHighQuality).toBe(false);
      expect(assessment?.qualityLevel).toBe('POOR');
    });
  });

  describe('getHealthStatus', () => {
    it('should return health status of all circuit breakers', () => {
      // Arrange
      const mockMetrics = {
        'PRICE_RECEIVED': {
          state: CircuitBreakerState.CLOSED,
          metrics: {
            totalRequests: 100,
            successCount: 95,
            failureCount: 5,
            currentFailureCount: 0,
            currentSuccessCount: 0,
            successRate: 95,
            averageResponseTime: 10.5,
          },
        },
        'PRICE_NORMALIZED': {
          state: CircuitBreakerState.CLOSED,
          metrics: {
            totalRequests: 100,
            successCount: 98,
            failureCount: 2,
            currentFailureCount: 0,
            currentSuccessCount: 0,
            successRate: 98,
            averageResponseTime: 5.2,
          },
        },
        'DATA_STORAGE': {
          state: CircuitBreakerState.OPEN,
          metrics: {
            totalRequests: 50,
            successCount: 45,
            failureCount: 5,
            currentFailureCount: 5,
            currentSuccessCount: 0,
            successRate: 90,
            averageResponseTime: 25.0,
          },
        },
      };

      (circuitBreaker.getAllMetrics as jest.Mock).mockReturnValue(mockMetrics);

      // Act
      const health = service.getHealthStatus();

      // Assert
      expect(health.healthy).toBe(false); // One stage is open
      expect(health.stages['PRICE_RECEIVED'].healthy).toBe(true);
      expect(health.stages['PRICE_RECEIVED'].state).toBe('CLOSED');
      expect(health.stages['DATA_STORAGE'].healthy).toBe(false);
      expect(health.stages['DATA_STORAGE'].state).toBe('OPEN');
      expect(health.timestamp).toBeInstanceOf(Date);
    });

    it('should return all healthy when all circuits are closed', () => {
      // Arrange
      const mockMetrics = {
        'PRICE_RECEIVED': {
          state: CircuitBreakerState.CLOSED,
          metrics: {
            totalRequests: 100,
            successCount: 100,
            failureCount: 0,
            currentFailureCount: 0,
            currentSuccessCount: 0,
            successRate: 100,
            averageResponseTime: 5,
          },
        },
      };

      (circuitBreaker.getAllMetrics as jest.Mock).mockReturnValue(mockMetrics);

      // Act
      const health = service.getHealthStatus();

      // Assert
      expect(health.healthy).toBe(true);
    });
  });

  describe('resetCircuitBreaker', () => {
    it('should reset a specific circuit breaker', () => {
      // Act
      service.resetCircuitBreaker('PRICE_NORMALIZED');

      // Assert
      expect(circuitBreaker.reset).toHaveBeenCalledWith('PRICE_NORMALIZED');
    });
  });

  describe('Edge Cases', () => {
    it('should handle price with very small value', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'BTC/USD';
      priceDto.price = 0.00001; // Very small value
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 0.00001,
          precision: 8,
          currency: 'BTC',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent],
        {
          symbol: 'BTC/USD',
          aggregatedPrice: 0.00001,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 0.00001,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 0.00001,
          maxPrice: 0.00001,
          weightedConfidence: 1.0,
          qualityScore: 1.0,
        },
        20
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.event?.aggregatedPrice).toBe(0.00001);
    });

    it('should handle price with very large value', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'BTC/USD';
      priceDto.price = 95000; // Very large value
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 95000,
          precision: 2,
          currency: 'BTC',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent],
        {
          symbol: 'BTC/USD',
          aggregatedPrice: 95000,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 95000,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 95000,
          maxPrice: 95000,
          weightedConfidence: 1.0,
          qualityScore: 1.0,
        },
        20
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.event?.aggregatedPrice).toBe(95000);
    });

    it('should generate unique trace IDs for different prices', async () => {
      // Arrange
      const priceDto1 = new PriceInputDto();
      priceDto1.symbol = 'EUR/USD';
      priceDto1.price = 1.0850;
      priceDto1.source = 'Bloomberg';
      priceDto1.timestamp = new Date().toISOString();

      const priceDto2 = new PriceInputDto();
      priceDto2.symbol = 'EUR/USD';
      priceDto2.price = 1.0851;
      priceDto2.source = 'Reuters';
      priceDto2.timestamp = new Date().toISOString();

      const priceReceivedEvent1 = new PriceReceivedEvent(priceDto1);
      const priceReceivedEvent2 = new PriceReceivedEvent(priceDto2);

      const priceNormalizedEvent1 = new PriceNormalizedEvent(
        priceReceivedEvent1,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const priceNormalizedEvent2 = new PriceNormalizedEvent(
        priceReceivedEvent2,
        {
          normalizedPrice: 1.085100,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent1, priceNormalizedEvent2],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.0851,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 1.085000,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 0.6,
              included: true,
              deviationPercent: 0.01,
            },
            {
              source: 'Reuters',
              price: 1.085100,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 0.4,
              included: true,
              deviationPercent: 0.01,
            },
          ],
          standardDeviation: 0.0001,
          coefficientOfVariation: 0.0001,
          minPrice: 1.085000,
          maxPrice: 1.085100,
          weightedConfidence: 1.0,
          qualityScore: 1.0,
        },
        20
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValueOnce(priceReceivedEvent1);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValueOnce(priceNormalizedEvent1);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValueOnce([priceNormalizedEvent1]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValueOnce(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValueOnce(undefined);

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValueOnce(priceReceivedEvent2);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValueOnce(priceNormalizedEvent2);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValueOnce([priceNormalizedEvent2]);

      // Act
      const result1 = await service.processPriceData(priceDto1);
      const result2 = await service.processPriceData(priceDto2);

      // Assert
      expect(result1.traceId).not.toBe(result2.traceId);
    });
  });

  describe('Circuit Breaker Integration', () => {
    it('should pass timeout configuration to circuit breaker execute', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);

      const executeSpyFn = jest
        .spyOn(circuitBreaker, 'execute')
        .mockImplementation((stage, handler) => handler());

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);

      // Act
      await service.processPriceData(priceDto);

      // Assert
      expect(executeSpyFn).toHaveBeenCalledWith(
        'PRICE_RECEIVED',
        expect.any(Function),
        expect.objectContaining({
          timeout: 2000,
        })
      );
    });

    it('should handle circuit breaker open error with fallback', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const fallbackNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.0850,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'FALLBACK',
        },
        5
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);

      const circuitBreakerError = new CircuitBreakerOpenError('PRICE_NORMALIZED', {
        totalRequests: 100,
        successCount: 95,
        failureCount: 5,
        currentFailureCount: 5,
        currentSuccessCount: 0,
        successRate: 95,
        averageResponseTime: 10,
      });

      (circuitBreaker.execute as jest.Mock).mockImplementation(
        (stage, handler, options) => {
          if (stage === 'PRICE_NORMALIZED') {
            return options.fallback?.(circuitBreakerError);
          }
          return handler();
        }
      );

      jest
        .spyOn(normalizationService, 'createFallbackNormalization')
        .mockResolvedValue(fallbackNormalizedEvent);

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [fallbackNormalizedEvent],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.0850,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 1.0850,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 1.0850,
          maxPrice: 1.0850,
          weightedConfidence: 1.0,
          qualityScore: 0.9,
        },
        20
      );

      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([fallbackNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert - Should recover with fallback
      expect(result.success).toBe(true);
    });
  });

  describe('Disabled Storage', () => {
    beforeEach(() => {
      (configService.get as jest.Mock).mockImplementation((key) => ({
        ...mockConfig,
        aggregation: {
          ...mockConfig.aggregation,
          enableStorage: false,
        },
      })[key]);
    });

    it('should skip storage stage when disabled', async () => {
      // Arrange
      const priceDto = new PriceInputDto();
      priceDto.symbol = 'EUR/USD';
      priceDto.price = 1.0850;
      priceDto.source = 'Bloomberg';
      priceDto.timestamp = new Date().toISOString();

      const priceReceivedEvent = new PriceReceivedEvent(priceDto);
      const priceNormalizedEvent = new PriceNormalizedEvent(
        priceReceivedEvent,
        {
          normalizedPrice: 1.085000,
          precision: 6,
          currency: 'EUR',
          baseCurrency: 'USD',
          normalizationMethod: 'DECIMAL_PRECISION',
        },
        10
      );

      const priceAggregatedEvent = new PriceAggregatedEvent(
        [priceNormalizedEvent],
        {
          symbol: 'EUR/USD',
          aggregatedPrice: 1.085000,
          aggregationMethod: 'WEIGHTED_MEDIAN',
          sources: [
            {
              source: 'Bloomberg',
              price: 1.085000,
              sourceWeight: 2.0,
              confidence: 1.0,
              aggregationWeight: 1.0,
              included: true,
              deviationPercent: 0,
            },
          ],
          standardDeviation: 0,
          coefficientOfVariation: 0,
          minPrice: 1.085000,
          maxPrice: 1.085000,
          weightedConfidence: 1.0,
          qualityScore: 1.0,
        },
        20
      );

      jest.spyOn(receptionService, 'receivePrice').mockResolvedValue(priceReceivedEvent);
      jest.spyOn(normalizationService, 'normalize').mockResolvedValue(priceNormalizedEvent);
      jest.spyOn(outlierDetectionService, 'detectOutliers').mockResolvedValue([priceNormalizedEvent]);
      jest.spyOn(aggregationService, 'aggregate').mockResolvedValue(priceAggregatedEvent);
      jest.spyOn(storageService, 'store').mockResolvedValue(undefined);

      // Act
      const result = await service.processPriceData(priceDto);

      // Assert
      expect(result.success).toBe(true);
      expect(result.status.dataStored).toBe(true); // Marked as completed even though skipped
      expect(storageService.store).not.toHaveBeenCalled(); // Storage not called
    });
  });
});