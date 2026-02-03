import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { EventEmitterModule } from '@nestjs/event-emitter';

import { DataReceptionService } from './services/data-reception.service';
import { AggregationService } from './services/aggregation.service';
import { NormalizationService } from './services/normalization.service';
import { OutlierDetectionService } from './services/outlier-detection.service';
import { DataStorageService } from './services/data-storage.service';

import { WeightedAverageAggregator } from './strategies/aggregators/weighted-average.aggregator';
import { MedianAggregator } from './strategies/aggregators/median.aggregator';
import { TrimmedMeanAggregator } from './strategies/aggregators/trimmed-mean.aggregator';

import { AggregatorOrchestrationModule } from './modules/aggregator-orchestration.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env.local', '.env'],
    }),
    HttpModule,
    EventEmitterModule.forRoot({
      wildcard: true,
      delimiter: '.',
      newListener: false,
      removeListener: false,
      maxListeners: 20,
      verboseMemoryLeak: true,
      ignoreErrors: false,
    }),
    // Import the complete orchestration pipeline module
    AggregatorOrchestrationModule,
  ],
  controllers: [],
  providers: [
    // Core Services
    DataReceptionService,
    AggregationService,
    NormalizationService,
    OutlierDetectionService,
    DataStorageService,

    // Aggregation Strategies
    WeightedAverageAggregator,
    MedianAggregator,
    TrimmedMeanAggregator,
  ],
  exports: [
    AggregationService,
    DataReceptionService,
    NormalizationService,
    OutlierDetectionService,
    DataStorageService,
  ],
})
export class AppModule {}