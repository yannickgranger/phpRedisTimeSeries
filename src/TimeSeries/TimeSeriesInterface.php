<?php

namespace Palicao\PhpRedisTimeSeries\TimeSeries;

use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\AggregationRule;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Filter;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Metadata;

/**
 * All date and times are expressed in timestamp with milliseconds
 */
interface TimeSeriesInterface
{
    public function info(string $key): Metadata;

    public function create(string $key, int $retentionMs = null, array $labels = []): void;

    public function alter(string $key, int $retentionMs = null, array $labels = []): void;

    public function add(RawSample $rawSample, int $retentionMs = null, array $labels = []): RawSample;

    public function addMany(array $rawSamples): array;

    public function getLastRaw(string $key): RawSample|array;

    public function getLastRaws(Filter $filter): array;

    public function getKeysByFilter(Filter $filter);

    public function range(string $key, int $from = null, int $to = null, int $count = null, AggregationRule $rule = null, bool $reverse = false);

    public function multiRange(Filter $filter, int $from = null, int $to = null, int $count = null, AggregationRule $rule = null, bool $reverse = false);

    public function multiRangeWithLabels(Filter $filter, int $from = null, int $to = null, int $count = null, AggregationRule $rule = null, bool $reverse = false);

    public function unlink(string $key);
}
