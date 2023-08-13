<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries;

use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Label;

final class RawSampleWithLabels extends RawSample
{
    /** @var array<Label> */
    private array $labels;

    /**
     * RawSampleWithLabels constructor.
     */
    public function __construct(string $key, float|string $value, array $labels = [], ?int $tsms = null)
    {
        parent::__construct(key: $key, value: $value, tsms: $tsms = null);
        $this->labels = $labels;
    }

    public static function createFromTimestampAndLabels(
        string $key,
        float|string $value,
        int $timestampWithMs,
        array $labels = []
    ): RawSampleWithLabels {
        return new self(key: $key, value: $value, labels: $labels, tsms: $timestampWithMs);
    }

    public function getLabels(): array
    {
        return $this->labels;
    }
}
