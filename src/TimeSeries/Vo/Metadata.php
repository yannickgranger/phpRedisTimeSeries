<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries\Vo;

use DateTimeInterface;
use Palicao\PhpRedisTimeSeries\TimeSeries\TimeStampToDateTime;

final class Metadata
{
    private DateTimeInterface $lastTimestamp;

    private int $retentionTime;

    private int $chunkCount;

    private int $maxRawsPerChunk;

    /** @var array<Label> */
    private array $labels;

    private string|null $sourceKey;

    /** @var array<AggregationRule> */
    private array $rules;

    public function __construct(
        DateTimeInterface $lastTimestamp,
        int $retentionTime = 0,
        int $chunkCount = 0,
        int $maxRawsPerChunk = 0,
        array $labels = [],
        array $rules = [],
        ?string $sourceKey = null,
    ) {
        $this->lastTimestamp = $lastTimestamp;
        $this->retentionTime = $retentionTime;
        $this->chunkCount = $chunkCount;
        $this->maxRawsPerChunk = $maxRawsPerChunk;
        $this->labels = $labels;
        $this->rules = $rules;
        $this->sourceKey = $sourceKey;
    }

    /**
     * @param int $lastTimestamp
     * @param int $retentionTime
     * @param int $chunkCount
     * @param int $maxRawsPerChunk
     * @param Label[] $labels
     * @param string|null $sourceKey
     * @param AggregationRule[] $rules
     * @return static
     */
    public static function fromRedis(
        int $lastTimestamp,
        int $retentionTime = 0,
        int $chunkCount = 0,
        int $maxRawsPerChunk = 0,
        array $labels = [],
        array $rules = [],
        ?string $sourceKey = null
    ): self {
        $dateTime = TimeStampToDateTime::dateTimeFromTimestampWithMs($lastTimestamp);
        return new self(
            lastTimestamp: $dateTime,
            retentionTime: $retentionTime,
            chunkCount: $chunkCount,
            maxRawsPerChunk: $maxRawsPerChunk,
            labels: $labels,
            rules: $rules,
            sourceKey: $sourceKey
        );
    }

    public function getLastTimestamp(): DateTimeInterface
    {
        return $this->lastTimestamp;
    }

    public function getRetentionTime(): int
    {
        return $this->retentionTime;
    }

    public function getChunkCount(): int
    {
        return $this->chunkCount;
    }

    public function getMaxRawsPerChunk(): int
    {
        return $this->maxRawsPerChunk;
    }

    public function getLabels(): array
    {
        return $this->labels;
    }

    public function getSourceKey(): ?string
    {
        return $this->sourceKey;
    }

    public function getRules(): array
    {
        return $this->rules;
    }
}
