<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries;

use DateTimeInterface;

class RawSample
{
    protected string $key;

    protected float|string $value;
    /**
     * Value in milliseconds without decimals
     * if not stated defaults to redis insert dateTime
     */
    protected ?int $tsms;

    public function __construct(string $key, float|string $value, ?int $tsms = null)
    {
        $this->key = $key;
        $this->value = $value;
        $this->tsms = $tsms;
    }

    public static function createFromTimestamp(string $key, float|string $value, ?int $tsms): RawSample
    {
        return new self($key, $value, $tsms);
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getValue(): float|string
    {
        return $this->value;
    }

    public function getDateTime(): ?DateTimeInterface
    {
        return TimeStampToDateTime::dateTimeFromTimestampWithMs($this->tsms);
    }

    public function getTimestampWithMs(): string
    {
        if ($this->tsms === null) {
            return '*';
        }
        return (string) $this->tsms;
    }

    public function toRedisParams(): array
    {
        return [$this->getKey(), $this->getTimestampWithMs(), (string) $this->getValue()];
    }
}
