<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries;

use DateTimeImmutable;
use DateTimeInterface;
use Palicao\PhpRedisTimeSeries\TimeSeries\Exception\TimestampParsingException;

final class TimeStampToDateTime
{
    public static function dateTimeFromTimestampWithMs(int $timestamp): DateTimeInterface
    {
        $dateTime = DateTimeImmutable::createFromFormat('U.u', sprintf('%.03f', $timestamp / 1000));
        if ($dateTime === false) {
            throw new TimestampParsingException(sprintf("Unable to parse timestamp: %d", $timestamp));
        }
        return $dateTime;
    }
}
