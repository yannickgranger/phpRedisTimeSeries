<?php
declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\Tests\Unit;

use DateTimeImmutable;
use Palicao\PhpRedisTimeSeries\TimeSeries\RawSample;
use PHPUnit\Framework\TestCase;

class SampleTest extends TestCase
{
    public function testGetTimestampWithMs(): void
    {
        $sample = new RawSample('a', 1, new DateTimeImmutable('2017-01-01T20.01.06.234'));
        $ts = $sample->getTimestampWithMs();
        self::assertEquals(1483300866234, $ts);
    }

    public function testCreateFromTimestamp(): void
    {
        $sample = RawSample::createFromTimestamp('a', 1, 1483300866234);
        $dateTime = $sample->getDateTime();
        self::assertEquals(new DateTimeImmutable('2017-01-01T20.01.06.234'), $dateTime);
    }

    public function testCurrentTimestampReturnsStar(): void
    {
        $sample = new RawSample('a', 1);
        $params = $sample->toRedisParams();
        self::assertEquals(['a', '*', 1], $params);
    }

    public function testToRedisParams(): void
    {
        $sample = RawSample::createFromTimestamp('a', 1, 1483300866234);
        $params = $sample->toRedisParams();
        self::assertEquals(['a', 1483300866234, 1], $params);
    }
}
