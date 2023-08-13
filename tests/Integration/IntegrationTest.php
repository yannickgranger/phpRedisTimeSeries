<?php
/* @noinspection PhpUnhandledExceptionInspection */

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\Tests\Integration;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use Palicao\PhpRedisTimeSeries\TimeSeries\Client\RedisClient;
use Palicao\PhpRedisTimeSeries\TimeSeries\Client\RedisConnectionParams;
use Palicao\PhpRedisTimeSeries\TimeSeries\RawSample;
use Palicao\PhpRedisTimeSeries\TimeSeries\RawSampleWithLabels;
use Palicao\PhpRedisTimeSeries\TimeSeries\TimeSeries;
use Palicao\PhpRedisTimeSeries\TimeSeries\TimeStampToDateTime;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\AggregationRule;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Filter;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Label;
use PHPUnit\Framework\TestCase;
use Redis;

class IntegrationTest extends TestCase
{
    private $redisClient;
    private $sut;

    public function setUp(): void
    {
        $host = getenv('REDIS_HOST') ?: 'php-rts-redis';
        $port = getenv('REDIS_PORT') ? (int) getenv('REDIS_PORT') : 6379;
        $connectionParams = new RedisConnectionParams($host, $port);
        $this->redisClient = new RedisClient(new Redis(), $connectionParams);
        $this->redisClient->executeCommand(['FLUSHDB']);
        $this->sut = new TimeSeries($this->redisClient);
    }

    protected function tearDown(): void
    {
        $this->redisClient->executeCommand(['FLUSHDB']);
    }

    public function testAddAndRetrieveAsRange(): void
    {
        $from = new DateTimeImmutable('2019-11-06 20:34:17.000');
        $to = new DateTimeImmutable('2019-11-06 20:34:17.100');

        $this->sut->create(
            'temperature:3:11',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );
        $this->sut->add(new RawSample('temperature:3:11', 30, $from));
        $this->sut->add(new RawSample('temperature:3:11', 42, $to));

        $range = $this->sut->range(
            'temperature:3:11',
            $from,
            $to,
            null,
            new AggregationRule(AggregationRule::AGG_AVG, 10)
        );

        $expectedRange = [
            new RawSample('temperature:3:11', 30, new DateTimeImmutable('2019-11-06 20:34:17.000')),
            new RawSample('temperature:3:11', 42, new DateTimeImmutable('2019-11-06 20:34:17.100'))
        ];

        self::assertEquals($expectedRange, $range);
    }

    public function testAddAndRetrieveWithDuplicatePolicySum(): void
    {
        $dt = new DateTimeImmutable('2019-11-06 20:34:17.000');
        $this->sut->create(
            'temperature:3:11',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')],
            false,
            null,
            TimeSeries::DUPLICATE_POLICY_SUM
        );
        
        $this->sut->add(new RawSample('temperature:3:11', 10.0, $dt));
        $this->sut->add(new RawSample('temperature:3:11', 20.0, $dt));
        
        $result = $this->sut->range('temperature:3:11', $dt, $dt);
        
        self::assertEquals([new RawSample('temperature:3:11', 30.0, $dt)], $result);
    }

    public function testAddAndRetrieveAsMultirangeWithLabelsReverse(): void
    {
        $this->sut->create(
            'temperature:3:11',
            60000,
            [new Label('sensor_id', '3'), new Label('area_id', '11')]
        );
        $this->sut->add(
            new RawSample('temperature:3:11', 30, new DateTimeImmutable('2019-11-06 20:34:10.400'))
        );
        $this->sut->add(
            new RawSample('temperature:3:11', 42, new DateTimeImmutable('2019-11-06 20:34:11.400'))
        );

        $this->sut->create(
            'temperature:3:12',
            60000,
            [new Label('sensor_id', '3'), new Label('area_id', '12')]
        );
        $this->sut->add(
            new RawSample('temperature:3:12', 34, new DateTimeImmutable('2019-11-06 20:34:10.000'))
        );
        $this->sut->add(
            new RawSample('temperature:3:12', 48, new DateTimeImmutable('2019-11-06 20:34:11.000'))
        );

        $range = $this->sut->multiRangeWithLabels(
            new Filter('sensor_id', '3'),
            null,
            null,
            null,
            new AggregationRule(AggregationRule::AGG_AVG, 60000), // 1-minute aggregation
            true
        );

        $expectedRange = [
            new RawSampleWithLabels(
                'temperature:3:11',
                36, // average between 30 and 42
                new DateTimeImmutable('2019-11-06 20:34:00.000'),
                [new Label('sensor_id', '3'), new Label('area_id', '11')]
            ),
            new RawSampleWithLabels(
                'temperature:3:12',
                41, //average beween 34 and 48
                new DateTimeImmutable('2019-11-06 20:34:00.000'),
                [new Label('sensor_id', '3'), new Label('area_id', '12')]
            ),
        ];

        self::assertEquals($expectedRange, $range);
    }

    public function testAddAndRetrieveAsMultiRangeWithMultipleFilters(): void
    {
        $from = new DateTimeImmutable('2019-11-06 20:34:17.000');
        $to = new DateTimeImmutable('2019-11-06 20:34:17.100');

        $this->sut->create(
            'temperature:3:11',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );
        $this->sut->add(new RawSample('temperature:3:11', 30, $from));
        $this->sut->add(new RawSample('temperature:3:11', 42, $to));

        $filter = new Filter('sensor_id', '2');
        $filter->add('area_id', Filter::OP_EQUALS, '32');

        $range = $this->sut->multiRange($filter);

        $expectedRange = [
            new RawSample('temperature:3:11', 30, new DateTimeImmutable('2019-11-06 20:34:17.000')),
            new RawSample('temperature:3:11', 42, new DateTimeImmutable('2019-11-06 20:34:17.100'))
        ];

        self::assertEquals($expectedRange, $range);
    }

    public function testAddAndRetrieveAsLastSamplesWithMultipleFilters(): void
    {
        $from = new DateTimeImmutable('2019-11-06 20:34:17.000');
        $to = new DateTimeImmutable('2019-11-06 20:34:18.000');

        $this->sut->create(
            'temperature:3:11',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );
        $this->sut->add(new RawSample('temperature:3:11', 30, $from));
        $this->sut->add(new RawSample('temperature:3:11', 42, $to));

        $this->sut->create(
            'temperature:3:12',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );
        $this->sut->add(new RawSample('temperature:3:12', 30, $from));
        $this->sut->add(new RawSample('temperature:3:12', 42, $to));

        $filter = new Filter('sensor_id', '2');
        $filter->add('area_id', Filter::OP_EQUALS, '32');

        $range = $this->sut->getLastSamples($filter);

        $expectedResult = [
            new RawSample('temperature:3:11', 42, new DateTimeImmutable('2019-11-06 20:34:18.000')),
            new RawSample('temperature:3:12', 42, new DateTimeImmutable('2019-11-06 20:34:18.000'))
        ];

        self::assertEquals($expectedResult, $range);
    }

    public function testAddAndRetrieveKeysWithMultipleFilters(): void
    {
        $from = new DateTimeImmutable('2019-11-06 20:34:17.000');
        $to = new DateTimeImmutable('2019-11-06 20:34:17.100');

        $this->sut->create(
            'temperature:3:11',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );
        $this->sut->add(new RawSample('temperature:3:11', 30, $from));
        $this->sut->add(new RawSample('temperature:3:11', 42, $to));

        $this->sut->create(
            'temperature:3:12',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );
        $this->sut->add(new RawSample('temperature:3:12', 30, $from));
        $this->sut->add(new RawSample('temperature:3:12', 42, $to));

        $filter = new Filter('sensor_id', '2');
        $filter->add('area_id', Filter::OP_EQUALS, '32');

        $range = $this->sut->getKeysByFilter($filter);

        $expectedResult = ['temperature:3:11', 'temperature:3:12'];

        self::assertEquals($expectedResult, $range);
    }

    public function testAddAndRetrieveWithDateTimeObjectAsMultiRangeWithMultipleFilters(): void
    {
        $currentDate = new DateTime();
        $from = (clone $currentDate)->sub(new DateInterval('P1D'));
        $to = $currentDate;

        $this->sut->create(
            'temperature:3:11',
            6000,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );
        $this->sut->add(new RawSample('temperature:3:11', 30, $from));
        $this->sut->add(new RawSample('temperature:3:11', 42, $to));

        $filter = new Filter('sensor_id', '2');
        $filter->add('area_id', Filter::OP_EQUALS, '32');

        $range = $this->sut->multiRange($filter);

        $expectedRange = [
            RawSample::createFromTimestamp('temperature:3:11', (float)42, TimeStampToDateTime::timestampWithMsFromDateTime(new DateTimeImmutable($to->format('Y-m-d H:i:s.u'))))
        ];

        self::assertEquals($expectedRange, $range);
    }

    public function testAddAndRetrieveWithDateTimeObjectAsRange(): void
    {
        $from = new DateTimeImmutable('2019-11-06 20:34:17.103000');
        $to = new DateTimeImmutable('2019-11-06 20:34:17.107000');

        $this->sut->create(
            'temperature:3:11',
            null,
            [new Label('sensor_id', '2'), new Label('area_id', '32')]
        );

        $this->sut->add(new RawSample('temperature:3:11', 30, $from));
        $this->sut->add(new RawSample('temperature:3:11', 42, $to));

        $range = $this->sut->range(
            'temperature:3:11'
        );

        $expectedRange = [
            RawSample::createFromTimestamp(
                'temperature:3:11',
                (float)30,
                TimeStampToDateTime::timestampWithMsFromDateTime(new DateTimeImmutable($from->format('Y-m-d H:i:s.u')))
            ),
            RawSample::createFromTimestamp(
                'temperature:3:11',
                (float)42,
                TimeStampToDateTime::timestampWithMsFromDateTime(new DateTimeImmutable($to->format('Y-m-d H:i:s.u')))
            ),
        ];

        self::assertEquals($expectedRange, $range);
    }
}
