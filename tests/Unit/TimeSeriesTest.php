<?php
/* @noinspection PhpUnhandledExceptionInspection */
/* @noinspection PhpDocSignatureInspection */
declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\Tests\Unit;

use DateTimeImmutable;
use Palicao\PhpRedisTimeSeries\TimeSeries\Client\RedisClient;
use Palicao\PhpRedisTimeSeries\TimeSeries\RawSample;
use Palicao\PhpRedisTimeSeries\TimeSeries\TimeSeries;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\AggregationRule;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Filter;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Label;
use Palicao\PhpRedisTimeSeries\TimeSeries\Vo\Metadata;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class TimeSeriesTest extends TestCase
{
    /**
     * @var TimeSeries
     */
    private $sut;

    /**
     * @var RedisClient|MockObject
     */
    private $redisClientMock;

    public function setUp(): void
    {
        $this->redisClientMock = $this->createMock(RedisClient::class);
        $this->sut = new TimeSeries($this->redisClientMock);
    }

    /**
     * @dataProvider createDataProvider
     */
    public function testCreate(array $params, array $expectedParams): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with($expectedParams);
        $this->sut->create(...$params);
    }

    public function createDataProvider(): array
    {
        return [
            'full' => [
                [
                    'a',
                    10,
                    [new Label('l1', 'v1'), new Label('l2', 'v2')],
                    true,
                    10000,
                    TimeSeries::DUPLICATE_POLICY_SUM
                ],
                [
                    'TS.CREATE', 
                    'a',
                    'RETENTION',
                    10, 
                    'UNCOMPRESSED', 
                    'CHUNK_SIZE', 
                    '10000', 
                    'DUPLICATE_POLICY', 
                    'SUM',
                    'LABELS',
                    'l1',
                    'v1', 
                    'l2',
                    'v2'
                ],
            ],
            'most common' => [
                [
                    'a',
                    10, 
                    [new Label('l1', 'v1'), new Label('l2', 'v2')]
                ],
                ['TS.CREATE', 'a', 'RETENTION', 10, 'LABELS', 'l1', 'v1', 'l2', 'v2']
            ],
            'no labels' => [['a', 10], ['TS.CREATE', 'a', 'RETENTION', 10]],
            'minimal' => [['a'], ['TS.CREATE', 'a']]
        ];
    }

    public function testAlter(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.ALTER', 'a', 'RETENTION', 10, 'LABELS', 'l1', 'v1', 'l2', 'v2']);
        $this->sut->alter(
            'a',
            10,
            [new Label('l1', 'v1'), new Label('l2', 'v2')]
        );
    }

    /**
     * @dataProvider addDataProvider
     */
    public function testAdd(array $params, array $expectedParams): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with($expectedParams)
            ->willReturn(1483300866234);
        $addedSample = $this->sut->add(...$params);
        $expectedSample = new RawSample('a', 10.1, new DateTimeImmutable('2017-01-01T20.01.06.234'));
        self::assertEquals($expectedSample, $addedSample);
    }

    public function addDataProvider(): array
    {
        return [
            'full' => [
                [
                    new RawSample('a', 10.1, new DateTimeImmutable('2017-01-01T20.01.06.234')),
                    10,
                    [new Label('l1', 'v1'), new Label('l2', 'v2')]
                ],
                ['TS.ADD', 'a', 1483300866234, 10.1, 'RETENTION', 10, 'LABELS', 'l1', 'v1', 'l2', 'v2']
            ],
            'no datetime' => [
                [
                    new RawSample('a', 10.1),
                    10,
                    [new Label('l1', 'v1'), new Label('l2', 'v2')]
                ],
                ['TS.ADD', 'a', '*', 10.1, 'RETENTION', 10, 'LABELS', 'l1', 'v1', 'l2', 'v2']
            ]
        ];
    }

    public function testAddMany(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.MADD', 'a', '*', 10.1, 'b', 1483300866234, 1.0])
            ->willReturn([1483300866233, 1483300866234]);
        $addedSamples = $this->sut->addMany([
            new RawSample('a', 10.1),
            new RawSample('b', 1, new DateTimeImmutable('2017-01-01T20.01.06.234'))
        ]);
        $expectedSamples = [
            new RawSample('a', 10.1, new DateTimeImmutable('2017-01-01T20.01.06.233')),
            new RawSample('b', 1.0, new DateTimeImmutable('2017-01-01T20.01.06.234'))
        ];
        self::assertEquals($expectedSamples, $addedSamples);
    }

    public function testAddManyEmpty(): void
    {
        $this->redisClientMock
            ->expects(self::never())
            ->method('executeCommand')
            ->willReturn([]);
        $addedSamples = $this->sut->addMany([]);
        self::assertEquals([], $addedSamples);
    }

    public function testIncrementBy(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.INCRBY', 'a', 10.1, 'RESET', 10, 'RETENTION', 20, 'LABELS', 'l1', 'v1', 'l2', 'v2']);
        $this->sut->incrementBy(
            new RawSample('a', 10.1),
            10,
            20,
            [new Label('l1', 'v1'), new Label('l2', 'v2')]
        );
    }

    public function testDecrementBy(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.DECRBY', 'a', 10.1, 'RESET', 10, 'TIMESTAMP', 1483300866234, 'RETENTION', 20, 'LABELS', 'l1', 'v1', 'l2', 'v2'])
            ->willReturn(1483300866234);
        $this->sut->decrementBy(
            new RawSample('a', 10.1, new DateTimeImmutable('2017-01-01T20.01.06.234')),
            10,
            20,
            [new Label('l1', 'v1'), new Label('l2', 'v2')]
        );
    }

    public function testCreateRule(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.CREATERULE', 'a', 'b', 'AGGREGATION', 'AVG', 100])
            ->willReturn(1483300866234);
        $this->sut->createRule('a', 'b', new AggregationRule(AggregationRule::AGG_AVG, 100));
    }

    public function testDeleteRule(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.DELETERULE', 'a', 'b'])
            ->willReturn(1483300866234);
        $this->sut->deleteRule('a', 'b');
    }

    /**
     * @dataProvider rangeDataProvider
     */
    public function testRange(array $params, array $expectedRedisParam): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with($expectedRedisParam)
            ->willReturn([[1483300866234, '9.1'], [1522923630234, '9.2']]);
        $returnedSamples = $this->sut->range(...$params);
        $expectedSamples = [
            new RawSample('a', 9.1, new DateTimeImmutable('2017-01-01T20.01.06.234')),
            new RawSample('a', 9.2, new DateTimeImmutable('2018-04-05T10.20.30.234'))
        ];

        self::assertEquals($expectedSamples, $returnedSamples);
    }

    public function rangeDataProvider(): array
    {
        return [
            'full data' => [[
                'a',
                new DateTimeImmutable('2017-01-01T20.01.06.234'),
                new DateTimeImmutable('2018-04-05T10.20.30.234'),
                100,
                new AggregationRule(AggregationRule::AGG_LAST, 200)
            ], [
                'TS.RANGE', 'a', 1483300866234, 1522923630234, 'COUNT', 100, 'AGGREGATION', 'LAST', 200
            ]],
            'missing from' => [[
                'a',
                null,
                new DateTimeImmutable('2018-04-05T10.20.30.234'),
                100,
                new AggregationRule(AggregationRule::AGG_LAST, 200)
            ], [
                'TS.RANGE', 'a', '-', 1522923630234, 'COUNT', 100, 'AGGREGATION', 'LAST', 200
            ]],
            'missing from and to' => [[
                'a',
                null,
                null,
                100,
                new AggregationRule(AggregationRule::AGG_LAST, 200)
            ], [
                'TS.RANGE', 'a', '-', '+', 'COUNT', 100, 'AGGREGATION', 'LAST', 200
            ]],
            'missing from, to and count' => [[
                'a',
                null,
                null,
                null,
                new AggregationRule(AggregationRule::AGG_LAST, 200)
            ], [
                'TS.RANGE', 'a', '-', '+', 'AGGREGATION', 'LAST', 200
            ]],
            'minimal' => [['a'], ['TS.RANGE', 'a', '-', '+']]
        ];
    }

    public function testInfo(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.INFO', 'a'])
            ->willReturn([
                'lastTimestamp',
                1522923630234,
                'retentionTime',
                100,
                'chunkCount',
                10,
                'maxSamplesPerChunk',
                360,
                'labels',
                [['a', 'a1'], ['b', 'b1']],
                'sourceKey',
                null,
                'rules',
                [['aa', 10, 'AVG']]
            ]);
        $returned = $this->sut->info('a');
        $expected = new Metadata(
            new DateTimeImmutable('2018-04-05T10.20.30.234'),
            100,
            10,
            360,
            [new Label('a', 'a1'), new Label('b', 'b1')],
            null,
            ['aa' => new AggregationRule(AggregationRule::AGG_AVG, 10)]
        );

        self::assertEquals($expected, $returned);
    }
    
    public function testGetLastSample(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.GET', 'a'])
            ->willReturn([1483300866234, '7']);
        $response = $this->sut->getLastSample('a');
        $expected = new RawSample('a', 7.0, new DateTimeImmutable('2017-01-01T20.01.06.234'));
        self::assertEquals($expected, $response);
    }

    public function testGetLastSamples(): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.MGET', 'FILTER', 'a=a1'])
            ->willReturn([
                ['a', [['a', 'a1'], ['b', 'b1']], 1483300866234, '7'],
                ['b', [['a', 'a1'], ['c', 'c1']], 1522923630234, '7.1'],
            ]);
        $response = $this->sut->getLastSamples(new Filter('a', 'a1'));
        $expected = [
            new RawSample('a', 7.0, new DateTimeImmutable('2017-01-01T20.01.06.234')),
            new RawSample('b', 7.1, new DateTimeImmutable('2018-04-05T10.20.30.234')),
        ];
        self::assertEquals($expected, $response);
    }

    /**
     * @dataProvider multiRangeDataProvider
     */
    public function testMultiRange(array $params, array $expectedRedisParams): void
    {
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with($expectedRedisParams)
            ->willReturn([
                ['a', [['a', 'a1']], [[1483300866234, '7']]],
                ['b', [['a', 'a1']], [[1522923630234, '7.1']]],
            ]);
        $response = $this->sut->multiRange(...$params);
        $expected = [
            new RawSample('a', 7.0, new DateTimeImmutable('2017-01-01T20.01.06.234')),
            new RawSample('b', 7.1, new DateTimeImmutable('2018-04-05T10.20.30.234')),
        ];
        self::assertEquals($expected, $response);
    }

    public function multiRangeDataProvider(): array
    {
        return [
            'full data' => [
                [
                    new Filter('a', 'a1'),
                    new DateTimeImmutable('2017-01-01T20.01.06.234'),
                    new DateTimeImmutable('2018-04-05T10.20.30.234'),
                    100,
                    new AggregationRule(AggregationRule::AGG_LAST, 200)
                ],
                ['TS.MRANGE', 1483300866234, 1522923630234, 'COUNT', 100, 'AGGREGATION', 'LAST', 200, 'FILTER', 'a=a1']
            ],
            'missing dates' => [
                [
                    new Filter('a', 'a1'),
                    null,
                    null,
                    100,
                    new AggregationRule(AggregationRule::AGG_LAST, 200)
                ],
                ['TS.MRANGE', '-', '+', 'COUNT', 100, 'AGGREGATION', 'LAST', 200, 'FILTER', 'a=a1']
            ],
            'missing dates and count' => [
                [
                    new Filter('a', 'a1'),
                    null,
                    null,
                    null,
                    new AggregationRule(AggregationRule::AGG_LAST, 200)
                ],
                ['TS.MRANGE', '-', '+', 'AGGREGATION', 'LAST', 200, 'FILTER', 'a=a1']
            ],
            'minimal' => [
                [new Filter('a', 'a1')],
                ['TS.MRANGE', '-', '+', 'FILTER', 'a=a1']
            ]
        ];
    }

    public function testGetKeysByFilter(): void
    {
        $keys = ['a', 'b'];
        $this->redisClientMock
            ->expects(self::once())
            ->method('executeCommand')
            ->with(['TS.QUERYINDEX', 'a=a1'])
            ->willReturn($keys);
        $response = $this->sut->getKeysByFilter(new Filter('a', 'a1'));
        self::assertEquals($keys, $response);
    }
}
