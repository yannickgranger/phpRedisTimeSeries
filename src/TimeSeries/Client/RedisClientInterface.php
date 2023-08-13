<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries\Client;

interface RedisClientInterface
{
    public function set(string $key, array|string $value, int|float|null $expiration);
    public function get(string $key);
    public function executeCommand(array $params);

    public function unlink(string $key);
}
