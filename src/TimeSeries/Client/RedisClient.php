<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries\Client;

use Palicao\PhpRedisTimeSeries\TimeSeries\Client\Exception\RedisAuthenticationException;
use Palicao\PhpRedisTimeSeries\TimeSeries\Client\Exception\RedisClientException;
use Redis;
use RedisException;

final class RedisClient implements RedisClientInterface
{
    private Redis $redis;
    private RedisConnectionParams $connectionParams;

    public function __construct(Redis $redis, RedisConnectionParams $connectionParams)
    {
        $this->redis = $redis;
        $this->connectionParams = $connectionParams;
    }

    /**
     * @throws RedisClientException|RedisException
     */
    private function connectIfNeeded(): void
    {
        if ($this->redis->isConnected()) {
            return;
        }

        $params = $this->connectionParams;

        $result = $this->redis->pconnect(
            host: $params->getHost(),
            port: $params->getPort(),
            timeout: $params->getTimeout(),
            persistent_id: $params->isPersistentConnection() ? gethostname() : null,
            retry_interval: $params->getRetryInterval(),
            read_timeout: $params->getReadTimeout()
        );

        if ($result === false) {
            throw new RedisClientException(sprintf(
                'Unable to connect to redis server %s:%s: %s',
                $params->getHost(),
                $params->getPort(),
                $this->redis->getLastError() ?? 'unknown error'
            ));
        }

        $this->authenticate($params->getUsername(), $params->getPassword());
    }

    public function executeCommand(array $params)
    {
        $this->connectIfNeeded();
        // UNDOCUMENTED FEATURE: option 8 is REDIS_OPT_REPLY_LITERAL
        $value = (PHP_VERSION_ID < 70300) ? '1' : 1;
        $this->redis->setOption(8, $value);
        return $this->redis->rawCommand(...$params);
    }

    private function authenticate(?string $username, ?string $password): void
    {
        try {
            if ($password) {
                if ($username) {
                    // Calling auth() with an array throws a TypeError in some cases
                    $result = $this->redis->rawCommand('AUTH', $username, $password);
                } else {
                    $result = $this->redis->auth($password);
                }
                if ($result === false) {
                    throw new RedisAuthenticationException(sprintf(
                        'Failure authenticating user %s',
                        $username ?: 'default'
                    ));
                }
            }
        } catch (RedisException $e) {
            throw new RedisAuthenticationException(sprintf(
                'Failure authenticating user %s: %s',
                $username ?: 'default',
                $e->getMessage()
            ));
        }
    }

    public function set(string $key, array|string $value, ?int $expiration): bool|Redis
    {
        $options = [];
        if($expiration !== null) {
            $options['EX'] = $expiration;
        }
        return $this->redis->set(key: $key, value: $value, options: $options);
    }

    public function get(string $key): false|Redis|string
    {
        return $this->redis->get($key);
    }

    public function unlink(string $key)
    {
        if (false !== $this->redis->get($key)) {
            $this->redis->unlink($key);
        }
    }
}
