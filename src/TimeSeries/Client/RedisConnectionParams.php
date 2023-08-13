<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries\Client;

final class RedisConnectionParams
{
    private bool $persistentConnection;

    private string $host;

    private int $port;

    private int $timeout;

    private int $retryInterval;

    private float $readTimeout;

    private ?string $username;

    private ?string $password;


    public function __construct(
        string $host = '127.0.0.1',
        int $port = 6379,
        ?string $username = null,
        ?string $password = null
    ) {
        $this->persistentConnection = false;
        $this->host = $host;
        $this->port = $port;
        $this->username = $username;
        $this->password = $password;
        $this->timeout = 0;
        $this->retryInterval = 0;
        $this->readTimeout = 0.0;
    }

    /**
     * Whether to use a persistent connection
     */
    public function setPersistentConnection(bool $persistentConnection): RedisConnectionParams
    {
        $this->persistentConnection = $persistentConnection;
        return $this;
    }

    /**
     * Connection timeout (in seconds)
     */
    public function setTimeout(int $timeout): RedisConnectionParams
    {
        $this->timeout = $timeout;
        return $this;
    }

    /**
     * Retry interval (in seconds)
     */
    public function setRetryInterval(int $retryInterval): RedisConnectionParams
    {
        $this->retryInterval = $retryInterval;
        return $this;
    }

    /**
     * Read timeout in seconds
     */
    public function setReadTimeout(float $readTimeout): RedisConnectionParams
    {
        $this->readTimeout = $readTimeout;
        return $this;
    }

    public function isPersistentConnection(): bool
    {
        return $this->persistentConnection;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    public function getRetryInterval(): int
    {
        return $this->retryInterval;
    }

    public function getReadTimeout(): float
    {
        return $this->readTimeout;
    }

    public function getUsername(): ?string
    {
        return $this->username;
    }

    public function getPassword(): ?string
    {
        return $this->password;
    }
}
