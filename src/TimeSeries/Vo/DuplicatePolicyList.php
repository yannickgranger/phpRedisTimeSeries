<?php

declare(strict_types=1);

namespace Palicao\PhpRedisTimeSeries\TimeSeries\Vo;

enum DuplicatePolicyList: string
{
    case BLOCK = 'BLOCK';
    case FIRST = 'FIRST';
    case LAST = 'LAST';
    case MIN = 'MIN';
    case MAX = 'MAX';
    case SUM = 'SUM';
}
