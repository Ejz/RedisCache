<?php

namespace Ejz;

class RedisCacheException
    extends \InvalidArgumentException
    implements \Psr\SimpleCache\InvalidArgumentException
{
}
