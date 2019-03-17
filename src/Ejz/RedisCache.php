<?php

namespace Ejz;

use Ejz\RedisClient;
use Psr\SimpleCache\CacheInterface;

class RedisCache implements CacheInterface
{
    /**
     * PREFIXES
     */
    private const PREFIX_SHARD     = 's_';
    private const PREFIX_TAG       = 't_';
    private const PREFIX_KEY_VALUE = 'kv_';
    private const PREFIX_KEY_TAGS  = 'kt_';

    /**
     * Number of shards.
     */
    private const SHARDS = 1000;

    /** @var RedisClient */
    private $client;

    /** @var string */
    private $prefix;

    /**
     * @param RedisClient $client
     * @param string      $prefix (optional)
     */
    public function __construct(RedisClient $client, string $prefix = '')
    {
        $this->client = $client;
        $this->prefix = $prefix;
    }

    /**
     * @param string $key
     * @param mixed  $default (optional)
     *
     * @return mixed
     *
     * @throws RedisCacheException
     */
    public function get($key, $default = null)
    {
        return $this->getMultiple([$key], $default)[$key];
    }

    /**
     * @param iterable $keys
     * @param mixed    $default (optional)
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function getMultiple($keys, $default = null): array
    {
        if (!is_iterable($keys)) {
            throw new RedisCacheException('Argument $keys is not iterable.');
        }
        if (!count($keys)) {
            return [];
        }
        $keys = array_unique($keys);
        $this->validateKeys($keys);
        $args = array_map(function ($key) {
            return self::PREFIX_KEY_VALUE . $key;
        }, $keys);
        $values = $this->client->MGET(...$args);
        $values = array_map(function ($value) use ($default) {
            return $value === null ? $default : $this->unpack($value);
        }, $values);
        return array_combine($keys, $values);
    }

    /**
     * @param string                $key
     * @param mixed                 $value
     * @param DateInterval|int|null $ttl   (optional)
     * @param array                 $tags  (optional)
     *
     * @return bool
     *
     * @throws RedisCacheException
     */
    public function set($key, $value, $ttl = null, array $tags = []): bool
    {
        return $this->setMultiple([$key => $value], $ttl, $tags);
    }

    /**
     * @param iterable              $values
     * @param DateInterval|int|null $ttl    (optional)
     * @param array                 $tags   (optional)
     *
     * @return bool
     *
     * @throws RedisCacheException
     */
    public function setMultiple($values, $ttl = null, array $tags = []): bool
    {
        if (!is_iterable($values)) {
            throw new RedisCacheException('Argument $values is not iterable.');
        }
        if (!count($values)) {
            return true;
        }
        $this->validateTags($tags);
        $keys = array_keys($values);
        $this->deleteMultiple($keys);
        if ($ttl instanceof \DateInterval) {
            $expire = (new \DateTime('now'))->add($ttl)->getTimeStamp() - time();
        } elseif (is_int($ttl) || ctype_digit($ttl)) {
            $expire = $ttl;
        } else {
            $expire = 0;
        }
        if ($expire < 0) {
            return false;
        }
        $args = [];
        foreach ($values as $key => $value) {
            $args[] = self::PREFIX_KEY_VALUE . $key;
            $args[] = $this->pack($value);
        }
        $this->client->MSET(...$args);
        if ($expire) {
            $this->client->EVAL(
                self::SCRIPT_SET_EXPIRE,
                count($keys),
                ...$keys,
                ...[$expire, self::PREFIX_KEY_VALUE]
            );
        }
        $tags = array_unique(array_values($tags));
        if (count($tags)) {
            $this->client->EVAL(
                self::SCRIPT_SET_TAGS,
                count($keys),
                ...$keys,
                ...$tags,
                ...[self::PREFIX_KEY_TAGS, self::PREFIX_TAG]
            );
        }
        $args = [[], []];
        foreach ($keys as $key) {
            $args[0][] = $key;
            $args[1][] = crc32($key) % self::SHARDS;
        }
        $this->client->EVAL(
            self::SCRIPT_SET_SHARDS,
            count($args[0]),
            ...$args[0],
            ...$args[1],
            ...[self::PREFIX_SHARD]
        );
        return true;
    }

    /**
     * @param string $key
     *
     * @return bool
     *
     * @throws RedisCacheException
     */
    public function delete($key): bool
    {
        return $this->deleteMultiple([$key]);
    }

    /**
     * @param iterable $keys
     *
     * @return bool
     *
     * @throws RedisCacheException
     */
    public function deleteMultiple($keys): bool
    {
        if (!is_iterable($keys)) {
            throw new RedisCacheException('Argument $keys is not iterable.');
        }
        if (!count($keys)) {
            return true;
        }
        $keys = array_unique($keys);
        $this->validateKeys($keys);
        $this->client->EVAL(
            self::SCRIPT_DELETE,
            count($keys),
            ...$keys,
            ...[self::PREFIX_TAG, self::PREFIX_KEY_TAGS]
        );
        $this->client->DEL(
            ...array_map(function ($key) {
                return self::PREFIX_KEY_VALUE . $key;
            }, $keys),
            ...array_map(function ($key) {
                return self::PREFIX_KEY_TAGS . $key;
            }, $keys)
        );
        return true;
    }

    /**
     * @param string $key
     *
     * @return bool
     *
     * @throws RedisCacheException
     */
    public function has($key): bool
    {
        return $this->hasMultiple([$key])[$key];
    }

    /**
     * @param itetable $keys
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function hasMultiple($keys): array
    {
        if (!is_iterable($keys)) {
            throw new RedisCacheException('Argument $keys is not iterable.');
        }
        if (!count($keys)) {
            return [];
        }
        $keys = array_unique($keys);
        $this->validateKeys($keys);
        $result = $this->client->EVAL(
            self::SCRIPT_HAS,
            count($keys),
            ...$keys,
            ...[self::PREFIX_KEY_VALUE]
        );
        return array_combine($keys, array_map('boolval', $result));
    }

    /**
     * @param string ...$tags
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function search(...$tags): array
    {
        $this->validateTags($tags);
        if (!count($tags)) {
            return [];
        }
        $tags = array_unique($tags);
        $tags = array_map(function ($tag) {
            return self::PREFIX_TAG . $tag;
        }, $tags);
        $return = $this->client->EVAL(
            self::SCRIPT_SEARCH,
            count($tags),
            ...$tags,
            ...[
                self::PREFIX_TAG,
                self::PREFIX_KEY_VALUE,
                self::PREFIX_KEY_TAGS,
            ]
        );
        return $return ?: [];
    }

    /**
     * @param string ...$tags
     *
     * @return bool
     *
     * @throws RedisCacheException
     */
    public function drop(...$tags): bool
    {
        $keys = $this->search(...$tags);
        return $this->deleteMultiple($keys);
    }

    /**
     * @param string $key
     *
     * @return array|null
     *
     * @throws RedisCacheException
     */
    public function getTags($key): ?array
    {
        return $this->getTagsMultiple([$key])[$key];
    }

    /**
     * @param array $keys
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function getTagsMultiple(array $keys): array
    {
        if (!is_iterable($keys)) {
            throw new RedisCacheException('Argument $keys is not iterable.');
        }
        if (!count($keys)) {
            return [];
        }
        $keys = array_unique($keys);
        $this->validateKeys($keys);
        $return = $this->client->EVAL(
            self::SCRIPT_GET_TAGS,
            count($keys),
            ...$keys,
            ...[
                self::PREFIX_KEY_VALUE,
                self::PREFIX_KEY_TAGS,
            ]
        );
        return array_map(function ($value) {
            return $value === 0 ? null : $value;
        }, array_combine($keys, $return));
    }

    /**
     * @param string $key
     *
     * @return int|null
     *
     * @throws RedisCacheException
     */
    public function getTtl($key): ?int
    {
        return $this->getTtlMultiple([$key])[$key];
    }

    /**
     * @param array $keys
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function getTtlMultiple(array $keys): array
    {
        if (!is_iterable($keys)) {
            throw new RedisCacheException('Argument $keys is not iterable.');
        }
        if (!count($keys)) {
            return [];
        }
        $keys = array_unique($keys);
        $this->validateKeys($keys);
        $return = $this->client->EVAL(
            self::SCRIPT_GET_TTL,
            count($keys),
            ...$keys,
            ...[
                self::PREFIX_KEY_VALUE,
            ]
        );
        return array_map(function ($value) {
            $value = (int)$value;
            if ($value === -1) {
                return 0;
            }
            if ($value === -2 || $value === 0) {
                return null;
            }
            return $value;
        }, array_combine($keys, $return));
    }

    /**
     * @return \Generator
     */
    public function all(): \Generator
    {
        foreach (range(0, self::SHARDS - 1) as $shard) {
            $keys = $this->client->EVAL(
                self::SCRIPT_ALL,
                0,
                ...[
                    self::PREFIX_SHARD . $shard,
                    self::PREFIX_KEY_VALUE,
                ]
            );
            yield from $keys;
        }
    }

    /**
     * @return bool
     */
    public function clear(): bool
    {
        foreach ($this->all() as $key) {
            $this->delete($key);
        }
        return true;
    }

    /**
     * @param mixed $key
     *
     * @throws RedisCacheException
     */
    private function validateKey($key)
    {
        if (!is_string($key) || $key === '') {
            throw new RedisCacheException('Argument $key must be a valid non-empty string.');
        }
        if (strpbrk($key, '{}()/\@:') !== false) {
            throw new RedisCacheException(sprintf('Key "%s" contains unsupported characters.', $key));
        }
    }

    /**
     * @param iterable $keys
     *
     * @throws RedisCacheException
     */
    private function validateKeys(iterable $keys)
    {
        array_walk($keys, [$this, 'validateKey']);
    }

    /**
     * @param mixed $tag
     *
     * @throws RedisCacheException
     */
    private function validateTag($tag)
    {
        if (!is_string($tag) || $tag === '') {
            throw new RedisCacheException('Argument $tag must be a valid non-empty string.');
        }
        if (strpbrk($tag, '{}()/\@:') !== false) {
            throw new RedisCacheException(sprintf('Tag "%s" contains unsupported characters.', $tag));
        }
    }

    /**
     * @param iterable $tags
     *
     * @throws RedisCacheException
     */
    private function validateTags(iterable $tags)
    {
        array_walk($tags, [$this, 'validateTag']);
    }

    /**
     * @param mixed $value
     *
     * @return string
     */
    private function pack($value): string
    {
        if (is_string($value)) {
            return 'r' . $value;
        }
        return 's' . serialize($value);
    }

    /**
     * @param string $value
     *
     * @return mixed
     */
    private function unpack(string $value)
    {
        $c = $value[0];
        $value = substr($value, 1);
        if ($c === 'r') {
            return $value;
        }
        return unserialize($value);
    }

    /**
     * SCRIPTS
     */
    private const SCRIPT_SET_EXPIRE = '
        local expire = ARGV[1]
        local prefix_key_value = ARGV[2]
        for _, key in ipairs(KEYS) do
            redis.call("EXPIRE", prefix_key_value .. key, expire)
        end
    ';
    private const SCRIPT_SET_TAGS = '
        local prefix_tag = table.remove(ARGV)
        local prefix_key_tags = table.remove(ARGV)
        for _, key in ipairs(KEYS) do
            redis.call("SADD", prefix_key_tags .. key, unpack(ARGV))
        end
        for _, tag in ipairs(ARGV) do
            redis.call("SADD", prefix_tag .. tag, unpack(KEYS))
        end
    ';
    private const SCRIPT_SET_SHARDS = '
        local tag_all = table.remove(ARGV)
        for i, key in ipairs(KEYS) do
            redis.call("SADD", tag_all .. ARGV[i], key)
        end
    ';
    private const SCRIPT_DELETE = '
        local prefix_tag = ARGV[1]
        local prefix_key_tags = ARGV[2]
        for _, key in ipairs(KEYS) do
            local tags = redis.call("SMEMBERS", prefix_key_tags .. key)
            for _, tag in ipairs(tags) do
                redis.call("SREM", prefix_tag .. tag, key)
            end
        end
    ';
    private const SCRIPT_HAS = '
        local ret = {}
        local prefix_key_value = ARGV[1]
        for _, key in ipairs(KEYS) do
            table.insert(ret, redis.call("EXISTS", prefix_key_value .. key))
        end
        return ret
    ';
    private const SCRIPT_SEARCH = '
        local ret = {}
        local tags = {}
        local prefix_tag = ARGV[1]
        local prefix_key_value = ARGV[2]
        local prefix_key_tags = ARGV[3]
        local keys = redis.call("SINTER", unpack(KEYS))
        for _, key in ipairs(keys) do
            if redis.call("EXISTS", prefix_key_value .. key) == 1 then
                table.insert(ret, key)
            else
                tags = redis.call("SMEMBERS", prefix_key_tags .. key)
                for _, tag in ipairs(tags) do
                    redis.call("SREM", prefix_tag .. tag, key)
                end
                redis.call("DEL", prefix_key_tags .. key)
            end
        end
        return ret
    ';
    private const SCRIPT_ALL = '
        local ret = {}
        local shard = ARGV[1]
        local prefix_key_value = ARGV[2]
        local keys = redis.call("SMEMBERS", shard)
        for _, key in ipairs(keys) do
            if redis.call("EXISTS", prefix_key_value .. key) == 1 then
                table.insert(ret, key)
            else
                redis.call("SREM", shard, key)
            end
        end
        return ret
    ';
    private const SCRIPT_GET_TAGS = '
        local ret = {}
        local prefix_key_value = ARGV[1]
        local prefix_key_tags = ARGV[2]
        for _, key in ipairs(KEYS) do
            if redis.call("EXISTS", prefix_key_value .. key) == 1 then
                table.insert(ret, redis.call("SMEMBERS", prefix_key_tags .. key))
            else
                table.insert(ret, 0)
            end
        end
        return ret
    ';
    private const SCRIPT_GET_TTL = '
        local ret = {}
        local prefix_key_value = ARGV[1]
        for _, key in ipairs(KEYS) do
            table.insert(ret, redis.call("TTL", prefix_key_value .. key))
        end
        return ret
    ';
}
