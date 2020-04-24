<?php

namespace Ejz;

class RedisCache
{
    /**
     * PREFIXES
     */
    private const PREFIX_TAG       = 't_';
    private const PREFIX_KEY_VALUE = 'kv_';
    private const PREFIX_KEY_TAGS  = 'kt_';
    private const PREFIX_KEY_LINKS = 'kl_';

    /** @var RedisClient */
    private $client;

    /** @var array */
    private $prefixes;

    /**
     * @param RedisClient $client
     * @param string      $prefix (optional)
     */
    public function __construct(RedisClient $client, string $prefix = '')
    {
        $this->client = $client;
        $this->prefixes = [
            'tag' => $prefix . self::PREFIX_TAG,
            'key_value' => $prefix . self::PREFIX_KEY_VALUE,
            'key_tags' => $prefix . self::PREFIX_KEY_TAGS,
            'key_links' => $prefix . self::PREFIX_KEY_LINKS,
        ];
    }

    /**
     * @return RedisClient
     */
    public function getClient(): RedisClient
    {
        return $this->client;
    }

    /**
     * @param string $key
     * @param mixed  $default (optional)
     *
     * @return mixed
     *
     * @throws RedisCacheException
     */
    public function get(string $key, $default = null)
    {
        return $this->getMultiple([$key], $default)[$key];
    }

    /**
     * @param array $keys
     * @param mixed $default (optional)
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function getMultiple(array $keys, $default = null): array
    {
        $result = $this->multiple($keys, self::SCRIPT_GET_MULTIPLE, true);
        return array_map(function ($value) use ($default) {
            return $value === null ? $default : $this->unpack($value);
        }, $result);
    }

    /**
     * @param string $key
     *
     * @throws RedisCacheException
     */
    public function delete(string $key)
    {
        $this->deleteMultiple([$key]);
    }

    /**
     * @param array $keys
     *
     * @throws RedisCacheException
     */
    public function deleteMultiple(array $keys)
    {
        $this->multiple($keys, self::SCRIPT_DELETE_MULTIPLE, false);
    }

    /**
     * @param string $key
     *
     * @return ?array
     *
     * @throws RedisCacheException
     */
    public function tags(string $key): ?array
    {
        return $this->tagsMultiple([$key])[$key];
    }

    /**
     * @param array $keys
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function tagsMultiple(array $keys): array
    {
        $result = $this->multiple($keys, self::SCRIPT_TAGS_MULTIPLE, true);
        $result = array_map(function ($value) {
            return is_array($value) ? $value : null;
        }, $result);
        return $result;
    }

    /**
     * @param string $key
     *
     * @return ?array
     *
     * @throws RedisCacheException
     */
    public function links(string $key): ?array
    {
        return $this->linksMultiple([$key])[$key];
    }

    /**
     * @param array $keys
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function linksMultiple(array $keys): array
    {
        $result = $this->multiple($keys, self::SCRIPT_LINKS_MULTIPLE, true);
        $result = array_map(function ($value) {
            return is_array($value) ? $value : null;
        }, $result);
        return $result;
    }

    /**
     * @param array  $keys
     * @param string $script
     * @param bool   $combine
     *
     * @return ?array
     *
     * @throws RedisCacheException
     */
    private function multiple(array $keys, string $script, bool $combine): ?array
    {
        if (!count($keys)) {
            return [];
        }
        $keys = array_unique(array_values($keys));
        $this->validateKeys($keys);
        $result = $this->client->EVAL(
            $script,
            count($keys),
            ...$keys,
            ...array_values($this->prefixes)
        );
        return $combine ? array_combine($keys, $result) : null;
    }

    /**
     * @param string $key
     * @param mixed  $value
     * @param int    $ttl   (optional)
     * @param array  $tags  (optional)
     * @param array  $links (optional)
     *
     * @throws RedisCacheException
     */
    public function set(string $key, $value, int $ttl = 0, array $tags = [], array $links = [])
    {
        $this->setMultiple([$key => $value], $ttl, $tags, $links);
    }

    /**
     * @param array $values
     * @param int   $ttl    (optional)
     * @param array $tags   (optional)
     * @param array $links  (optional)
     *
     * @throws RedisCacheException
     */
    public function setMultiple(array $values, int $ttl = 0, array $tags = [], array $links = [])
    {
        $keys = array_keys($values);
        $this->setMultipleComplex(
            $values,
            array_fill_keys($keys, $ttl),
            array_fill_keys($keys, $tags),
            array_fill_keys($keys, $links)
        );
    }

    /**
     * @param array $values
     * @param array $ttls
     * @param array $tags
     * @param array $links
     *
     * @throws RedisCacheException
     */
    public function setMultipleComplex(array $values, array $ttls, array $tags, array $links)
    {
        $c = count($values);
        if (!$c) {
            return;
        }
        $tags = array_map(function ($tags) {
            return array_unique(array_values($tags));
        }, $tags);
        array_walk($tags, [$this, 'validateTags']);
        $links = array_map(function ($links) {
            return array_unique(array_values($links));
        }, $links);
        array_walk($links, [$this, 'validateKeys']);
        $keys = array_keys($values);
        $this->validateKeys($keys);
        $args = [];
        foreach ($keys as $key) {
            $t = $tags[$key];
            $l = $links[$key];
            array_push($args, count($t), ...$t);
            array_push($args, count($l), ...$l);
        }
        $this->client->EVAL(
            self::SCRIPT_SET_MULTIPLE_COMPLEX,
            $c,
            ...$keys,
            ...array_values($this->prefixes),
            ...array_values(array_map([$this, 'pack'], $values)),
            ...array_values($ttls),
            ...$args
        );
    }

    /**
     * @param array ...$tags
     *
     * @return array
     *
     * @throws RedisCacheException
     */
    public function search(...$tags): array
    {
        if (!count($tags)) {
            return [];
        }
        $this->validateTags($tags);
        $tags = array_unique($tags);
        $args = array_values($this->prefixes);
        array_push($args, __FUNCTION__);
        return $this->client->EVAL(
            self::SCRIPT_SEARCH_DROP,
            count($tags),
            ...$tags,
            ...$args
        );
    }

    /**
     * @param array ...$tags
     *
     * @throws RedisCacheException
     */
    public function drop(...$tags)
    {
        if (!count($tags)) {
            return [];
        }
        $this->validateTags($tags);
        $tags = array_unique($tags);
        $args = array_values($this->prefixes);
        array_push($args, __FUNCTION__);
        $this->client->EVAL(
            self::SCRIPT_SEARCH_DROP,
            count($tags),
            ...$tags,
            ...$args
        );
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
     * @param array $keys
     *
     * @throws RedisCacheException
     */
    private function validateKeys(array $keys)
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
     * @param array $tags
     *
     * @throws RedisCacheException
     */
    private function validateTags(array $tags)
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
     * SCRIPT_GET_MULTIPLE
     */
    private const SCRIPT_GET_MULTIPLE = '
        local nkeys = #KEYS
        local prefix_key_value = ARGV[2]
        local ret = {}
        local iret = 1
        for i = 1, nkeys do
            ret[iret] = redis.call("GET", prefix_key_value .. KEYS[i])
            if ret[iret] and ret[iret]:sub(1, 1) == "l" then
                ret[iret] = redis.call("GET", prefix_key_value .. ret[iret]:sub(2))
            end
            iret = iret + 1
        end
        return ret
    ';

    /**
     * SCRIPT_SET_MULTIPLE_COMPLEX
     */
    private const SCRIPT_SET_MULTIPLE_COMPLEX = '
        local nkeys = #KEYS
        local prefix_tag = ARGV[1]
        local prefix_key_value = ARGV[2]
        local prefix_key_tags = ARGV[3]
        local prefix_key_links = ARGV[4]
        local pointer = nkeys + nkeys + 5
        for i = 1, nkeys do
            local key = KEYS[i]
            local kv = prefix_key_value .. key
            local kt = prefix_key_tags .. key
            local kl = prefix_key_links .. key
            local v = ARGV[i + 4]
            local t = tonumber(ARGV[i + nkeys + 4])
            local args = {"SET", kv, v}
            if t > 0 then
                args[4] = "EX"
                args[5] = t
            end
            redis.call(unpack(args))
            --
            local n
            local links = redis.call("SMEMBERS", kl)
            local nlinks = #links
            for j = 1, nlinks do
                redis.call("DEL", prefix_key_value .. links[j])
            end
            redis.call("DEL", kl)
            local tags = redis.call("SMEMBERS", kt)
            local ntags = #tags
            redis.call("DEL", kt)
            for j = 1, ntags do
                redis.call("SREM", prefix_tag .. tags[j], key)
            end
            tags = {}
            local itags = 1
            n = tonumber(ARGV[pointer])
            for j = 1, n do
                tags[itags] = ARGV[pointer + j]
                itags = itags + 1
            end
            pointer = pointer + n + 1
            links = {}
            local ilinks = 1
            n = tonumber(ARGV[pointer])
            for j = 1, n do
                links[ilinks] = ARGV[pointer + j]
                ilinks = ilinks + 1
            end
            pointer = pointer + n + 1
            if itags > 1 then
                redis.call("SADD", kt, unpack(tags))
                for j = 1, itags - 1 do
                    redis.call("SADD", prefix_tag .. tags[j], key)
                end
            end
            if ilinks > 1 then
                redis.call("SADD", kl, unpack(links))
                for j = 1, ilinks - 1 do
                    local old = redis.call("GETRANGE", prefix_key_value .. links[j], 1, -1)
                    if old ~= "" and old ~= key then
                        redis.call("SREM", prefix_key_links .. old, links[j])
                    end
                    redis.call("SET", prefix_key_value .. links[j], "l" .. key)
                end
            end
        end
    ';

    /**
     * SCRIPT_SEARCH
     */
    private const SCRIPT_SEARCH_DROP = '
        local nkeys = #KEYS
        local prefix_tag = ARGV[1]
        local prefix_key_value = ARGV[2]
        local prefix_key_tags = ARGV[3]
        local is_search = ARGV[5] == "search"
        for i = 1, nkeys do
            KEYS[i] = prefix_tag .. KEYS[i]
        end
        local keys = redis.call("SINTER", unpack(KEYS))
        nkeys = #keys
        local ret = {}
        local iret = 1
        for i = 1, nkeys do
            local key = keys[i]
            if is_search and redis.call("EXISTS", prefix_key_value .. key) == 1 then
                ret[iret] = key
                iret = iret + 1
            else
                local tags = redis.call("SMEMBERS", prefix_key_tags .. key)
                redis.call("DEL", prefix_key_tags .. key)
                local ntags = #tags
                for j = 1, ntags do
                    redis.call("SREM", prefix_tag .. tags[j], key)
                end
                if not is_search then
                    redis.call("DEL", prefix_key_value .. key)
                end
            end
        end
        return ret
    ';

    /**
     * SCRIPT_DELETE_MULTIPLE
     */
    private const SCRIPT_DELETE_MULTIPLE = '
        local nkeys = #KEYS
        local prefix_key_value = ARGV[2]
        for i = 1, nkeys do
            redis.call("DEL", prefix_key_value .. KEYS[i])
        end
    ';

    /**
     * SCRIPT_TAGS_MULTIPLE
     */
    private const SCRIPT_TAGS_MULTIPLE = '
        local nkeys = #KEYS
        local prefix_key_value = ARGV[2]
        local prefix_key_tags = ARGV[3]
        local ret = {}
        local iret = 1
        for i = 1, nkeys do
            local kv = prefix_key_value .. KEYS[i]
            local kt = prefix_key_tags .. KEYS[i]
            if redis.call("EXISTS", kv) == 1 then
                ret[iret] = redis.call("SMEMBERS", kt)
                iret = iret + 1
            else
                ret[iret] = 0
                iret = iret + 1
            end
        end
        return ret
    ';

    /**
     * SCRIPT_LINKS_MULTIPLE
     */
    private const SCRIPT_LINKS_MULTIPLE = '
        local nkeys = #KEYS
        local prefix_key_value = ARGV[2]
        local prefix_key_links = ARGV[4]
        local ret = {}
        local iret = 1
        for i = 1, nkeys do
            local kv = prefix_key_value .. KEYS[i]
            local kl = prefix_key_links .. KEYS[i]
            if redis.call("EXISTS", kv) == 1 then
                ret[iret] = redis.call("SMEMBERS", kl)
                iret = iret + 1
            else
                ret[iret] = 0
                iret = iret + 1
            end
        end
        return ret
    ';
}
