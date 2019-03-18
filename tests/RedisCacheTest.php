<?php

use Ejz\RedisCache;
use Ejz\RedisClient;
use Ejz\RedisCacheException;
use PHPUnit\Framework\TestCase;

class RedisCacheTest extends TestCase
{
    /** @var RedisCache */
    private $cache;

    /**
     *
     */
    public function setUp()
    {
        parent::setUp();
        $this->cache = new RedisCache(new RedisClient());
    }

    /**
     * @test
     */
    public function test_redis_cache_get_set()
    {
        $c = $this->cache;
        // get
        $key = md5(microtime(true));
        $this->assertTrue($c->get($key) === null);
        // set-get
        $key = md5(microtime(true));
        $c->set($key, $value = 'foo');
        $this->assertTrue($c->get($key) === $value);
        // set-get-set-get
        $key = md5(microtime(true));
        $c->set($key, $value = 'foo');
        $this->assertTrue($c->get($key) === $value);
        $c->set($key, $value = 'bar');
        $this->assertTrue($c->get($key) === $value);
        // set-get-expire-get
        $key = md5(microtime(true));
        $c->set($key, $value = 'foo', 2);
        $this->assertTrue($c->get($key) === $value);
        sleep(3);
        $this->assertTrue($c->get($key) === null);
        // set-get-delete-get
        $key = md5(microtime(true));
        $c->set($key, $value = 'foo');
        $this->assertTrue($c->get($key) === $value);
        $c->delete($key);
        $this->assertTrue($c->get($key) === null);
    }

    /**
     * @test
     */
    public function test_redis_cache_search()
    {
        $c = $this->cache;
        // set-search
        $key = md5(microtime(true));
        $tag = md5($key);
        $c->set($key, $value = 'foo', 0, [$tag]);
        $this->assertTrue($c->search($tag) === [$key]);
        // set-search-expire-search
        $key = md5(microtime(true));
        $tag = md5($key);
        $c->set($key, $value = 'foo', 2, [$tag]);
        $this->assertTrue($c->search($tag) === [$key]);
        sleep(3);
        $this->assertTrue($c->search($tag) === []);
        // set-search-set-search
        $key = md5(microtime(true));
        $tag = md5($key);
        $c->set($key, $value = 'foo', 0, [$tag]);
        $this->assertTrue($c->search($tag) === [$key]);
        $c->set($key, $value = 'bar', 0);
        $this->assertTrue($c->search($tag) === []);
    }

    /**
     * @test
     */
    public function test_redis_cache_drop()
    {
        $c = $this->cache;
        // set-search
        $key = md5(microtime(true));
        $tag1 = md5($key . 1);
        $tag2 = md5($key . 2);
        $c->set($key, $value = 'foo', 0, [$tag1, $tag2]);
        $this->assertTrue($c->search($tag1) === [$key]);
        $this->assertTrue($c->search($tag2) === [$key]);
        $this->assertTrue($c->search($tag1, $tag2) === [$key]);
        // drop
        $c->drop($tag1, $tag2);
        $this->assertTrue($c->search($tag1) === []);
        $this->assertTrue($c->search($tag2) === []);
        $this->assertTrue($c->search($tag1, $tag2) === []);
    }

    /**
     * @test
     */
    public function test_redis_cache_has()
    {
        $c = $this->cache;
        // set-has
        $key = md5(microtime(true));
        $tag = md5($key);
        $c->set($key, $value = 'foo', 0, [$tag]);
        $this->assertTrue($c->has($key) === true);
        // set-expire-has
        $key = md5(microtime(true));
        $tag = md5($key);
        $c->set($key, $value = 'foo', 2, [$tag]);
        sleep(3);
        $this->assertTrue($c->has($key) === false);
    }

    /**
     * @test
     */
    public function test_redis_cache_clear()
    {
        $c = $this->cache;
        // set-get
        $key = md5(microtime(true));
        $c->set($key, $value = 'foo');
        $this->assertTrue($c->get($key) === $value);
        // set-clear-get
        $key = md5(microtime(true));
        $c->set($key, $value = 'foo');
        $c->clear();
        $this->assertTrue($c->has($key) === false);
        $this->assertTrue($c->get($key) === null);
    }

    /**
     * @test
     */
    public function test_redis_cache_ttl()
    {
        $c = $this->cache;
        // set-get
        $key1 = md5(microtime(true));
        $c->set($key1, $value = 'foo', 3);
        $key2 = md5(microtime(true));
        $c->set($key2, $value = 'bar', 0);
        $key3 = md5(microtime(true));
        $ttls = $c->getTtlMultiple([$key1, $key2, $key3]);
        $this->assertTrue(
            $ttls[$key1] === 3
                &&
            $ttls[$key2] === 0
                &&
            $ttls[$key3] === null
        );
        $this->assertTrue(
            $ttls[$key1] === $c->getTtl($key1)
                &&
            $ttls[$key2] === $c->getTtl($key2)
                &&
            $ttls[$key3] === $c->getTtl($key3)
        );
        sleep(1);
        $ttls = $c->getTtlMultiple([$key1, $key2, $key3]);
        $this->assertTrue(
            $ttls[$key1] < 3 && $ttls[$key1] >= 1
                &&
            $ttls[$key2] === 0
                &&
            $ttls[$key3] === null
        );
        sleep(3);
        $ttls = $c->getTtlMultiple([$key1, $key2, $key3]);
        $this->assertTrue(
            $ttls[$key1] === null
                &&
            $ttls[$key2] === 0
                &&
            $ttls[$key3] === null
        );
    }

    /**
     * @test
     */
    public function test_redis_cache_exception()
    {
        $c = $this->cache;
        $this->expectException(RedisCacheException::class);
        $c->get('/');
    }

    /**
     * @test
     */
    public function test_redis_cache_tags()
    {
        $c = $this->cache;
        //
        $key1 = md5(microtime(true));
        $c->set($key1, $value = 'foo', 2, $tags1 = ['tag1', 'tag2']);
        $key2 = md5(microtime(true));
        $c->set($key2, $value = 'bar', 0, $tags2 = ['tag3', 'tag4']);
        $key3 = md5(microtime(true));
        $c->set($key3, $value = 'moo', 0, []);
        $key4 = md5(microtime(true));
        $tags = $c->getTagsMultiple([$key1, $key2, $key3, $key4]);
        $this->assertTrue(
            $tags[$key1] === $tags1
                &&
            $tags[$key2] === $tags2
                &&
            $tags[$key3] === []
                &&
            $tags[$key4] === null
        );
        sleep(3);
        $tags = $c->getTagsMultiple([$key1, $key2, $key3, $key4]);
        $this->assertTrue(
            $tags[$key1] === null
                &&
            $tags[$key2] === $tags2
                &&
            $tags[$key3] === []
                &&
            $tags[$key4] === null
        );
        $this->assertTrue(
            $tags[$key1] === $c->getTags($key1)
                &&
            $tags[$key2] === $c->getTags($key2)
                &&
            $tags[$key3] === $c->getTags($key3)
                &&
            $tags[$key4] === $c->getTags($key4)
        );
    }
}
