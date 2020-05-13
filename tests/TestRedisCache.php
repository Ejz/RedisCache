<?php

use Ejz\RedisCache;
use Ejz\RedisClient;
use Ejz\RedisCacheException;
use PHPUnit\Framework\TestCase;

class TestRedisCache extends TestCase
{
    /** @var RedisCache */
    private $cache;

    /**
     * @return void
     */
    public function setUp(): void
    {
        parent::setUp();
        $client = new RedisClient();
        $client->FLUSHDB();
        $this->cache = new RedisCache($client);
    }

    /**
     * @test
     */
    public function test_redis_cache_get_set()
    {
        $c = $this->cache;
        //
        $this->assertTrue($c->get('foo') === null);
        //
        $this->assertTrue($c->get('foo', 'a') === 'a');
        //
        $c->set('foo', 'bar');
        $this->assertTrue($c->get('foo') === 'bar');
        //
        $c->set('foo', ['bar']);
        $this->assertTrue($c->get('foo') === ['bar']);
        //
        $c->set('foo', 'bar', 1);
        $this->assertTrue($c->get('foo') === 'bar');
        sleep(2);
        $this->assertTrue($c->get('foo') === null);
    }

    /**
     * @test
     */
    public function test_redis_cache_delete()
    {
        $c = $this->cache;
        //
        $this->assertTrue($c->get('foo') === null);
        //
        $c->set('foo', 'bar');
        $this->assertTrue($c->get('foo') === 'bar');
        //
        $c->delete('foo');
        $this->assertTrue($c->get('foo') === null);
    }

    /**
     * @test
     */
    public function test_redis_cache_tags()
    {
        $c = $this->cache;
        //
        $this->assertTrue($c->tags('foo') === null);
        //
        $c->set('foo', 'bar');
        $this->assertTrue($c->tags('foo') === []);
        //
        $c->set('foo', 'bar', 1);
        $this->assertTrue($c->tags('foo') === []);
        sleep(2);
        $this->assertTrue($c->tags('foo') === null);
        //
        $c->set('foo', 'bar', 0, ['t1']);
        $this->assertTrue($c->tags('foo') === ['t1']);
        //
        $c->setMultiple(['k1' => 'v1', 'k2' => 'v2'], 0, ['t1']);
        $this->assertTrue($c->tags('k1') === ['t1']);
        $this->assertTrue($c->tags('k2') === ['t1']);
        //
        $c->setMultipleComplex([
            'k3' => 'v1',
            'k4' => 'v2'
        ], [
            'k3' => 0,
            'k4' => 1,
        ], [
            'k3' => ['t1'],
            'k4' => ['t2', 't3'],
        ], [
            'k3' => [],
            'k4' => [],
        ]);
        $this->assertTrue($c->tags('k3') === ['t1']);
        $this->assertTrue($c->tags('k4') === ['t2', 't3']);
        sleep(2);
        $this->assertTrue($c->tags('k3') === ['t1']);
        $this->assertTrue($c->tags('k4') === null);
        //
        $c->set('k5', 'v1', 0, ['t1']);
        $c->set('k5', 'v2', 1, ['t2']);
        $this->assertTrue($c->get('k5') === 'v2');
        $this->assertTrue($c->tags('k5') === ['t2']);
        sleep(2);
        $this->assertTrue($c->get('k5') === null);
        $this->assertTrue($c->tags('k5') === null);
    }

    /**
     * @test
     */
    public function test_redis_cache_links_1()
    {
        $c = $this->cache;
        //
        $this->assertTrue($c->links('foo') === null);
        //
        $c->set('foo', 'bar');
        $this->assertTrue($c->links('foo') === []);
        //
        $c->set('foo', 'bar', 1);
        $this->assertTrue($c->tags('foo') === []);
        sleep(2);
        $this->assertTrue($c->tags('foo') === null);
        //
        $c->set('foo', 'bar', 0, [], ['l1']);
        $this->assertTrue($c->links('foo') === ['l1']);
        //
        $c->setMultiple(['k1' => 'v1', 'k2' => 'v2'], 0, [], ['l1']);
        $this->assertTrue($c->links('k1') === []);
        $this->assertTrue($c->links('k2') === ['l1']);
        $this->assertTrue($c->get('l1') === 'v2');
        //
        $c->setMultipleComplex([
            'k3' => 'v1',
            'k4' => 'v2'
        ], [
            'k3' => 0,
            'k4' => 1,
        ], [
            'k3' => [],
            'k4' => [],
        ], [
            'k3' => ['l1'],
            'k4' => ['l2', 'l3'],
        ]);
        $this->assertTrue($c->links('k3') === ['l1']);
        $this->assertTrue($c->links('k4') === ['l2', 'l3']);
        sleep(2);
        $this->assertTrue($c->links('k3') === ['l1']);
        $this->assertTrue($c->links('k4') === null);
        //
        $c->set('k5', 'v1', 0, [], ['l1']);
        $c->set('k5', 'v2', 1, [], ['l2']);
        $this->assertTrue($c->get('k5') === 'v2');
        $this->assertTrue($c->links('k5') === ['l2']);
        sleep(2);
        $this->assertTrue($c->get('k5') === null);
        $this->assertTrue($c->links('k5') === null);
    }

    /**
     * @test
     */
    public function test_redis_cache_links_2()
    {
        $c = $this->cache;
        //
        $c->set('k1', 'bar', 0, [], ['l1']);
        $this->assertTrue($c->get('l1') === 'bar');
        //
        $c->set('k2', 'bar', 1, [], ['l2']);
        $this->assertTrue($c->get('l2') === 'bar');
        sleep(2);
        $this->assertTrue($c->get('l2') === null);
        //
        $c->set('k3', 'bar', 0, [], ['l3']);
        $this->assertTrue($c->get('l3') === 'bar');
        $c->set('k3', 'bar', 0, [], []);
        $this->assertTrue($c->get('l3') === null);
        //
        $c->set('k4', 'bar', 0, [], ['l41', 'l42']);
        $this->assertTrue($c->get('l41') === 'bar');
        $this->assertTrue($c->get('l42') === 'bar');
        $c->set('k4', 'bar', 0, [], ['l41']);
        $this->assertTrue($c->get('l41') === 'bar');
        $this->assertTrue($c->get('l42') === null);
        //
        $c->set('k51', 'bar1', 0, [], ['l5']);
        $c->set('k52', 'bar2', 0, [], ['l5']);
        $this->assertTrue($c->get('l5') === 'bar2');
        $c->set('k51', 'bar1', 0, [], []);
        $this->assertTrue($c->get('l5') === 'bar2');
        //
        $c->set('k6', 'bar', 0, [], ['l61', 'l62']);
        $c->set('k6', 'bar', 0, [], ['l61']);
        $c->set('k6', 'bar', 0, [], ['l61', 'l62']);
        $c->set('k6', 'bar', 0, [], ['l61']);
        $this->assertTrue($c->get('l61') === 'bar');
        $this->assertTrue($c->get('l62') === null);
    }

    /**
     * @test
     */
    public function test_redis_cache_search()
    {
        $c = $this->cache;
        //
        $c->set('k1', 'v1', 0, ['t1']);
        $this->assertTrue($c->search('t1') === ['k1']);
        //
        $c->set('k2', 'v2', 1, ['t2']);
        $this->assertTrue($c->search('t2') === ['k2']);
        sleep(2);
        $this->assertTrue($c->search('t2') === []);
        //
        $c->set('k3', 'v3', 0, ['t3']);
        $this->assertTrue($c->search('t3') === ['k3']);
        $c->set('k3', 'v33', 0);
        $this->assertTrue($c->get('k3') === 'v33');
        $this->assertTrue($c->search('t3') === []);
        //
        $c->set('a1', '', 0, ['z1']);
        $c->set('a2', '', 0, ['z1', 'z2', 'z3']);
        $c->set('a3', '', 0, ['z2']);
        $this->assertEquals($c->search('z1'), ['a1', 'a2']);
        $this->assertEquals($c->search('z2'), ['a2', 'a3']);
        $this->assertEquals($c->search('z3'), ['a2']);
    }

    /**
     * @test
     */
    public function test_redis_cache_drop()
    {
        $c = $this->cache;
        //
        $c->set('k1', 'foo', 0, ['t1', 't2']);
        $c->set('k2', 'foo', 0, ['t1', 't3']);
        $this->assertTrue($c->search('t1') === ['k1', 'k2']);
        $this->assertTrue($c->search('t2') === ['k1']);
        $this->assertTrue($c->search('t1', 't2') === ['k1']);
        $c->drop('t1', 't2');
        $this->assertTrue($c->search('t1') === ['k2']);
        $this->assertTrue($c->search('t2') === []);
        $this->assertTrue($c->search('t1', 't2') === []);
    }
}
