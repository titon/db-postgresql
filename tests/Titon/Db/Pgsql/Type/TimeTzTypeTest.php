<?php
namespace Titon\Db\Pgsql\Type;

use Titon\Test\Stub\DriverStub;
use Titon\Test\TestCase;
use \DateTime;
use \DateTimeZone;
use \PDO;

/**
 * @property \Titon\Db\Pgsql\Type\TimeTzType $object
 */
class TimeTypeTest extends TestCase {

    protected function setUp() {
        parent::setUp();

        $this->object = new TimeTzType(new DriverStub([]));
    }

    public function testFrom() {
        // Method returns as is
        $this->assertSame('00:02:05+0400', $this->object->from('00:02:05+0400'));
        $this->assertSame('21:05:29+1200', $this->object->from('21:05:29+1200'));
        $this->assertSame('12:33:00-0600', $this->object->from('12:33:00-0600'));
        $this->assertSame('02:44:55-0800', $this->object->from('02:44:55-0800'));
    }

    public function testTo() {
        $this->assertSame('00:02:05+0000', $this->object->to(mktime(0, 2, 5, 2, 26, 1988)));
        $this->assertSame('21:05:29+0000', $this->object->to('2011-03-11 21:05:29'));
        $this->assertSame('12:33:00+0000', $this->object->to('June 6th 1985, 12:33pm'));
        $this->assertSame('02:44:55-0800', $this->object->to(new DateTime('1995-11-30 02:44:55', new DateTimeZone('America/Los_Angeles'))));
        $this->assertSame('09:32:45-0800', $this->object->to([
            'hour' => 9,
            'minute' => 32,
            'second' => 45,
            'month' => 2,
            'day' => 26,
            'year' => 1988,
            'timezone' => 'America/Los_Angeles'
        ]));
        $this->assertSame('21:32:45-0800', $this->object->to([
            'meridiem' => 'pm',
            'hour' => 9,
            'minute' => 32,
            'second' => 45,
            'month' => 2,
            'day' => 26,
            'year' => 1988,
            'timezone' => 'America/Los_Angeles'
        ]));
    }

    public function testGetName() {
        $this->assertEquals('timetz', $this->object->getName());
    }

    public function testGetBindingType() {
        $this->assertEquals(PDO::PARAM_STR, $this->object->getBindingType());
    }

    public function testGetDefaultOptions() {
        $this->assertEquals(['null' => true, 'default' => null], $this->object->getDefaultOptions());
    }

}