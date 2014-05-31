<?php
namespace Titon\Db\Pgsql\Type;

use Titon\Test\Stub\DriverStub;
use Titon\Test\TestCase;
use \DateTime;
use \DateTimeZone;
use \PDO;

/**
 * @property \Titon\Db\Pgsql\Type\DatetimeTzType $object
 */
class DatetimeTypeTest extends TestCase {

    protected function setUp() {
        parent::setUp();

        $this->object = new DatetimeTzType(new DriverStub([]));
    }

    public function testFrom() {
        // Method returns as is
        $this->assertSame('1988-02-26 00:02:05+0400', $this->object->from('1988-02-26 00:02:05+0400'));
        $this->assertSame('2011-03-11 21:05:29+1200', $this->object->from('2011-03-11 21:05:29+1200'));
        $this->assertSame('1985-06-06 12:33:00-0600', $this->object->from('1985-06-06 12:33:00-0600'));
        $this->assertSame('1995-11-30 02:44:55-0800', $this->object->from('1995-11-30 02:44:55-0800'));
    }

    public function testTo() {
        $this->assertSame('1988-02-26 00:02:05+0000', $this->object->to(mktime(0, 2, 5, 2, 26, 1988))); // Can't pass timezone
        $this->assertSame('2011-03-11 21:05:29+0000', $this->object->to('2011-03-11 21:05:29')); // Can't pass timezone
        $this->assertSame('1985-06-06 12:33:00+0000', $this->object->to('June 6th 1985, 12:33pm')); // Can't pass timezone
        $this->assertSame('1995-11-30 02:44:55-0800', $this->object->to(new DateTime('1995-11-30 02:44:55', new DateTimeZone('America/Los_Angeles'))));
        $this->assertSame('1988-02-26 01:32:45-0800', $this->object->to([
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
        $this->assertEquals('datetimetz', $this->object->getName());
    }

    public function testGetBindingType() {
        $this->assertEquals(PDO::PARAM_STR, $this->object->getBindingType());
    }

    public function testGetDefaultOptions() {
        $this->assertEquals(['null' => true, 'default' => null], $this->object->getDefaultOptions());
    }

}