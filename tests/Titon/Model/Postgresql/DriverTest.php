<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Common\Config;
use Titon\Model\Postgresql\PostgresqlDriver;
use Titon\Test\Stub\Model\User;

/**
 * Test class for driver specific testing.
 */
class DriverTest extends \Titon\Model\Driver\PdoDriverTest {

	/**
	 * This method is called before a test is executed.
	 */
	protected function setUp() {
		$this->object = new PostgresqlDriver('default', Config::get('db'));
		$this->object->connect();

		$this->model = new User();
	}

	/**
	 * Test DSN building.
	 */
	public function testGetDsn() {
		$this->assertEquals('pgsql:dbname=titon_test;host=127.0.0.1;port=5432', $this->object->getDsn());

		$this->object->config->dsn = 'custom:dsn';
		$this->assertEquals('custom:dsn', $this->object->getDsn());
	}

}