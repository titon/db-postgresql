<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Sqlite;

use Titon\Common\Config;
use Titon\Common\Registry;
use Titon\Model\Pgsql\PgsqlDriver;
use Titon\Test\Stub\Model\Stat;
use Titon\Test\Stub\Model\User;

/**
 * Test class for driver specific testing.
 */
class DriverTest extends \Titon\Model\Driver\PdoDriverTest {

	/**
	 * This method is called before a test is executed.
	 */
	protected function setUp() {
		$this->object = new PgsqlDriver('default', Config::get('db'));
		$this->object->connect();

		$this->model = new User();
	}

	/**
	 * Test table inspecting.
	 */
	public function testDescribeTable() {
		$this->loadFixtures(['Users', 'Stats']);

		$user = new User();
		$this->assertEquals([
			'id' => [
				'field' => 'id',
				'type' => 'integer',
				'length' => '',
				'null' => false,
				'default' => "nextval('users_id_seq'::regclass)",
				'primary' => true,
				'ai' => true
			],
			'country_id' => [
				'field' => 'country_id',
				'type' => 'integer',
				'length' => '',
				'null' => true
			],
			'username' => [
				'field' => 'username',
				'type' => 'character varying',
				'length' => '255',
				'null' => false
			],
			'password' => [
				'field' => 'password',
				'type' => 'character varying',
				'length' => '255',
				'null' => true
			],
			'email' => [
				'field' => 'email',
				'type' => 'character varying',
				'length' => '255',
				'null' => true
			],
			'firstName' => [
				'field' => 'firstName',
				'type' => 'character varying',
				'length' => '255',
				'null' => true
			],
			'lastName' => [
				'field' => 'lastName',
				'type' => 'character varying',
				'length' => '255',
				'null' => true
			],
			'age' => [
				'field' => 'age',
				'type' => 'smallint',
				'length' => '',
				'null' => true
			],
			'created' => [
				'field' => 'created',
				'type' => 'timestamp without time zone',
				'length' => '',
				'null' => true,
				'default' => null
			],
			'modified' => [
				'field' => 'modified',
				'type' => 'timestamp without time zone',
				'length' => '',
				'null' => true,
				'default' => null
			],
		], $user->getDriver()->describeTable($user->getTable()));

		$stat = new Stat();
		$this->assertEquals([
			'id' => [
				'field' => 'id',
				'type' => 'integer',
				'length' => '',
				'null' => false,
				'default' => "nextval('stats_id_seq'::regclass)",
				'primary' => true,
				'ai' => true
			],
			'name' => [
				'field' => 'name',
				'type' => 'character varying',
				'length' => '255',
				'null' => true
			],
			'health' => [
				'field' => 'health',
				'type' => 'integer',
				'length' => '',
				'null' => true
			],
			'energy' => [
				'field' => 'energy',
				'type' => 'smallint',
				'length' => '',
				'null' => true
			],
			'damage' => [
				'field' => 'damage',
				'type' => 'real',
				'length' => '',
				'null' => true
			],
			'defense' => [
				'field' => 'defense',
				'type' => 'double precision',
				'length' => '',
				'null' => true
			],
			'range' => [
				'field' => 'range',
				'type' => 'numeric',
				'length' => '8,2',
				'null' => true
			],
			'isMelee' => [
				'field' => 'isMelee',
				'type' => 'boolean',
				'length' => '',
				'null' => true
			],
			'data' => [
				'field' => 'data',
				'type' => 'bytea',
				'length' => '',
				'null' => true
			],
		], $user->getDriver()->describeTable($stat->getTable()));
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