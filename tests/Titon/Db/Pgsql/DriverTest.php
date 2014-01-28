<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql;

use PDO;
use Titon\Common\Config;
use Titon\Db\Query;
use Titon\Test\Stub\Repository\Stat;
use Titon\Test\Stub\Repository\User;

/**
 * Test class for driver specific testing.
 */
class DriverTest extends \Titon\Db\Driver\PdoDriverTest {

    /**
     * This method is called before a test is executed.
     */
    protected function setUp() {
        $this->object = new PgsqlDriver('default', Config::get('db'));
        $this->object->connect();

        $this->table = new User();
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

    /**
     * Test that query params are resolved for binds.
     * Should be in correct order.
     */
    public function testResolveParams() {
        $query1 = new Query(Query::SELECT, $this->table);
        $query1->where('id', 1)->where(function() {
            $this->like('name', 'Titon')->in('size', [1, 2, 3]);
        });

        $this->assertEquals([
            [1, PDO::PARAM_STR],
            ['Titon', PDO::PARAM_STR],
            [1, PDO::PARAM_INT],
            [2, PDO::PARAM_INT],
            [3, PDO::PARAM_INT],
        ], $this->object->resolveParams($query1));

        // Include fields
        $query2 = new Query(Query::UPDATE, $this->table);
        $query2->fields([
            'username' => 'miles',
            'age' => 26
        ])->where('id', 666);

        $this->assertEquals([
            ['miles', PDO::PARAM_STR],
            [26, PDO::PARAM_INT],
            [666, PDO::PARAM_STR],
        ], $this->object->resolveParams($query2));

        // All at once!
        $query3 = new Query(Query::UPDATE, $this->table);
        $query3->fields([
            'username' => 'miles',
            'age' => 26
        ])->orWhere(function() {
            $this
                ->in('id', [4, 5, 6])
                ->also(function() {
                    $this->eq('status', true)->notEq('email', 'email@domain.com');
                })
                ->between('age', 30, 50);
        });

        $this->assertEquals([
            ['miles', PDO::PARAM_STR],
            [26, PDO::PARAM_INT],
            [4, PDO::PARAM_STR],
            [5, PDO::PARAM_STR],
            [6, PDO::PARAM_STR],
            [true, PDO::PARAM_BOOL],
            ['email@domain.com', PDO::PARAM_STR],
            [30, PDO::PARAM_INT],
            [50, PDO::PARAM_INT],
        ], $this->object->resolveParams($query3));
    }

}