<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Model\Sqlite;

use Exception;
use Titon\Common\Config;
use Titon\Model\Driver\Dialect;
use Titon\Model\Driver\Schema;
use Titon\Model\Pgsql\PgsqlDialect;
use Titon\Model\Pgsql\PgsqlDriver;
use Titon\Model\Query;
use Titon\Test\Stub\Model\User;

/**
 * Test class for dialect SQL building.
 */
class DialectTest extends \Titon\Model\Driver\DialectTest {

    /**
     * This method is called before a test is executed.
     */
    protected function setUp() {
        $this->driver = new PgsqlDriver('default', Config::get('db'));
        $this->driver->connect();

        $this->object = $this->driver->getDialect();
    }

    /**
     * Test create index statement building.
     */
    public function testBuildCreateIndex() {
        $query = new Query(Query::CREATE_INDEX, new User());
        $query->fields('profile_id')->from('users')->asAlias('idx');

        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\)/', $this->object->buildCreateIndex($query));

        $query->fields(['profile_id' => 5]);
        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\(5\)\)/', $this->object->buildCreateIndex($query));

        $query->fields(['profile_id' => 'asc', 'other_id']);
        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")? ASC, (`|\")?other_id(`|\")?\)/', $this->object->buildCreateIndex($query));

        $query->fields(['profile_id' => ['length' => 5, 'order' => 'desc']]);
        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\(5\) DESC\)/', $this->object->buildCreateIndex($query));

        $query->fields('profile_id')->attribute([
            'type' => PgsqlDialect::UNIQUE,
            'concurrently' => true
        ]);
        $this->assertRegExp('/CREATE UNIQUE INDEX CONCURRENTLY (`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\)/', $this->object->buildCreateIndex($query));
    }

    /**
     * Test create table statement creation.
     */
    public function testBuildCreateTable() {
        $schema = new Schema('foobar');
        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true
        ]);

        $query = new Query(Query::CREATE_TABLE, new User());
        $query->schema($schema);

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL\n\);/', $this->object->buildCreateTable($query));

        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true,
            'primary' => true
        ]);

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\);/', $this->object->buildCreateTable($query));

        $schema->addColumn('column2', [
            'type' => 'int',
            'null' => true,
            'index' => true
        ]);

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\n(`|\")?column2(`|\")? integer NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\);/', $this->object->buildCreateTable($query));

        $schema->addOption('onCommit', PgsqlDialect::DELETE_ROWS);
        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\n(`|\")?column2(`|\")? integer NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\) ON COMMIT DELETE ROWS;/', $this->object->buildCreateTable($query));

        $schema->addOption('tablespace', 'foobar');
        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\n(`|\")?column2(`|\")? integer NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\) ON COMMIT DELETE ROWS TABLESPACE foobar;/', $this->object->buildCreateTable($query));

        $schema->addOption('with', PgsqlDialect::WITH_OIDS);
        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\n(`|\")?column2(`|\")? integer NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\) ON COMMIT DELETE ROWS TABLESPACE foobar WITH OIDS;/', $this->object->buildCreateTable($query));
    }

    /**
     * Test delete statement creation.
     */
    public function testBuildDelete() {
        $query = new Query(Query::DELETE, new User());

        $query->from('foobar');
        $this->assertRegExp('/DELETE FROM\s+(`|\")?foobar(`|\")?;/', $this->object->buildDelete($query));

        // pgsql doesn't support limit
        $query->limit(5);
        $this->assertRegExp('/DELETE FROM\s+(`|\")?foobar(`|\")?;/', $this->object->buildDelete($query));

        $query->where('id', [1, 2, 3]);
        $this->assertRegExp('/DELETE FROM\s+(`|\")?foobar(`|\")?\s+WHERE (`|\")?id(`|\")? IN \(\?, \?, \?\);/', $this->object->buildDelete($query));

        // or order by
        $query->orderBy('id', 'asc');
        $this->assertRegExp('/DELETE FROM\s+(`|\")?foobar(`|\")?\s+WHERE (`|\")?id(`|\")? IN \(\?, \?, \?\);/', $this->object->buildDelete($query));

        $query->attribute('only', true);
        $this->assertRegExp('/DELETE FROM ONLY (`|\")?foobar(`|\")?\s+WHERE (`|\")?id(`|\")? IN \(\?, \?, \?\);/', $this->object->buildDelete($query));
    }

    /**
     * Test delete statements that contain joins.
     */
    public function testBuildDeleteJoins() {
        $this->markTestSkipped('PGSQL does not support delete joins');
    }

    /**
     * Test drop table statement creation.
     */
    public function testBuildDropTable() {
        $query = new Query(Query::DROP_TABLE, new User());
        $query->from('foobar');

        $this->assertRegExp('/DROP TABLE IF EXISTS (`|\")?foobar(`|\")?;/', $this->object->buildDropTable($query));

        $query->attribute('action', PgsqlDialect::RESTRICT);
        $this->assertRegExp('/DROP TABLE IF EXISTS (`|\")?foobar(`|\")? RESTRICT;/', $this->object->buildDropTable($query));
    }

    /**
     * Test drop index statement building.
     */
    public function testBuildDropIndex() {
        $query = new Query(Query::DROP_INDEX, new User());
        $query->from('users')->asAlias('idx');

        $this->assertRegExp('/DROP INDEX\s+IF EXISTS (`|\")?idx(`|\")?/', $this->object->buildDropIndex($query));

        $query->attribute('concurrently', true);
        $this->assertRegExp('/DROP INDEX CONCURRENTLY IF EXISTS (`|\")?idx(`|\")?/', $this->object->buildDropIndex($query));
    }

    /**
     * Test select statements that contain joins.
     */
    public function testBuildSelectJoins() {
        $user = new User();
        $query = $user->select();
        $query->rightJoin($user->getRelation('Profile'), []);

        $this->assertRegExp('/SELECT\s+(`|\")?User(`|\")?.*, (`|\")?Profile(`|\")?.* FROM (`|\")?users(`|\")? AS (`|\")?User(`|\")? RIGHT JOIN (`|\")?profiles(`|\")? AS (`|\")?Profile(`|\")? ON (`|\")?User(`|\")?.(`|\")?id(`|\")? = (`|\")?Profile(`|\")?.(`|\")?user_id(`|\")?;/', $this->object->buildSelect($query));

        // With fields
        $query = $user->select('id', 'username');
        $query->rightJoin($user->getRelation('Profile'), ['id', 'avatar', 'lastLogin']);

        $this->assertRegExp('/SELECT\s+(`|\")?User(`|\")?.(`|\")?id(`|\")? AS User__id, (`|\")?User(`|\")?.(`|\")?username(`|\")? AS User__username, (`|\")?Profile(`|\")?.(`|\")?id(`|\")? AS Profile__id, (`|\")?Profile(`|\")?.(`|\")?avatar(`|\")? AS Profile__avatar, (`|\")?Profile(`|\")?.(`|\")?lastLogin(`|\")? AS Profile__lastLogin FROM (`|\")?users(`|\")? AS (`|\")?User(`|\")? RIGHT JOIN (`|\")?profiles(`|\")? AS (`|\")?Profile(`|\")? ON (`|\")?User(`|\")?.(`|\")?id(`|\")? = (`|\")?Profile(`|\")?.(`|\")?user_id(`|\")?;/', $this->object->buildSelect($query));

        // Three joins
        $query = $user->select('id');
        $query->leftJoin('foo', ['id'], ['User.id' => 'foo.id']);
        $query->outerJoin(['bar', 'Bar'], ['id'], ['User.bar_id' => 'Bar.id']);

        $this->assertRegExp('/SELECT\s+(`|\")?User(`|\")?.(`|\")?id(`|\")? AS User__id, (`|\")?foo(`|\")?.(`|\")?id(`|\")? AS foo__id, (`|\")?Bar(`|\")?.(`|\")?id(`|\")? AS Bar__id FROM (`|\")?users(`|\")? AS (`|\")?User(`|\")? LEFT JOIN (`|\")?foo(`|\")? ON (`|\")?User(`|\")?.(`|\")?id(`|\")? = (`|\")?foo(`|\")?.(`|\")?id(`|\")? FULL OUTER JOIN (`|\")?bar(`|\")? AS (`|\")?Bar(`|\")? ON (`|\")?User(`|\")?.(`|\")?bar_id(`|\")? = (`|\")?Bar(`|\")?.(`|\")?id(`|\")?;/', $this->object->buildSelect($query));
    }

    /**
     * Test truncate table statement creation.
     */
    public function testBuildTruncate() {
        $query = new Query(Query::TRUNCATE, new User());
        $query->from('foobar');

        $this->assertRegExp('/TRUNCATE\s+(`|\")?foobar(`|\")?;/', $this->object->buildTruncate($query));

        $query->attribute('identity', PgsqlDialect::RESTART_IDENTITY);
        $this->assertRegExp('/TRUNCATE\s+(`|\")?foobar(`|\")? RESTART IDENTITY;/', $this->object->buildTruncate($query));

        $query->attribute('only', true);
        $this->assertRegExp('/TRUNCATE ONLY (`|\")?foobar(`|\")? RESTART IDENTITY;/', $this->object->buildTruncate($query));

        $query->attribute('action', PgsqlDialect::CASCADE);
        $this->assertRegExp('/TRUNCATE ONLY (`|\")?foobar(`|\")? RESTART IDENTITY CASCADE;/', $this->object->buildTruncate($query));
    }

    /**
     * Test update statement creation.
     */
    public function testBuildUpdate() {
        $query = new Query(Query::UPDATE, new User());

        // No fields
        try {
            $this->object->buildUpdate($query);
            $this->assertTrue(false);
        } catch (Exception $e) {
            $this->assertTrue(true);
        }

        $query->fields(['username' => 'miles']);

        // No table
        try {
            $this->object->buildUpdate($query);
            $this->assertTrue(false);
        } catch (Exception $e) {
            $this->assertTrue(true);
        }

        $query->from('foobar');
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?username(`|\")? = \?;/', $this->object->buildUpdate($query));

        // pgsql doesn't support limit
        $query->limit(15);
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?username(`|\")? = \?;/', $this->object->buildUpdate($query));

        // or order by
        $query->orderBy('username', 'desc');
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?username(`|\")? = \?\;/', $this->object->buildUpdate($query));

        $query->fields([
            'email' => 'email@domain.com',
            'website' => 'http://titon.io'
        ]);
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?email(`|\")? = \?, (`|\")?website(`|\")? = \?;/', $this->object->buildUpdate($query));

        $query->where('status', 3);
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?email(`|\")? = \?, (`|\")?website(`|\")? = \?\s+WHERE (`|\")?status(`|\")? = \?;/', $this->object->buildUpdate($query));

        $query->attribute('only', true);
        $this->assertRegExp('/UPDATE ONLY (`|\")?foobar(`|\")?\s+SET (`|\")?email(`|\")? = \?, (`|\")?website(`|\")? = \?\s+WHERE (`|\")?status(`|\")? = \?;/', $this->object->buildUpdate($query));
    }

    /**
     * Test update statements that contain joins.
     */
    public function testBuildUpdateJoins() {
        $this->markTestSkipped('PGSQL does not support update joins');
    }

    /**
     * Test table column formatting builds according to the options defined.
     */
    public function testFormatColumns() {
        $schema = new Schema('foobar');
        $schema->addColumn('column', [
            'type' => 'int'
        ]);

        $this->assertRegExp('/(`|\")?column(`|\")? integer NULL/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'unsigned' => true,
            'zerofill' => true
        ]);

        $this->assertRegExp('/(`|\")?column(`|\")? integer NULL/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'null' => false,
            'comment' => 'Some comment here'
        ]);

        $this->assertRegExp('/(`|\")?column(`|\")? integer NOT NULL/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true,
            'length' => 11
        ]);

        $this->assertRegExp('/(`|\")?column(`|\")? integer\(11\) NOT NULL/', $this->object->formatColumns($schema));

        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true,
            'length' => 11,
            'unsigned' => true,
            'zerofill' => true,
            'null' => false,
            'default' => null,
            'comment' => 'Some comment here'
        ]);

        $expected = '(`|\")?column(`|\")? integer\(11\) NOT NULL DEFAULT NULL';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        $schema->addColumn('column2', [
            'type' => 'varchar',
            'length' => 255,
            'null' => true
        ]);

        $expected .= ',\n(`|\")?column2(`|\")? varchar\(255\) NULL';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        $schema->addColumn('column3', [
            'type' => 'smallint',
            'default' => 3,
            'null' => false
        ]);

        $expected .= ',\n(`|\")?column3(`|\")? smallint NOT NULL DEFAULT 3';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        // inherits values from type
        $schema->addColumn('column4', [
            'type' => 'timestamp'
        ]);

        $expected .= ',\n(`|\")?column4(`|\")? timestamp NULL DEFAULT NULL';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));

        $schema->addColumn('column5', [
            'type' => 'varchar',
            'collate' => 'en_US',
            'charset' => 'utf8'
        ]);

        $expected .= ',\n(`|\")?column5(`|\")? varchar\(255\) COLLATE en_US NULL';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));
    }

    /**
     * Test index keys.
     */
    public function testFormatTableIndex() {
        $this->markTestSkipped('PGSQL does not support CREATE TABLE statement indices');
    }

    /**
     * Test table keys are built with primary, unique, foreign and index.
     */
    public function testFormatTableKeys() {
        $schema = new Schema('foobar');
        $schema->addUnique('primary');

        $expected = ',\nUNIQUE \((`|\")?primary(`|\")?\)';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addUnique('unique', [
            'constraint' => 'uniqueSymbol'
        ]);

        $expected .= ',\nCONSTRAINT (`|\")?uniqueSymbol(`|\")? UNIQUE \((`|\")?unique(`|\")?\)';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addForeign('fk1', 'users.id');

        $expected .= ',\nFOREIGN KEY \((`|\")?fk1(`|\")?\) REFERENCES (`|\")?users(`|\")?\((`|\")?id(`|\")?\)';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addForeign('fk2', [
            'references' => 'posts.id',
            'onUpdate' => Dialect::SET_NULL,
            'onDelete' => Dialect::NO_ACTION
        ]);

        $expected .= ',\nFOREIGN KEY \((`|\")?fk2(`|\")?\) REFERENCES (`|\")?posts(`|\")?\((`|\")?id(`|\")?\) ON UPDATE SET NULL ON DELETE NO ACTION';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));

        $schema->addIndex('column1');
        $schema->addIndex('column2');

        // no indices

        $this->assertRegExp('/' . $expected . '/', $this->object->formatTableKeys($schema));
    }

    /**
     * Test unique keys.
     */
    public function testFormatTableUnique() {
        $data = ['columns' => ['foo'], 'constraint' => '', 'index' => 'idx'];

        $this->assertRegExp('/UNIQUE \((`|\")?foo(`|\")?\)/', $this->object->formatTableUnique($data));

        $data['constraint'] = 'symbol';
        $this->assertRegExp('/CONSTRAINT (`|\")?symbol(`|\")? UNIQUE \((`|\")?foo(`|\")?\)/', $this->object->formatTableUnique($data));

        $data['columns'][] = 'bar';
        $this->assertRegExp('/CONSTRAINT (`|\")?symbol(`|\")? UNIQUE \((`|\")?foo(`|\")?, (`|\")?bar(`|\")?\)/', $this->object->formatTableUnique($data));
    }

    /**
     * Test identifier quoting.
     */
    public function testQuote() {
        $this->assertEquals('"foo"', $this->object->quote('foo'));
        $this->assertEquals('"foo"', $this->object->quote('foo"'));
        $this->assertEquals('"foo"', $this->object->quote('""foo"'));

        $this->assertEquals('"foo"."bar"', $this->object->quote('foo.bar'));
        $this->assertEquals('"foo"."bar"', $this->object->quote('foo"."bar'));
        $this->assertEquals('"foo"."bar"', $this->object->quote('"foo"."bar"'));
        $this->assertEquals('"foo".*', $this->object->quote('foo.*'));
    }

    /**
     * Test multiple identifier quoting.
     */
    public function testQuoteList() {
        $this->assertEquals('"foo", "bar", "baz"', $this->object->quoteList(['foo', '"bar', '"baz"']));
        $this->assertEquals('"foo"."bar", "baz"', $this->object->quoteList(['foo.bar', '"baz"']));
    }

}