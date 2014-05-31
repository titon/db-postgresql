<?php
namespace Titon\Db\Pgsql;

use Exception;
use Titon\Common\Config;
use Titon\Db\Driver\Dialect;
use Titon\Db\Driver\Dialect\Statement;
use Titon\Db\Driver\Schema;
use Titon\Db\Query;
use Titon\Test\Stub\Repository\User;

class DialectTest extends \Titon\Db\Driver\DialectTest {

    protected function setUp() {
        $this->driver = new PgsqlDriver(Config::get('db'));
        $this->driver->connect();

        $this->object = $this->driver->getDialect();
    }

    public function testAddStatements() {
        $this->assertFalse($this->object->hasStatement('foo'));
        $this->assertEquals(new Statement('TRUNCATE {only} {table} {identity} {action}'), $this->object->getStatement('truncate'));

        $this->object->addStatements([
            'foo' => new Statement('Foo'),
            'truncate' => new Statement('TRUNCATE ALL THE THINGS')
        ]);

        $this->assertTrue($this->object->hasStatement('foo'));
        $this->assertEquals(new Statement('TRUNCATE ALL THE THINGS'), $this->object->getStatement('truncate'));
    }

    public function testBuildCreateIndex() {
        $query = new Query(Query::CREATE_INDEX, new User());
        $query->data(['profile_id'])->from('users')->asAlias('idx');

        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\)/', $this->object->buildCreateIndex($query));

        $query->data(['profile_id' => 5]);
        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\(5\)\)/', $this->object->buildCreateIndex($query));

        $query->data(['profile_id' => 'asc', 'other_id']);
        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")? ASC, (`|\")?other_id(`|\")?\)/', $this->object->buildCreateIndex($query));

        $query->data(['profile_id' => ['length' => 5, 'order' => 'desc']]);
        $this->assertRegExp('/CREATE\s+INDEX\s+(`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\(5\) DESC\)/', $this->object->buildCreateIndex($query));

        $query->data(['profile_id'])->attribute([
            'type' => PgsqlDialect::UNIQUE,
            'concurrently' => true
        ]);
        $this->assertRegExp('/CREATE UNIQUE INDEX CONCURRENTLY (`|\")?idx(`|\")? ON (`|\")?users(`|\")? \((`|\")?profile_id(`|\")?\)/', $this->object->buildCreateIndex($query));
    }

    public function testBuildCreateTable() {
        $schema = new Schema('foobar');
        $schema->addColumn('column', [
            'type' => 'int',
            'ai' => true
        ]);

        $query = new Query(Query::CREATE_TABLE, new User());
        $query->schema($schema);

        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL\n\);/', $this->object->buildCreateTable($query));

        // With columns
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

        // With options
        $schema->addOption('onCommit', PgsqlDialect::DELETE_ROWS);
        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\n(`|\")?column2(`|\")? integer NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\) ON COMMIT DELETE ROWS;/', $this->object->buildCreateTable($query));

        $schema->addOption('tablespace', 'foobar');
        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\n(`|\")?column2(`|\")? integer NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\) ON COMMIT DELETE ROWS TABLESPACE foobar;/', $this->object->buildCreateTable($query));

        $schema->addOption('with', PgsqlDialect::WITH_OIDS);
        $this->assertRegExp('/CREATE\s+TABLE IF NOT EXISTS (`|\")?foobar(`|\")? \(\n(`|\")?column(`|\")? integer NOT NULL,\n(`|\")?column2(`|\")? integer NULL,\nPRIMARY KEY \((`|\")?column(`|\")?\)\n\) ON COMMIT DELETE ROWS TABLESPACE foobar WITH OIDS;/', $this->object->buildCreateTable($query));
    }

    public function testBuildDelete() {
        $query = new Query(Query::DELETE, new User());

        $query->from('foobar');
        $this->assertRegExp('/DELETE FROM\s+(`|\")?foobar(`|\")?;/', $this->object->buildDelete($query));

        // PGSQL doesn't support limit
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

    public function testBuildDeleteJoins() {
        $this->markTestSkipped('PGSQL does not support delete joins');
    }

    public function testBuildDropTable() {
        $query = new Query(Query::DROP_TABLE, new User());
        $query->from('foobar');

        $this->assertRegExp('/DROP TABLE IF EXISTS (`|\")?foobar(`|\")?;/', $this->object->buildDropTable($query));

        $query->attribute('action', PgsqlDialect::RESTRICT);
        $this->assertRegExp('/DROP TABLE IF EXISTS (`|\")?foobar(`|\")? RESTRICT;/', $this->object->buildDropTable($query));
    }

    public function testBuildDropIndex() {
        $query = new Query(Query::DROP_INDEX, new User());
        $query->from('users')->asAlias('idx');

        $this->assertRegExp('/DROP INDEX\s+IF EXISTS (`|\")?idx(`|\")?/', $this->object->buildDropIndex($query));

        $query->attribute('concurrently', true);
        $this->assertRegExp('/DROP INDEX CONCURRENTLY IF EXISTS (`|\")?idx(`|\")?/', $this->object->buildDropIndex($query));
    }

    public function testBuildSelectLocking() {
        $query = new PgsqlQuery(Query::SELECT, new User());
        $query->from('users')->where('name', 'like', '%miles%')->lockForShare();

        $this->assertRegExp('/SELECT\s+\* FROM\s+"users"\s+WHERE "name" LIKE \?\s+FOR SHARE;/', $this->object->buildSelect($query));

        $query->lockForUpdate();
        $this->assertRegExp('/SELECT\s+\* FROM\s+"users"\s+WHERE "name" LIKE \?\s+FOR UPDATE;/', $this->object->buildSelect($query));
    }

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

    public function testBuildUpdate() {
        $query = new Query(Query::UPDATE, new User());
        $query->from('foobar');

        $query->data(['username' => 'miles']);
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?username(`|\")? = \?;/', $this->object->buildUpdate($query));

        // PGSQL doesn't support limit
        $query->limit(15);
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?username(`|\")? = \?;/', $this->object->buildUpdate($query));

        // or order by
        $query->orderBy('username', 'desc');
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?username(`|\")? = \?\;/', $this->object->buildUpdate($query));

        $query->data([
            'email' => 'email@domain.com',
            'website' => 'http://titon.io'
        ]);
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?email(`|\")? = \?, (`|\")?website(`|\")? = \?;/', $this->object->buildUpdate($query));

        $query->where('status', 3);
        $this->assertRegExp('/UPDATE\s+(`|\")?foobar(`|\")?\s+SET (`|\")?email(`|\")? = \?, (`|\")?website(`|\")? = \?\s+WHERE (`|\")?status(`|\")? = \?;/', $this->object->buildUpdate($query));

        $query->attribute('only', true);
        $this->assertRegExp('/UPDATE ONLY (`|\")?foobar(`|\")?\s+SET (`|\")?email(`|\")? = \?, (`|\")?website(`|\")? = \?\s+WHERE (`|\")?status(`|\")? = \?;/', $this->object->buildUpdate($query));
    }

    public function testBuildUpdateJoins() {
        $this->markTestSkipped('PGSQL does not support update joins');
    }

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

        $schema->addColumn('column6', [
            'type' => 'varchar',
            'constraint' => 'foobar'
        ]);

        $expected .= ',\n(`|\")?column6(`|\")? varchar\(255\) CONSTRAINT "foobar" NULL';

        $this->assertRegExp('/' . $expected . '/', $this->object->formatColumns($schema));
    }

    public function testFormatFieldsWithJoins() {
        $query = new Query(Query::SELECT, new User());
        $query->fields(['id', 'country_id', 'username']);
        $query->leftJoin(['countries', 'Country'], ['iso'],['users.country_id' => 'Country.id'] );

        $this->assertRegExp('/(`|\")?User(`|\")?\.(`|\")?id(`|\")? AS User__id, (`|\")?User(`|\")?\.(`|\")?country_id(`|\")? AS User__country_id, (`|\")?User(`|\")?\.(`|\")?username(`|\")? AS User__username, (`|\")?Country(`|\")?\.(`|\")?iso(`|\")? AS Country__iso/', $this->object->formatFields($query));
    }

    public function testFormatTableIndex() {
        $this->markTestSkipped('PGSQL does not support CREATE TABLE statement indices');
    }

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

    public function testFormatTableUnique() {
        $data = ['columns' => ['foo'], 'constraint' => '', 'index' => 'idx'];

        $this->assertRegExp('/UNIQUE \((`|\")?foo(`|\")?\)/', $this->object->formatTableUnique($data));

        $data['constraint'] = 'symbol';
        $this->assertRegExp('/CONSTRAINT (`|\")?symbol(`|\")? UNIQUE \((`|\")?foo(`|\")?\)/', $this->object->formatTableUnique($data));

        $data['columns'][] = 'bar';
        $this->assertRegExp('/CONSTRAINT (`|\")?symbol(`|\")? UNIQUE \((`|\")?foo(`|\")?, (`|\")?bar(`|\")?\)/', $this->object->formatTableUnique($data));
    }

    public function testQuote() {
        $this->assertEquals('', $this->object->quote(''));
        $this->assertEquals('*', $this->object->quote('*'));
        $this->assertEquals('"foo"', $this->object->quote('foo'));
        $this->assertEquals('"foo"', $this->object->quote('foo"'));
        $this->assertEquals('"foo"', $this->object->quote('""foo"'));
        $this->assertEquals('"foo"', $this->object->quote('f"o"o'));

        $this->assertEquals('"foo"."bar"', $this->object->quote('foo.bar'));
        $this->assertEquals('"foo"."bar"', $this->object->quote('foo"."bar'));
        $this->assertEquals('"foo"."bar"', $this->object->quote('"foo"."bar"'));
        $this->assertEquals('"foo".*', $this->object->quote('foo.*'));
    }

    public function testQuoteList() {
        $this->assertEquals('"foo", "bar", "baz"', $this->object->quoteList(['foo', '"bar', '"baz"']));
        $this->assertEquals('"foo"."bar", "baz"', $this->object->quoteList(['foo.bar', '"baz"']));
    }

    public function testRenderStatement() {
        $this->assertEquals('SELECT  * FROM tableName;', $this->object->renderStatement(Query::SELECT, [
            'table' => 'tableName',
            'fields' => '*'
        ]));
    }

}