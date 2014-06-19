<?php
namespace Titon\Db\Pgsql;

use Titon\Db\Entity;
use Titon\Db\EntityCollection;
use Titon\Db\Query;
use Titon\Db\Query\Func;
use Titon\Db\Query\Predicate;
use Titon\Db\Repository;
use Titon\Test\Stub\Repository\Book;
use Titon\Test\Stub\Repository\Order;
use Titon\Test\Stub\Repository\Stat;
use Titon\Test\Stub\Repository\User;
use \DateTime;

class RepositoryTest extends \Titon\Db\RepositoryTest {

    public function testCreateDropTable() {
        $sql = sprintf("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_catalog = 'titon_test' AND table_name = '%s';", $this->object->getTable());

        $this->assertEquals(0, $this->object->getDriver()->executeQuery($sql)->count());

        $this->object->createTable();

        $this->assertEquals(1, $this->object->getDriver()->executeQuery($sql)->count());

        $this->object->dropTable();

        $this->assertEquals(0, $this->object->getDriver()->executeQuery($sql)->count());
    }

    public function testDeleteWithLimit() {
        $this->markTestSkipped('PgSQL does not support LIMIT in DELETE statements');
    }

    public function testDeleteWithOrdering() {
        $this->markTestSkipped('PgSQL does not support ORDER BY in DELETE statements');
    }

    public function testFindLastReversedOrder() {
        $this->loadFixtures('Users');

        // PGSQL returns empty field values first
        $this->assertEquals(new Entity([
            'id' => 5,
            'country_id' => 4,
            'username' => 'wolverine',
            'firstName' => 'Logan',
            'lastName' => '',
            'password' => '1Z5895jf72yL77h',
            'email' => 'wolverine@email.com',
            'age' => 355,
            'created' => '2000-11-30 21:22:34',
            'modified' => null
        ]), $this->object->select()->orderBy('lastName', 'asc')->last());
    }

    public function testSelect() {
        $query = new PgsqlQuery(PgsqlQuery::SELECT, $this->object);
        $query->from($this->object->getTable(), 'User')->fields('id', 'username');

        $this->assertEquals($query, $this->object->select('id', 'username'));
    }

    public function testSelectRawExpressions() {
        $this->loadFixtures('Stats');

        $stat = new Stat();

        // In place of expr()
        $query = $stat->select();
        $query->fields([
            'name AS role',
            Query::raw('"name" AS "class"')
        ]);

        $this->assertEquals(new EntityCollection([
            new Entity(['role' => 'Warrior', 'class' => 'Warrior']),
            new Entity(['role' => 'Ranger', 'class' => 'Ranger']),
            new Entity(['role' => 'Mage', 'class' => 'Mage']),
        ]), $query->all());

        // In place of func()
        $query = $stat->select();
        $query->fields([
            Query::raw('SUBSTR("name", 1, 3) as "shortName"')
        ]);

        $this->assertEquals(new EntityCollection([
            new Entity(['shortName' => 'War']),
            new Entity(['shortName' => 'Ran']),
            new Entity(['shortName' => 'Mag']),
        ]), $query->all());
    }

    public function testSelectFunctions() {
        $this->loadFixtures('Stats');

        $stat = new Stat();

        // SUM
        $query = $stat->select();
        $query->fields([
            $query->func('SUM', ['health' => Func::FIELD])->asAlias('sum')
        ]);

        $this->assertEquals(new Entity(['sum' => 2900]), $query->first());

        // SUBSTRING
        $query = $stat->select();
        $query->fields([
            $query->func('SUBSTR', ['name' => Func::FIELD, 1, 3])->asAlias('shortName')
        ]);

        $this->assertEquals(new EntityCollection([
            new Entity(['shortName' => 'War']),
            new Entity(['shortName' => 'Ran']),
            new Entity(['shortName' => 'Mag']),
        ]), $query->all());
    }

    public function testSelectAggregateCount() {
        $this->loadFixtures('Books');

        $book = new Book();

        $query = $book->select();
        $this->assertEquals(15, $query->count());

        $query->where('series_id', 2);
        $this->assertEquals(7, $query->count());

        // PgSQL is case sensitive, so capitalize Prince
        $query->where('name', 'like', '%Prince%');
        $this->assertEquals(1, $query->count());
    }

    public function testSelectGrouping() {
        $this->loadFixtures('Books');

        $book = new Book();

        // PgSQL group by is different than MySQL
        // Use DISTINCT ON + multiple group/order by
        $query = $book->select('id', 'name')
            ->groupBy('id', 'series_id')
            ->orderBy([
                'series_id' => 'asc',
                'id' => 'asc'
            ])
            ->attribute('distinct', function(PgsqlDialect $dialect) {
                return sprintf($dialect->getClause(PgsqlDialect::DISTINCT_ON), $dialect->quote('series_id'));
            });

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 1, 'name' => 'A Game of Thrones']),
            new Entity(['id' => 6, 'name' => 'Harry Potter and the Philosopher\'s Stone']),
            new Entity(['id' => 13, 'name' => 'The Fellowship of the Ring'])
        ]), $query->all());
    }

    public function testSelectOrdering() {
        $this->loadFixtures('Books');

        $book = new Book();

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 15, 'series_id' => 3, 'name' => 'The Return of the King']),
            new Entity(['id' => 14, 'series_id' => 3, 'name' => 'The Two Towers']),
            new Entity(['id' => 13, 'series_id' => 3, 'name' => 'The Fellowship of the Ring']),
            new Entity(['id' => 12, 'series_id' => 2, 'name' => 'Harry Potter and the Deathly Hallows']),
            new Entity(['id' => 11, 'series_id' => 2, 'name' => 'Harry Potter and the Half-blood Prince']),
            new Entity(['id' => 10, 'series_id' => 2, 'name' => 'Harry Potter and the Order of the Phoenix']),
            new Entity(['id' => 9, 'series_id' => 2, 'name' => 'Harry Potter and the Goblet of Fire']),
            new Entity(['id' => 8, 'series_id' => 2, 'name' => 'Harry Potter and the Prisoner of Azkaban']),
            new Entity(['id' => 7, 'series_id' => 2, 'name' => 'Harry Potter and the Chamber of Secrets']),
            new Entity(['id' => 6, 'series_id' => 2, 'name' => 'Harry Potter and the Philosopher\'s Stone']),
            new Entity(['id' => 5, 'series_id' => 1, 'name' => 'A Dance with Dragons']),
            new Entity(['id' => 4, 'series_id' => 1, 'name' => 'A Feast for Crows']),
            new Entity(['id' => 3, 'series_id' => 1, 'name' => 'A Storm of Swords']),
            new Entity(['id' => 2, 'series_id' => 1, 'name' => 'A Clash of Kings']),
            new Entity(['id' => 1, 'series_id' => 1, 'name' => 'A Game of Thrones']),
        ]), $book->select('id', 'series_id', 'name')->orderBy([
            'series_id' => 'desc',
            'id' => 'desc'
        ])->all());

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 13, 'series_id' => 3, 'name' => 'The Fellowship of the Ring']),
            new Entity(['id' => 15, 'series_id' => 3, 'name' => 'The Return of the King']),
            new Entity(['id' => 14, 'series_id' => 3, 'name' => 'The Two Towers']),
            new Entity(['id' => 7, 'series_id' => 2, 'name' => 'Harry Potter and the Chamber of Secrets']),
            new Entity(['id' => 12, 'series_id' => 2, 'name' => 'Harry Potter and the Deathly Hallows']),
            new Entity(['id' => 9, 'series_id' => 2, 'name' => 'Harry Potter and the Goblet of Fire']),
            new Entity(['id' => 11, 'series_id' => 2, 'name' => 'Harry Potter and the Half-blood Prince']),
            new Entity(['id' => 10, 'series_id' => 2, 'name' => 'Harry Potter and the Order of the Phoenix']),
            new Entity(['id' => 6, 'series_id' => 2, 'name' => 'Harry Potter and the Philosopher\'s Stone']),
            new Entity(['id' => 8, 'series_id' => 2, 'name' => 'Harry Potter and the Prisoner of Azkaban']),
            new Entity(['id' => 2, 'series_id' => 1, 'name' => 'A Clash of Kings']),
            new Entity(['id' => 5, 'series_id' => 1, 'name' => 'A Dance with Dragons']),
            new Entity(['id' => 4, 'series_id' => 1, 'name' => 'A Feast for Crows']),
            new Entity(['id' => 1, 'series_id' => 1, 'name' => 'A Game of Thrones']),
            new Entity(['id' => 3, 'series_id' => 1, 'name' => 'A Storm of Swords']),
        ]), $book->select('id', 'series_id', 'name')->orderBy([
            'series_id' => 'desc',
            'name' => 'asc'
        ])->all());
    }

    public function testSelectHavingAnd() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                $query->func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                $query->func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->groupBy('id', 'user_id')
            ->orderBy([
                'user_id' => 'asc',
                'id' => 'asc'
            ])
            ->attribute('distinct', function(PgsqlDialect $dialect) {
                return sprintf($dialect->getClause(PgsqlDialect::DISTINCT_ON), $dialect->quote('user_id'));
            });

        // Since PgSQL uses distinct, the qty/count values are different
        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 1, 'user_id' => 1, 'quantity' => 15, 'status' => 'pending', 'shipped' => null, 'qty' => 15, 'count' => 1]),
            new Entity(['id' => 2, 'user_id' => 2, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1]),
            new Entity(['id' => 3, 'user_id' => 3, 'quantity' => 4, 'status' => 'pending', 'shipped' => null, 'qty' => 4, 'count' => 1]),
            new Entity(['id' => 4, 'user_id' => 4, 'quantity' => 24, 'status' => 'pending', 'shipped' => null, 'qty' => 24, 'count' => 1]),
            new Entity(['id' => 5, 'user_id' => 5, 'quantity' => 29, 'status' => 'pending', 'shipped' => null, 'qty' => 29, 'count' => 1]),
        ]), $query->all());

        // It also doesn't support aliases in having, so re-SUM
        $query->having($query->func('SUM', ['quantity' => 'field']), '>', 25);

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 11, 'user_id' => 1, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1]),
            new Entity(['id' => 2, 'user_id' => 2, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1]),
            new Entity(['id' => 12, 'user_id' => 4, 'quantity' => 26, 'status' => 'pending', 'shipped' => null, 'qty' => 26, 'count' => 1]),
            new Entity(['id' => 5, 'user_id' => 5, 'quantity' => 29, 'status' => 'pending', 'shipped' => null, 'qty' => 29, 'count' => 1]),
        ]), $query->all());
    }

    public function testSelectHavingOr() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                $query->func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                $query->func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->groupBy('id', 'user_id')
            ->orderBy([
                'user_id' => 'asc',
                'id' => 'asc'
            ])
            ->attribute('distinct', function(PgsqlDialect $dialect) {
                return sprintf($dialect->getClause(PgsqlDialect::DISTINCT_ON), $dialect->quote('user_id'));
            });

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 1, 'user_id' => 1, 'quantity' => 15, 'status' => 'pending', 'shipped' => null, 'qty' => 15, 'count' => 1]),
            new Entity(['id' => 2, 'user_id' => 2, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1]),
            new Entity(['id' => 3, 'user_id' => 3, 'quantity' => 4, 'status' => 'pending', 'shipped' => null, 'qty' => 4, 'count' => 1]),
            new Entity(['id' => 4, 'user_id' => 4, 'quantity' => 24, 'status' => 'pending', 'shipped' => null, 'qty' => 24, 'count' => 1]),
            new Entity(['id' => 5, 'user_id' => 5, 'quantity' => 29, 'status' => 'pending', 'shipped' => null, 'qty' => 29, 'count' => 1]),
        ]), $query->all());

        $query->orHaving('status', '=', 'delivered');

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 21, 'user_id' => 1, 'quantity' => 17, 'status' => 'delivered', 'shipped' => '2013-05-27 12:33:02', 'qty' => 17, 'count' => 1]),
            new Entity(['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 13, 'count' => 1]),
            new Entity(['id' => 19, 'user_id' => 4, 'quantity' => 20, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 20, 'count' => 1]),
            new Entity(['id' => 20, 'user_id' => 5, 'quantity' => 18, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 18, 'count' => 1]),
        ]), $query->all());

        $query->orHaving('status', '=', 'shipped');

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 21, 'user_id' => 1, 'quantity' => 17, 'status' => 'delivered', 'shipped' => '2013-05-27 12:33:02', 'qty' => 17, 'count' => 1]),
            new Entity(['id' => 17, 'user_id' => 2, 'quantity' => 26, 'status' => 'shipped', 'shipped' => '2013-06-28 12:33:02', 'qty' => 26, 'count' => 1]),
            new Entity(['id' => 18, 'user_id' => 3, 'quantity' => 23, 'status' => 'shipped', 'shipped' => '2013-06-29 12:33:02', 'qty' => 23, 'count' => 1]),
            new Entity(['id' => 19, 'user_id' => 4, 'quantity' => 20, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 20, 'count' => 1]),
            new Entity(['id' => 16, 'user_id' => 5, 'quantity' => 33, 'status' => 'shipped', 'shipped' => '2013-06-27 12:33:02', 'qty' => 33, 'count' => 1]),
        ]), $query->all());
    }

    public function testSelectHavingNested() {
        $this->loadFixtures('Orders');

        $order = new Order();
        $query = $order->select();
        $query
            ->fields([
                'id', 'user_id', 'quantity', 'status', 'shipped',
                $query->func('SUM', ['quantity' => 'field'])->asAlias('qty'),
                $query->func('COUNT', ['user_id' => 'field'])->asAlias('count')
            ])
            ->where('status', '!=', 'pending')
            ->groupBy('id', 'user_id')
            ->orderBy([
                'user_id' => 'asc',
                'id' => 'asc'
            ])
            ->attribute('distinct', function(PgsqlDialect $dialect) {
                return sprintf($dialect->getClause(PgsqlDialect::DISTINCT_ON), $dialect->quote('user_id'));
            })
            ->having(function(Predicate $pred, Query $query) {
                $pred->between($query->func('SUM', ['quantity' => 'field']), 20, 30);
                $pred->either(function(Predicate $pred2) {
                    $pred2->eq('status', 'shipped');
                    $pred2->eq('status', 'delivered');
                });
            });

        $this->assertEquals(new EntityCollection([
            new Entity(['id' => 17, 'user_id' => 2, 'quantity' => 26, 'status' => 'shipped', 'shipped' => '2013-06-28 12:33:02', 'qty' => 26, 'count' => 1]),
            new Entity(['id' => 18, 'user_id' => 3, 'quantity' => 23, 'status' => 'shipped', 'shipped' => '2013-06-29 12:33:02', 'qty' => 23, 'count' => 1]),
            new Entity(['id' => 19, 'user_id' => 4, 'quantity' => 20, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 20, 'count' => 1]),
        ]), $query->all());
    }

    public function testSelectRightJoin() {
        $this->loadFixtures(['Users', 'Countries']);

        $user = new User();
        $user->update([2, 5], ['country_id' => null]); // Reset some records

        $query = $user->select('id', 'username')
            ->rightJoin(['countries', 'Country'], ['id', 'name', 'iso'], ['User.country_id' => 'Country.id'])
            ->orderBy('User.id', 'asc');

        // PgSQL places nulls at the end
        $this->assertEquals(new EntityCollection([
            new Entity([
                'id' => 1,
                'username' => 'miles',
                'Country' => new Entity([
                    'id' => 1,
                    'name' => 'United States of America',
                    'iso' => 'USA'
                ])
            ]),
            new Entity([
                'id' => 3,
                'username' => 'superman',
                'Country' => new Entity([
                    'id' => 2,
                    'name' => 'Canada',
                    'iso' => 'CAN'
                ])
            ]),
            new Entity([
                'id' => 4,
                'username' => 'spiderman',
                'Country' => new Entity([
                    'id' => 5,
                    'name' => 'Mexico',
                    'iso' => 'MEX'
                ])
            ]),
            // Empty user
            new Entity([
                'id' => null,
                'username' => null,
                'Country' => new Entity([
                    'id' => 3,
                    'name' => 'England',
                    'iso' => 'ENG'
                ])
            ]),
            // Empty user
            new Entity([
                'id' => null,
                'username' => null,
                'Country' => new Entity([
                    'id' => 4,
                    'name' => 'Australia',
                    'iso' => 'AUS'
                ])
            ]),
        ]), $query->all());
    }

    public function testSelectUnions() {
        $this->loadFixtures(['Users', 'Books', 'Authors']);

        $user = new User();
        $query = $user->select('username AS name');
        $query->union($query->subQuery('name')->from('books')->where('series_id', 1));
        $query->union($query->subQuery('name')->from('authors'));

        // PGSQL returns them in a different order by default
        $this->assertEquals(new EntityCollection([
            new Entity(['name' => 'spiderman']),
            new Entity(['name' => 'J. R. R. Tolkien']),
            new Entity(['name' => 'wolverine']),
            new Entity(['name' => 'A Feast for Crows']),
            new Entity(['name' => 'A Game of Thrones']),
            new Entity(['name' => 'superman']),
            new Entity(['name' => 'batman']),
            new Entity(['name' => 'miles']),
            new Entity(['name' => 'A Dance with Dragons']),
            new Entity(['name' => 'A Storm of Swords']),
            new Entity(['name' => 'J. K. Rowling']),
            new Entity(['name' => 'A Clash of Kings']),
            new Entity(['name' => 'George R. R. Martin']),
        ]), $query->all());

        $query->orderBy('name', 'desc')->limit(10);

        $this->assertEquals(new EntityCollection([
            new Entity(['name' => 'wolverine']),
            new Entity(['name' => 'superman']),
            new Entity(['name' => 'spiderman']),
            new Entity(['name' => 'miles']),
            new Entity(['name' => 'J. R. R. Tolkien']),
            new Entity(['name' => 'J. K. Rowling']),
            new Entity(['name' => 'George R. R. Martin']),
            new Entity(['name' => 'batman']),
            new Entity(['name' => 'A Storm of Swords']),
            new Entity(['name' => 'A Game of Thrones']),
        ]), $query->all());
    }

    public function testSelectTypeCastingStatements() {
        $this->loadFixtures(['Stats', 'Users']);

        $stat = new Stat();
        $time = time();
        $date = date('Y-m-d H:i:s', $time);
        $driver = $stat->getDriver();

        // int
        $query = $driver->executeQuery($stat->select()->where('health', '>', '100'));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?stats(`|\")? WHERE (`|\")?health(`|\")? > 100;$/i", $query->getStatement());

        // PgSQL wraps the IN values in single quotes
        $query = $driver->executeQuery($stat->select()->where('id', [1, '2', 3]));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?stats(`|\")? WHERE (`|\")?id(`|\")? IN \('1', '2', '3'\);$/i", $query->getStatement());

        // string
        $query = $driver->executeQuery($stat->select()->where('name', '!=', 123.45));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?stats(`|\")? WHERE (`|\")?name(`|\")? != '123.45';$/i", $query->getStatement());

        // float (they are strings in PDO)
        $query = $driver->executeQuery($stat->select()->where('damage', '<', 55.25));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?stats(`|\")? WHERE (`|\")?damage(`|\")? < '55.25';$/i", $query->getStatement());

        // bool
        $query = $driver->executeQuery($stat->select()->where('isMelee', true));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?stats(`|\")? WHERE (`|\")?isMelee(`|\")? = 1;$/i", $query->getStatement());

        $query = $driver->executeQuery($stat->select()->where('isMelee', '0'));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?stats(`|\")? WHERE (`|\")?isMelee(`|\")? = 0;$/i", $query->getStatement());

        // datetime
        $query = $driver->executeQuery($this->object->select()->where('created', '>', $time));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?users(`|\")? WHERE (`|\")?created(`|\")? > '" . $date . "';$/i", $query->getStatement());

        $query = $driver->executeQuery($this->object->select()->where('created', '<=', new DateTime($date)));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?users(`|\")? WHERE (`|\")?created(`|\")? <= '" . $date . "';$/i", $query->getStatement());

        $query = $driver->executeQuery($this->object->select()->where('created', '!=', $date));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?users(`|\")? WHERE (`|\")?created(`|\")? != '" . $date . "';$/i", $query->getStatement());

        // null
        $query = $driver->executeQuery($this->object->select()->where('created', null));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?users(`|\")? WHERE (`|\")?created(`|\")? IS NULL;$/i", $query->getStatement());

        $query = $driver->executeQuery($this->object->select()->where('created', '!=', null));
        $this->assertRegExp("/^SELECT \* FROM (`|\")?users(`|\")? WHERE (`|\")?created(`|\")? IS NOT NULL;$/i", $query->getStatement());
    }

    public function testUpdateMultipleWithLimit() {
        $this->markTestSkipped('PgSQL does not support LIMIT in UPDATE statements');
    }

    public function testUpdateMultipleWithOrderBy() {
        $this->markTestSkipped('PgSQL does not support ORDER BY in UPDATE statements');
    }

    public function testUpdateTypeBlob() {
        $this->markTestIncomplete('Blob handling currently not supported');
    }
}