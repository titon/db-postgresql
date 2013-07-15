<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Pgsql;

use Titon\Model\Data\AbstractReadTest;
use Titon\Model\Entity;
use Titon\Model\Query\Func;
use Titon\Model\Query;
use Titon\Test\Stub\Model\Book;
use Titon\Test\Stub\Model\Order;
use Titon\Test\Stub\Model\Stat;
use Titon\Test\Stub\Model\User;

/**
 * Test class for database reading.
 */
class ReadTest extends AbstractReadTest {

	/**
	 * Test functions in select statements.
	 */
	public function testSelectFunctions() {
		$this->loadFixtures('Stats');

		$stat = new Stat();

		// SUM
		$query = $stat->select();
		$query->fields([
			$query->func('SUM', ['health' => Func::FIELD])->asAlias('sum')
		]);

		$this->assertEquals(['sum' => 2900], $query->fetch(false));

		// SUBSTRING
		$query = $stat->select();
		$query->fields([
			$query->func('SUBSTR', ['name' => Func::FIELD, 1, 3])->asAlias('shortName')
		]);

		$this->assertEquals([
			['shortName' => 'War'],
			['shortName' => 'Ran'],
			['shortName' => 'Mag'],
		], $query->fetchAll(false));
	}

	/**
	 * Test row counting.
	 */
	public function testSelectCount() {
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

	/**
	 * Test order by clause.
	 */
	public function testOrdering() {
		$this->loadFixtures('Books');

		$book = new Book();

		$this->assertEquals([
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
		], $book->select('id', 'series_id', 'name')->orderBy([
			'series_id' => 'desc',
			'id' => 'desc'
		])->fetchAll());

		$this->assertEquals([
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
		], $book->select('id', 'series_id', 'name')->orderBy([
			'series_id' => 'desc',
			'name' => 'asc'
		])->fetchAll());
	}

	/**
	 * Test group by clause.
	 */
	public function testGrouping() {
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

		$this->assertEquals([
			new Entity(['id' => 1, 'name' => 'A Game of Thrones']),
			new Entity(['id' => 6, 'name' => 'Harry Potter and the Philosopher\'s Stone']),
			new Entity(['id' => 13, 'name' => 'The Fellowship of the Ring'])
		], $query->fetchAll());
	}

	/**
	 * Test having predicates using AND conjunction.
	 */
	public function testHavingAnd() {
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
		$this->assertEquals([
			['id' => 1, 'user_id' => 1, 'quantity' => 15, 'status' => 'pending', 'shipped' => null, 'qty' => 15, 'count' => 1],
			['id' => 2, 'user_id' => 2, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1],
			['id' => 3, 'user_id' => 3, 'quantity' => 4, 'status' => 'pending', 'shipped' => null, 'qty' => 4, 'count' => 1],
			['id' => 4, 'user_id' => 4, 'quantity' => 24, 'status' => 'pending', 'shipped' => null, 'qty' => 24, 'count' => 1],
			['id' => 5, 'user_id' => 5, 'quantity' => 29, 'status' => 'pending', 'shipped' => null, 'qty' => 29, 'count' => 1],
		], $query->fetchAll(false));

		// It also doesn't support aliases in having, so re-SUM
		$query->having($query->func('SUM', ['quantity' => 'field']), '>', 25);

		$this->assertEquals([
			['id' => 11, 'user_id' => 1, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1],
			['id' => 2, 'user_id' => 2, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1],
			['id' => 12, 'user_id' => 4, 'quantity' => 26, 'status' => 'pending', 'shipped' => null, 'qty' => 26, 'count' => 1],
			['id' => 5, 'user_id' => 5, 'quantity' => 29, 'status' => 'pending', 'shipped' => null, 'qty' => 29, 'count' => 1],
		], $query->fetchAll(false));
	}

	/**
	 * Test having predicates using AND conjunction.
	 */
	public function testHavingOr() {
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

		$this->assertEquals([
			['id' => 1, 'user_id' => 1, 'quantity' => 15, 'status' => 'pending', 'shipped' => null, 'qty' => 15, 'count' => 1],
			['id' => 2, 'user_id' => 2, 'quantity' => 33, 'status' => 'pending', 'shipped' => null, 'qty' => 33, 'count' => 1],
			['id' => 3, 'user_id' => 3, 'quantity' => 4, 'status' => 'pending', 'shipped' => null, 'qty' => 4, 'count' => 1],
			['id' => 4, 'user_id' => 4, 'quantity' => 24, 'status' => 'pending', 'shipped' => null, 'qty' => 24, 'count' => 1],
			['id' => 5, 'user_id' => 5, 'quantity' => 29, 'status' => 'pending', 'shipped' => null, 'qty' => 29, 'count' => 1],
		], $query->fetchAll(false));

		$query->orHaving('status', '=', 'delivered');

		$this->assertEquals([
			['id' => 21, 'user_id' => 1, 'quantity' => 17, 'status' => 'delivered', 'shipped' => '2013-05-27 12:33:02', 'qty' => 17, 'count' => 1],
			['id' => 28, 'user_id' => 3, 'quantity' => 13, 'status' => 'delivered', 'shipped' => '2013-06-03 12:33:02', 'qty' => 13, 'count' => 1],
			['id' => 19, 'user_id' => 4, 'quantity' => 20, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 20, 'count' => 1],
			['id' => 20, 'user_id' => 5, 'quantity' => 18, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 18, 'count' => 1],
		], $query->fetchAll(false));

		$query->orHaving('status', '=', 'shipped');

		$this->assertEquals([
			['id' => 21, 'user_id' => 1, 'quantity' => 17, 'status' => 'delivered', 'shipped' => '2013-05-27 12:33:02', 'qty' => 17, 'count' => 1],
			['id' => 17, 'user_id' => 2, 'quantity' => 26, 'status' => 'shipped', 'shipped' => '2013-06-28 12:33:02', 'qty' => 26, 'count' => 1],
			['id' => 18, 'user_id' => 3, 'quantity' => 23, 'status' => 'shipped', 'shipped' => '2013-06-29 12:33:02', 'qty' => 23, 'count' => 1],
			['id' => 19, 'user_id' => 4, 'quantity' => 20, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 20, 'count' => 1],
			['id' => 16, 'user_id' => 5, 'quantity' => 33, 'status' => 'shipped', 'shipped' => '2013-06-27 12:33:02', 'qty' => 33, 'count' => 1],
		], $query->fetchAll(false));
	}

	/**
	 * Test nested having predicates.
	 */
	public function testHavingNested() {
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
			->having(function(Query $query) {
				$this->between($query->func('SUM', ['quantity' => 'field']), 20, 30);
				$this->either(function() {
					$this->eq('status', 'shipped');
					$this->eq('status', 'delivered');
				});
			});

		$this->assertEquals([
			['id' => 17, 'user_id' => 2, 'quantity' => 26, 'status' => 'shipped', 'shipped' => '2013-06-28 12:33:02', 'qty' => 26, 'count' => 1],
			['id' => 18, 'user_id' => 3, 'quantity' => 23, 'status' => 'shipped', 'shipped' => '2013-06-29 12:33:02', 'qty' => 23, 'count' => 1],
			['id' => 19, 'user_id' => 4, 'quantity' => 20, 'status' => 'delivered', 'shipped' => '2013-06-30 12:33:02', 'qty' => 20, 'count' => 1],
		], $query->fetchAll(false));
	}

	/**
	 * Test that right join fetches data.
	 */
	public function testRightJoin() {
		$this->loadFixtures(['Users', 'Countries']);

		$user = new User();
		$user->update([2, 5], ['country_id' => null]); // Reset some records

		$query = $user->select('id', 'username')
			->rightJoin($user->getRelation('Country'), [])
			->orderBy('User.id', 'asc');

		// PgSQL places nulls at the end
		$this->assertEquals([
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
		], $query->fetchAll());
	}

}