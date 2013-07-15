<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Pgsql;

use Titon\Model\Data\AbstractUpdateTest;
use Titon\Test\Stub\Model\Stat;

/**
 * Test class for database updating.
 */
class UpdateTest extends AbstractUpdateTest {

	/**
	 * Test multiple record updates with a limit and offset applied.
	 */
	public function testUpdateMultipleWithLimit() {
		$this->markTestSkipped('PgSQL does not support LIMIT in UPDATE statements');
	}

	/**
	 * Test multiple record updates with an order by applied.
	 */
	public function testUpdateMultipleWithOrderBy() {
		$this->markTestSkipped('PgSQL does not support ORDER BY in UPDATE statements');
	}

	/**
	 * Test updating blob data.
	 */
	public function testUpdateBlob() {
		$this->markTestIncomplete('Blob handling currently not supported');
	}

}