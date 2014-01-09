<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql;

use Titon\Db\Data\AbstractDeleteTest;

/**
 * Test class for database record deleting.
 */
class DeleteTest extends AbstractDeleteTest {

    /**
     * Test delete with a limit applied.
     */
    public function testDeleteLimit() {
        $this->markTestSkipped('PgSQL does not support LIMIT in DELETE statements');
    }

    /**
     * Test delete with ordering.
     */
    public function testDeleteOrdering() {
        $this->markTestSkipped('PgSQL does not support ORDER BY in DELETE statements');
    }

}