<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Model\Pgsql;

use Titon\Model\Data\AbstractMiscTest;
use Titon\Model\Query;
use Titon\Test\Stub\Model\User;

/**
 * Test class for misc database functionality.
 */
class MiscTest extends AbstractMiscTest {

    /**
     * Test table creation and deletion.
     */
    public function testCreateDropTable() {
        $user = new User();

        $sql = sprintf("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_catalog = 'titon_test' AND table_name = '%s';", $user->getTable());

        $this->assertEquals(0, $user->getDriver()->query($sql)->count());

        $user->createTable();

        $this->assertEquals(1, $user->getDriver()->query($sql)->count());

        $user->query(Query::DROP_TABLE)->save();

        $this->assertEquals(0, $user->getDriver()->query($sql)->count());
    }

}