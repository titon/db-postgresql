<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql;

use Titon\Db\Data\AbstractMiscTest;
use Titon\Db\Query;
use Titon\Test\Stub\Table\User;

/**
 * Test class for misc database functionality.
 */
class MiscTest extends AbstractMiscTest {

    /**
     * Test table creation and deletion.
     */
    public function testCreateDropTable() {
        $user = new User();

        $sql = sprintf("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_catalog = 'titon_test' AND table_name = '%s';", $user->getTableName());

        $this->assertEquals(0, $user->getDriver()->query($sql)->count());

        $user->createTable();

        $this->assertEquals(1, $user->getDriver()->query($sql)->count());

        $user->query(Query::DROP_TABLE)->save();

        $this->assertEquals(0, $user->getDriver()->query($sql)->count());
    }

}