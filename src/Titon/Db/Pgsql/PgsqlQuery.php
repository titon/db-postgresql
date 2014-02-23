<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql;

use Titon\Db\Query;

/**
 * Defines PostgreSQL only query functionality.
 *
 * @package Titon\Db\Pgsql
 */
class PgsqlQuery extends Query {

    /**
     * Lock all rows using a shared lock instead of exclusive.
     *
     * @return $this
     */
    public function lockForShare() {
        return $this->attribute('lock', PgsqlDialect::FOR_SHARE_LOCK);
    }

    /**
     * Lock all rows returned from a select as if they were locked for update.
     *
     * @return $this
     */
    public function lockForUpdate() {
        return $this->attribute('lock', PgsqlDialect::FOR_UPDATE_LOCK);
    }

}