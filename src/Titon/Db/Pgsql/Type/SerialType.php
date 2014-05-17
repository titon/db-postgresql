<?php
/**
 * @copyright   2010-2014, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql\Type;

/**
 * Reset the options for PostgreSQL specific serial type.
 *
 * @package Titon\Db\Pgsql\Type
 */
class SerialType extends \Titon\Db\Driver\Type\SerialType {

    /**
     * {@inheritdoc}
     */
    public function getDefaultOptions() {
        return ['null' => false, 'primary' => true];
    }

}