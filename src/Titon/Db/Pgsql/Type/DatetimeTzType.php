<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql\Type;

/**
 * Represents a timestamp with a time zone.
 *
 * @package Titon\Db\Pgsql\Type
 */
class DatetimeTzType extends \Titon\Db\Driver\Type\DatetimeType {

    /**
     * {@inheritdoc}
     */
    public $format = 'Y-m-d H:i:sO';

    /**
     * {@inheritdoc}
     */
    public function getName() {
        return self::DATETIME . 'tz';
    }

}