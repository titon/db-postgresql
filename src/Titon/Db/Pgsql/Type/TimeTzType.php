<?php
/**
 * @copyright   2010-2014, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql\Type;

/**
 * Represents a time with a time zone.
 *
 * @package Titon\Db\Pgsql\Type
 */
class TimeTzType extends \Titon\Db\Driver\Type\TimeType {

    /**
     * {@inheritdoc}
     */
    public $format = 'H:i:sO';

    /**
     * {@inheritdoc}
     */
    public function getName() {
        return self::TIME . 'tz';
    }

}