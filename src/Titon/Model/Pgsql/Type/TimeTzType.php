<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Model\Pgsql\Type;

/**
 * Represents a time with a time zone.
 *
 * @package Titon\Model\Pgsql\Type
 */
class TimeTzType extends \Titon\Model\Driver\Type\TimeType {

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