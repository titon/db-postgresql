<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Pgsql\Type;

/**
 * Reset the options for PostgreSQL specific serial type.
 *
 * @package Titon\Model\Pgsql\Type
 */
class SerialType extends \Titon\Model\Driver\Type\SerialType {

	/**
	 * {@inheritdoc}
	 */
	public function getDefaultOptions() {
		return ['null' => false, 'primary' => true];
	}

}