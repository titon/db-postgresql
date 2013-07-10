<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Postgresql;

use Titon\Model\Driver\AbstractPdoDriver;

/**
 * A driver that represents the PostgreSQL database and uses PDO.
 *
 * @package Titon\Model\Postgresql
 */
class PostgresqlDriver extends AbstractPdoDriver {

	/**
	 * Configuration.
	 */
	protected $_config = [
		'port' =>  5432
	];

	/**
	 * Set the dialect.
	 */
	public function initialize() {
		$this->setDialect(new PostgresqlDialect($this));
	}

	/**
	 * {@inheritdoc}
	 */
	public function getDriver() {
		return 'pgsql';
	}

	/**
	 * {@inheritdoc}
	 */
	public function getSupportedTypes() {
		return [
			'numeric' => 'Titon\Model\Driver\Type\DecimalType',
			'real' => 'Titon\Model\Driver\Type\FloatType',
			'double precision' => 'Titon\Model\Driver\Type\DoubleType',
			'smallserial' => 'Titon\Model\Driver\Type\SerialType',
			'bigserial' => 'Titon\Model\Driver\Type\SerialType',
			// TODO money
			'bytea' => 'Titon\Model\Driver\Type\BinaryType',
			// TODO timestamp w/ timezone
			// TODO interval
			// TODO bit
			// TODO complex types
		] + parent::getSupportedTypes();
	}

	/**
	 * {@inheritdoc}
	 */
	public function isEnabled() {
		return extension_loaded('pdo_pgsql');
	}

}