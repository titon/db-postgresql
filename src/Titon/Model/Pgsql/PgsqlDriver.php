<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Pgsql;

use Titon\Model\Driver\AbstractPdoDriver;

/**
 * A driver that represents the PostgreSQL database and uses PDO.
 *
 * @package Titon\Model\Pgsql
 */
class PgsqlDriver extends AbstractPdoDriver {

	/**
	 * Configuration.
	 */
	protected $_config = [
		'port' => 5432
	];

	/**
	 * Set the dialect.
	 */
	public function initialize() {
		$this->setDialect(new PgsqlDialect($this));
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
	public function getDsn() {
		if ($dsn = $this->config->dsn) {
			return $dsn;
		}

		$params = [
			'dbname=' . $this->getDatabase(),
			'host=' . $this->getHost(),
			'port=' . $this->getPort()
		];

		$dsn = $this->getDriver() . ':' . implode(';', $params);

		return $dsn;
	}

	/**
	 * {@inheritdoc}
	 */
	public function getSupportedTypes() {
		// TODO
		return [
			'bigint' => 'Titon\Model\Driver\Type\BigintType',
			'int8' => 'Titon\Model\Driver\Type\BigintType',
			'bigserial' => 'Titon\Model\Driver\Type\SerialType',
			'serial8' => 'Titon\Model\Driver\Type\SerialType',
			'bit' => 'Titon\Model\Driver\Type\BinaryType',
			'bit varying' => 'Titon\Model\Driver\Type\BinaryType',
			'varbit' => 'Titon\Model\Driver\Type\BinaryType',
			'bool' => 'Titon\Model\Driver\Type\BooleanType',
			'boolean' => 'Titon\Model\Driver\Type\BooleanType',
			// box
			'bytea' => 'Titon\Model\Driver\Type\BlobType',
			'char' => 'Titon\Model\Driver\Type\CharType',
			'character' => 'Titon\Model\Driver\Type\CharType',
			'character varying' => 'Titon\Model\Driver\Type\StringType',
			'varchar' => 'Titon\Model\Driver\Type\StringType',
			// cidr
			// circle
			'date' => 'Titon\Model\Driver\Type\DateType',
			'double precision' => 'Titon\Model\Driver\Type\DoubleType',
			'float8' => 'Titon\Model\Driver\Type\DoubleType',
			// inet
			'int' => 'Titon\Model\Driver\Type\IntType',
			'int4' => 'Titon\Model\Driver\Type\IntType',
			'integer' => 'Titon\Model\Driver\Type\IntType',
			// interval
			// line
			// lseg
			// macaddr
			// money
			'numeric' => 'Titon\Model\Driver\Type\DecimalType',
			'decimal' => 'Titon\Model\Driver\Type\DecimalType',
			// path
			// point
			// polygon
			'real' => 'Titon\Model\Driver\Type\FloatType',
			'float4' => 'Titon\Model\Driver\Type\FloatType',
			'smallint' => 'Titon\Model\Driver\Type\IntType',
			'int2' => 'Titon\Model\Driver\Type\IntType',
			// smallserial, serial2
			// serial, serial4
			'text' => 'Titon\Model\Driver\Type\TextType',
			'time' => 'Titon\Model\Driver\Type\TimeType',
			'timestamp' => 'Titon\Model\Driver\Type\DatetimeType',
			// tsquery
			// tsvector
			// txid_snapshot
			// uuid
			// xml
			// json
		];
	}

	/**
	 * {@inheritdoc}
	 */
	public function isEnabled() {
		return extension_loaded('pdo_pgsql');
	}

}