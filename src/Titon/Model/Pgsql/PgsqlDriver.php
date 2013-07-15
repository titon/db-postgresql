<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Pgsql;

use Titon\Model\Driver\AbstractPdoDriver;
use Titon\Model\Driver\Type\AbstractType;
use Titon\Model\Model;

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
	 *
	 * @uses Titon\Model\Type\AbstractType
	 */
	public function describeTable($table) {
		return $this->cache([__METHOD__, $table], function() use ($table) {
			$columns = $this->query('SELECT * FROM information_schema.columns WHERE table_catalog = ? AND table_schema = \'public\' AND table_name = ?;', [$this->getDatabase(), $table])->fetchAll(false);
			$schema = [];

			if (!$columns) {
				return $schema;
			}

			foreach ($columns as $column) {
				$field = $column['column_name'];
				$type = strtolower($column['data_type']);
				$length = $column['character_maximum_length'];

				// Determine type and length
				if (preg_match('/([a-z\s]+)(?:\(([0-9,]+)\))?/is', $type, $matches)) {
					$type = $matches[1];

					if (isset($matches[2])) {
						$length = $matches[2];
					}
				}

				// Inherit type defaults
				$data = AbstractType::factory($type, $this)->getDefaultOptions();

				// Overwrite with custom
				$data = [
					'field' => $field,
					'type' => $type,
					'length' => $length,
					'null' => ($column['is_nullable'] === 'YES'),
				] + $data;

				foreach ([
					'default' => 'column_default',
					'charset' => 'character_set_name',
					'collate' => 'collation_name'
				] as $key => $search) {
					if (!empty($column[$search])) {
						$data[$key] = $column[$search];
					}
				}

				if ($type === 'decimal' || $type === 'numeric') {
					$data['length'] = $column['numeric_precision'] . ',' . $column['numeric_scale'];
				}

				if (!empty($data['default']) && strpos($data['default'], '_seq') !== false) {
					$data['primary'] = true;
					$data['ai'] = true;
				}

				$schema[$field] = $data;
			}

			return $schema;
		});
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

		return $this->getDriver() . ':' . implode(';', [
			'dbname=' . $this->getDatabase(),
			'host=' . $this->getHost(),
			'port=' . $this->getPort()
		]);
	}

	/**
	 * {@inheritdoc}
	 */
	public function getLastInsertID(Model $model) {
		return $this->getConnection()->lastInsertId($model->getTable() . '_' . $model->getPrimaryKey() . '_seq');
	}

	/**
	 * {@inheritdoc}
	 */
	public function getSupportedTypes() {
		// TODO
		return [
			'bigint' => 'Titon\Model\Driver\Type\BigintType',
			'int8' => 'Titon\Model\Driver\Type\BigintType',
			'bigserial' => 'Titon\Model\Pgsql\Type\SerialType',
			'serial8' => 'Titon\Model\Pgsql\Type\SerialType',
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
			'cidr' => 'Titon\Model\Driver\Type\StringType',
			// circle
			'date' => 'Titon\Model\Driver\Type\DateType',
			'double precision' => 'Titon\Model\Driver\Type\DoubleType',
			'float8' => 'Titon\Model\Driver\Type\DoubleType',
			'inet' => 'Titon\Model\Driver\Type\StringType',
			'int' => 'Titon\Model\Driver\Type\IntType',
			'int4' => 'Titon\Model\Driver\Type\IntType',
			'integer' => 'Titon\Model\Driver\Type\IntType',
			// interval
			// line
			// lseg
			'macaddr' => 'Titon\Model\Driver\Type\StringType',
			'money' => 'Titon\Model\Driver\Type\DecimalType',
			'numeric' => 'Titon\Model\Driver\Type\DecimalType',
			'decimal' => 'Titon\Model\Driver\Type\DecimalType',
			// path
			// point
			// polygon
			'real' => 'Titon\Model\Driver\Type\FloatType',
			'float4' => 'Titon\Model\Driver\Type\FloatType',
			'smallint' => 'Titon\Model\Driver\Type\IntType',
			'int2' => 'Titon\Model\Driver\Type\IntType',
			'smallserial' => 'Titon\Model\Pgsql\Type\SerialType',
			'serial2' => 'Titon\Model\Pgsql\Type\SerialType',
			'serial' => 'Titon\Model\Pgsql\Type\SerialType',
			'serial4' => 'Titon\Model\Pgsql\Type\SerialType',
			'text' => 'Titon\Model\Driver\Type\TextType',
			'time' => 'Titon\Model\Driver\Type\TimeType',
			'time without time zone' => 'Titon\Model\Driver\Type\TimeType',
			'time with time zone' => 'Titon\Model\Pgsql\Type\TimeTzType',
			'timestamp' => 'Titon\Model\Driver\Type\DatetimeType',
			'timestamp without time zone' => 'Titon\Model\Driver\Type\DatetimeType',
			'timestamp with time zone' => 'Titon\Model\Pgsql\Type\DatetimeTzType',
			// tsquery
			// tsvector
			// txid_snapshot
			'uuid' => 'Titon\Model\Driver\Type\StringType',
			// xml
			// json
			// array
		];
	}

	/**
	 * {@inheritdoc}
	 */
	public function isEnabled() {
		return extension_loaded('pdo_pgsql');
	}

	/**
	 * {@inheritdoc}
	 */
	public function listTables($database = null) {
		$database = $database ?: $this->getDatabase();

		return $this->cache([__METHOD__, $database], function() use ($database) {
			$tables = $this->query('SELECT * FROM information_schema.tables WHERE table_schema = \'public\' AND table_catalog = ?;', [$database])->fetchAll(false);
			$schema = [];

			if (!$tables) {
				return $schema;
			}

			foreach ($tables as $table) {
				$schema[] = $table['table_name'];
			}

			return $schema;
		});
	}

}