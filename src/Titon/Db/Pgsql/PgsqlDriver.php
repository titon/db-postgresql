<?php
/**
 * @copyright   2010-2014, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql;

use Titon\Db\Driver\AbstractPdoDriver;
use Titon\Db\Repository;

/**
 * A driver that represents the PostgreSQL database and uses PDO.
 *
 * @package Titon\Db\Pgsql
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
     * @uses Titon\Db\Type\AbstractType
     */
    public function describeTable($table) {
        return $this->cacheQuery([__METHOD__, $table], function(PgsqlDriver $driver) use ($table) {
            $columns = $driver->executeQuery('SELECT * FROM information_schema.columns WHERE table_catalog = ? AND table_schema = ? AND table_name = ?;', [$driver->getDatabase(), 'public', $table])->find();
            $schema = [];

            if (!$columns) {
                return $schema;
            }

            foreach ($columns as $column) {
                $field = $column['column_name'];
                $type = strtolower($column['data_type']);
                $length = $column['character_maximum_length'];

                // Inherit type defaults
                $data = $driver->getType($type)->getDefaultOptions();

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
        }, '+1 year');
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
        if ($dsn = $this->getConfig('dsn')) {
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
    public function getLastInsertID(Repository $table) {
        return $this->getConnection()->lastInsertId($table->getTable() . '_' . $table->getPrimaryKey() . '_seq');
    }

    /**
     * {@inheritdoc}
     * @codeCoverageIgnore
     */
    public function getSupportedTypes() {
        // TODO
        return [
            'bigint' => 'Titon\Db\Driver\Type\BigintType',
            'int8' => 'Titon\Db\Driver\Type\BigintType',
            'bigserial' => 'Titon\Db\Pgsql\Type\SerialType',
            'serial8' => 'Titon\Db\Pgsql\Type\SerialType',
            'bit' => 'Titon\Db\Driver\Type\BinaryType',
            'bit varying' => 'Titon\Db\Driver\Type\BinaryType',
            'varbit' => 'Titon\Db\Driver\Type\BinaryType',
            'bool' => 'Titon\Db\Driver\Type\BooleanType',
            'boolean' => 'Titon\Db\Driver\Type\BooleanType',
            // box
            'bytea' => 'Titon\Db\Driver\Type\BlobType',
            'char' => 'Titon\Db\Driver\Type\CharType',
            'character' => 'Titon\Db\Driver\Type\CharType',
            'character varying' => 'Titon\Db\Driver\Type\StringType',
            'varchar' => 'Titon\Db\Driver\Type\StringType',
            'cidr' => 'Titon\Db\Driver\Type\StringType',
            // circle
            'date' => 'Titon\Db\Driver\Type\DateType',
            'double precision' => 'Titon\Db\Driver\Type\DoubleType',
            'float8' => 'Titon\Db\Driver\Type\DoubleType',
            'inet' => 'Titon\Db\Driver\Type\StringType',
            'int' => 'Titon\Db\Driver\Type\IntType',
            'int4' => 'Titon\Db\Driver\Type\IntType',
            'integer' => 'Titon\Db\Driver\Type\IntType',
            // interval
            // line
            // lseg
            'macaddr' => 'Titon\Db\Driver\Type\StringType',
            'money' => 'Titon\Db\Driver\Type\DecimalType',
            'numeric' => 'Titon\Db\Driver\Type\DecimalType',
            'decimal' => 'Titon\Db\Driver\Type\DecimalType',
            // path
            // point
            // polygon
            'real' => 'Titon\Db\Driver\Type\FloatType',
            'float4' => 'Titon\Db\Driver\Type\FloatType',
            'smallint' => 'Titon\Db\Driver\Type\IntType',
            'int2' => 'Titon\Db\Driver\Type\IntType',
            'smallserial' => 'Titon\Db\Pgsql\Type\SerialType',
            'serial2' => 'Titon\Db\Pgsql\Type\SerialType',
            'serial' => 'Titon\Db\Pgsql\Type\SerialType',
            'serial4' => 'Titon\Db\Pgsql\Type\SerialType',
            'text' => 'Titon\Db\Driver\Type\TextType',
            'time' => 'Titon\Db\Driver\Type\TimeType',
            'time without time zone' => 'Titon\Db\Driver\Type\TimeType',
            'time with time zone' => 'Titon\Db\Pgsql\Type\TimeTzType',
            'timestamp' => 'Titon\Db\Driver\Type\DatetimeType',
            'timestamp without time zone' => 'Titon\Db\Driver\Type\DatetimeType',
            'timestamp with time zone' => 'Titon\Db\Pgsql\Type\DatetimeTzType',
            // tsquery
            // tsvector
            // txid_snapshot
            'uuid' => 'Titon\Db\Driver\Type\StringType',
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

        return $this->cacheQuery([__METHOD__, $database], function(PgsqlDriver $driver) use ($database) {
            $tables = $driver->executeQuery('SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_catalog = ?;', ['public', $database])->find();
            $schema = [];

            if (!$tables) {
                return $schema;
            }

            foreach ($tables as $table) {
                $schema[] = $table['table_name'];
            }

            return $schema;
        }, '+1 year');
    }

    /**
     * {@inheritdoc}
     */
    public function newQuery($string) {
        return new PgsqlQuery($string);
    }

}