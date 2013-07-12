<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Pgsql;

use Titon\Model\Driver\Dialect\AbstractDialect;
use Titon\Model\Driver\Schema;
use Titon\Model\Driver\Type\AbstractType;
use Titon\Model\Query;

/**
 * Inherit the default dialect rules and override for PostgreSQL specific syntax.
 *
 * @package Titon\Model\Pgsql
 */
class PgsqlDialect extends AbstractDialect {

	const CONTINUE_IDENTITY = 'continueIdentity';
	const DELETE_ROWS = 'deleteRows';
	const DROP = 'drop';
	const INHERITS = 'inherits';
	const IS_GLOBAL = 'global';
	const LOCAL = 'local';
	const MATCH = 'match';
	const MATCH_FULL = 'matchFull';
	const MATCH_PARTIAL = 'matchPartial';
	const MATCH_SIMPLE = 'matchSimple';
	const ON_COMMIT = 'onCommit';
	const ONLY = 'only';
	const PRESERVE_ROWS = 'preserveRows';
	const RESTART_IDENTITY = 'restartIdentity';
	const RETURNING = 'returning';
	const SET_DEFAULT = 'setDefault';
	const TABLESPACE = 'tablespace';
	const UNLOGGED = 'unlogged';
	const WITH = 'with';
	const WITH_OIDS = 'withOids';
	const WITHOUT_OIDS = 'withoutOids';

	/**
	 * Configuration.
	 *
	 * @type array
	 */
	protected $_config = [
		'quoteCharacter' => '"'
	];

	/**
	 * List of full SQL statements.
	 *
	 * @type array
	 */
	protected $_statements = [
		Query::INSERT		=> 'INSERT INTO {table} {fields} VALUES {values}',
		Query::SELECT		=> 'SELECT {a.distinct} {fields} FROM {table} {joins} {where} {groupBy} {having} {orderBy} {limit}',
		Query::UPDATE		=> 'UPDATE {a.only} {table} SET {fields} {where}',
		Query::DELETE		=> 'DELETE FROM {a.only} {table} {joins} {where}',
		Query::TRUNCATE		=> 'TRUNCATE {a.only} {table} {a.identity} {a.action}',
		Query::DROP_TABLE	=> 'DROP TABLE IF EXISTS {table} {a.action}',
		Query::CREATE_TABLE	=> "CREATE {a.type} {a.temporary} {a.unlogged} TABLE IF NOT EXISTS {table} (\n{columns}{keys}\n) {options}"
	];

	/**
	 * Available attributes for each query type.
	 *
	 * @type array
	 */
	protected $_attributes = [
		Query::INSERT => [],
		Query::SELECT => [
			'distinct' => false
		],
		Query::UPDATE => [
			'only' => false
		],
		Query::DELETE => [
			'only' => false
		],
		Query::TRUNCATE => [
			'only' => false,
			'identity' => '', // restart, continue
			'action' => '' // cascade, restrict
		],
		Query::DROP_TABLE => [
			'action' => ''
		],
		Query::CREATE_TABLE => [
			'type' => '',
			'temporary' => false,
			'unlogged' => false
		],
	];

	/**
	 * Modify clauses and keywords.
	 */
	public function initialize() {
		parent::initialize();

		$this->_clauses = array_replace($this->_clauses, [
			self::MATCH				=> '%s',
			self::RETURNING			=> 'RETURNING %s',
			self::UNIQUE_KEY		=> 'UNIQUE (%2$s)',
			self::WITH				=> '%s'
		]);

		$this->_keywords = array_replace($this->_keywords, [
			self::CONTINUE_IDENTITY	=> 'CONTINUE IDENTITY',
			self::DELETE_ROWS 		=> 'DELETE ROWS',
			self::DROP				=> 'DROP',
			self::INHERITS			=> 'INHERITS',
			self::IS_GLOBAL			=> 'GLOBAL',
			self::LOCAL				=> 'LOCAL',
			self::MATCH_FULL		=> 'MATCH FULL',
			self::MATCH_PARTIAL		=> 'MATCH PARTIAL',
			self::MATCH_SIMPLE		=> 'MATCH SIMPLE',
			self::ON_COMMIT			=> 'ON COMMIT',
			self::ONLY				=> 'ONLY',
			self::PRESERVE_ROWS		=> 'PRESERVE ROWS',
			self::RESTART_IDENTITY	=> 'RESTART IDENTITY',
			self::SET_DEFAULT		=> 'SET DEFAULT',
			self::TABLESPACE		=> 'TABLESPACE',
			self::UNLOGGED			=> 'UNLOGGED',
			self::WITH_OIDS			=> 'WITH OIDS',
			self::WITHOUT_OIDS		=> 'WITHOUT OIDS'
		]);
	}

	/**
	 * {@inheritdoc}
	 */
	public function buildTruncate(Query $query) {
		$params = $this->renderAttributes($query->getAttributes() + $this->getAttributes(Query::TRUNCATE));
		$params = $params + [
			'table' => $this->formatTable($query->getTable())
		];

		return $this->renderStatement($this->getStatement(Query::TRUNCATE), $params);
	}

	/**
	 * Map between MySQL and PgSQL types.
	 *
	 * @param string $type
	 * @return string
	 */
	public function mapType($type) {
		switch ($type) {
			case 'tinyint':
				$type = 'smallint';
			break;
			case 'double':
				$type = 'double precision';
			break;
			case 'blob':
			case 'tinyblob':
			case 'mediumblob':
			case 'longblob':
				$type = 'bytea';
			break;
			case 'datetime':
				$type = 'timestamp';
			break;
		}

		return $type;
	}

	/**
	 * {@inheritdoc}
	 */
	public function formatColumns(Schema $schema) {
		$columns = [];

		foreach ($schema->getColumns() as $column => $options) {
			$baseType = $this->mapType($options['type']);
			$dataType = AbstractType::factory($baseType, $this->getDriver());

			$options = $dataType->getDefaultOptions() + $options;
			$options['type'] = $baseType;
			$type = $options['type'];

			if ($type === 'int') {
				$type = 'integer';
			}

			if (!empty($options['length'])) {
				$type .= '(' . $options['length'] . ')';
			}

			$output = [$this->quote($column), $type];

			if (!empty($options['collate'])) {
				$output[] = sprintf($this->getClause(self::COLLATE), $options['collate']);
			}

			if (!empty($options['constraint'])) {
				$output[] = sprintf($this->getClause(self::CONSTRAINT), $this->quote($options['constraint']));
			}

			$output[] = $this->getKeyword(empty($options['null']) ? self::NOT_NULL : self::NULL);

			if (array_key_exists('default', $options)) {
				$output[] = $this->formatDefault($options['default']);
			}

			$columns[] = trim(implode(' ', $output));
		}

		return implode(",\n", $columns);
	}

	/**
	 * {@inheritdoc}
	 */
	public function formatTableIndex($index, array $columns) {
		return ''; // PGSQL does not support indices within a CREATE TABLE statement
	}

}