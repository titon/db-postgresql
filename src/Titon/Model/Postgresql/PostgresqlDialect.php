<?php
/**
 * @copyright	Copyright 2010-2013, The Titon Project
 * @license		http://opensource.org/licenses/bsd-license.php
 * @link		http://titon.io
 */

namespace Titon\Model\Postgresql;

use Titon\Model\Driver\Dialect\AbstractDialect;
use Titon\Model\Query;

/**
 * Inherit the default dialect rules and override for PostgreSQL specific syntax.
 *
 * @package Titon\Model\Postgresql
 */
class PostgresqlDialect extends AbstractDialect {

	const DELETE_ROWS = 'deleteRows';
	const DROP = 'drop';
	const INHERITS = 'inherits';
	const IS_GLOBAL = 'global';
	const LOCAL = 'local';
	const ON_COMMIT = 'onCommit';
	const ONLY = 'only';
	const PRESERVE_ROWS = 'preserveRows';
	const RETURNING = 'returning';
	const TABLESPACE = 'tablespace';
	const WITH = 'with';
	const WITH_OIDS = 'withOids';
	const WITHOUT_OIDS = 'withoutOids';

	/**
	 * List of full SQL statements.
	 *
	 * @type array
	 */
	protected $_statements = [
		Query::INSERT		=> 'INSERT INTO {table} {fields} VALUES {values} {a.returning}',
		Query::SELECT		=> 'SELECT {a.distinct} {fields} FROM {table} {joins} {where} {groupBy} {having} {orderBy} {limit}',
		Query::UPDATE		=> 'UPDATE {a.only} {table} SET {fields} {joins} {where} {a.returning}',
		Query::DELETE		=> 'DELETE FROM {a.only} {table} {joins} {where} {a.returning}',
		Query::TRUNCATE		=> 'TRUNCATE {table} {a.action}',
		Query::DROP_TABLE	=> 'DROP TABLE IF EXISTS {table} {a.action}',
		Query::CREATE_TABLE	=> "CREATE {a.type} {a.temporary} TABLE {table} (\n{columns}{keys}\n) {options}"
	];

	/**
	 * Available attributes for each query type.
	 *
	 * @type array
	 */
	protected $_attributes = [
		Query::INSERT => [
			'returning' => ''
		],
		Query::SELECT => [
			'distinct' => false
		],
		Query::UPDATE => [
			'only' => false,
			'returning' => ''
		],
		Query::DELETE => [
			'only' => false,
			'returning' => ''
		],
		Query::TRUNCATE => [
			'action' => '' // cascade, restrict
		],
		Query::DROP_TABLE => [
			'action' => ''
		],
		Query::CREATE_TABLE => [
			'type' => '',
			'temporary' => false
		],
	];

	/**
	 * Modify clauses and keywords.
	 */
	public function initialize() {
		parent::initialize();

		$this->_clauses[self::RETURNING] = 'RETURNING %s';

		$this->_keywords[self::DELETE_ROWS] = 'DELETE ROWS';
		$this->_keywords[self::DROP] = 'DROP';
		$this->_keywords[self::INHERITS] = 'INHERITS';
		$this->_keywords[self::IS_GLOBAL] = 'GLOBAL';
		$this->_keywords[self::LOCAL] = 'LOCAL';
		$this->_keywords[self::ON_COMMIT] = 'ON COMMIT';
		$this->_keywords[self::ONLY] = 'ONLY';
		$this->_keywords[self::PRESERVE_ROWS] = 'PRESERVE ROWS';
		$this->_keywords[self::TABLESPACE] = 'TABLESPACE';
		$this->_keywords[self::WITH] = 'WITH';
		$this->_keywords[self::WITH_OIDS] = 'WITH OIDS';
		$this->_keywords[self::WITHOUT_OIDS] = 'WITHOUT OIDS';
	}

}