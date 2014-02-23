<?php
/**
 * @copyright   2010-2013, The Titon Project
 * @license     http://opensource.org/licenses/bsd-license.php
 * @link        http://titon.io
 */

namespace Titon\Db\Pgsql;

use Titon\Db\Driver\Dialect\AbstractPdoDialect;
use Titon\Db\Driver\Schema;
use Titon\Db\Driver\Type\AbstractType;
use Titon\Db\Query;
use Titon\Db\Query\Expr;
use Titon\Db\Query\Func;
use Titon\Db\Query\RawExpr;
use Titon\Db\Query\SubQuery;

/**
 * Inherit the default dialect rules and override for PostgreSQL specific syntax.
 *
 * @package Titon\Db\Pgsql
 */
class PgsqlDialect extends AbstractPdoDialect {

    const CONCURRENTLY = 'concurrently';
    const CONTINUE_IDENTITY = 'continueIdentity';
    const DELETE_ROWS = 'deleteRows';
    const DISTINCT_ON = 'distinctOn';
    const DROP = 'drop';
    const FOR_UPDATE_LOCK = 'forUpdateLock';
    const FOR_SHARE_LOCK = 'forShareLock';
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
    const UNIQUE = 'unique';
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
        Query::INSERT           => 'INSERT INTO {table} {fields} VALUES {values}',
        Query::SELECT           => 'SELECT {a.distinct} {fields} FROM {table} {joins} {where} {groupBy} {having} {compounds} {orderBy} {limit} {a.lock}',
        Query::UPDATE           => 'UPDATE {a.only} {table} SET {fields} {where}',
        Query::DELETE           => 'DELETE FROM {a.only} {table} {joins} {where}',
        Query::TRUNCATE         => 'TRUNCATE {a.only} {table} {a.identity} {a.action}',
        Query::CREATE_TABLE     => "CREATE {a.type} {a.temporary} {a.unlogged} TABLE IF NOT EXISTS {table} (\n{columns}{keys}\n) {options}",
        Query::CREATE_INDEX     => 'CREATE {a.type} INDEX {a.concurrently} {index} ON {table} ({fields})',
        Query::DROP_TABLE       => 'DROP TABLE IF EXISTS {table} {a.action}',
        Query::DROP_INDEX       => 'DROP INDEX {a.concurrently} IF EXISTS {index} {a.action}',
    ];

    /**
     * Available attributes for each query type.
     *
     * @type array
     */
    protected $_attributes = [
        Query::SELECT => [
            'distinct' => false,
            'lock' => ''
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
        Query::CREATE_TABLE => [
            'type' => '',
            'temporary' => false,
            'unlogged' => false
        ],
        Query::CREATE_INDEX => [
            'type' => '', // unique
            'concurrently' => false
        ],
        Query::DROP_TABLE => [
            'action' => '' // cascade, restrict
        ],
        Query::DROP_INDEX => [
            'concurrently' => false,
            'action' => '' // cascade, restrict
        ],
    ];

    /**
     * Modify clauses and keywords.
     */
    public function initialize() {
        parent::initialize();

        $this->_clauses = array_replace($this->_clauses, [
            self::DISTINCT_ON   => 'DISTINCT ON (%s)',
            self::JOIN_STRAIGHT => 'INNER JOIN %s ON %s',
            self::MATCH         => '%s',
            self::NOT_REGEXP    => '%s !~* ?',
            self::RETURNING     => 'RETURNING %s',
            self::REGEXP        => '%s ~* ?',
            self::RLIKE         => '%s ~* ?',
            self::UNIQUE_KEY    => 'UNIQUE (%2$s)',
            self::WITH          => '%s'
        ]);

        $this->_keywords = array_replace($this->_keywords, [
            self::CONCURRENTLY      => 'CONCURRENTLY',
            self::CONTINUE_IDENTITY => 'CONTINUE IDENTITY',
            self::DELETE_ROWS       => 'DELETE ROWS',
            self::DROP              => 'DROP',
            self::FOR_SHARE_LOCK    => 'FOR SHARE',
            self::FOR_UPDATE_LOCK   => 'FOR UPDATE',
            self::INHERITS          => 'INHERITS',
            self::IS_GLOBAL         => 'GLOBAL',
            self::LOCAL             => 'LOCAL',
            self::MATCH_FULL        => 'MATCH FULL',
            self::MATCH_PARTIAL     => 'MATCH PARTIAL',
            self::MATCH_SIMPLE      => 'MATCH SIMPLE',
            self::ON_COMMIT         => 'ON COMMIT',
            self::ONLY              => 'ONLY',
            self::PRESERVE_ROWS     => 'PRESERVE ROWS',
            self::RESTART_IDENTITY  => 'RESTART IDENTITY',
            self::SET_DEFAULT       => 'SET DEFAULT',
            self::TABLESPACE        => 'TABLESPACE',
            self::UNIQUE            => 'UNIQUE',
            self::UNLOGGED          => 'UNLOGGED',
            self::WITH_OIDS         => 'WITH OIDS',
            self::WITHOUT_OIDS      => 'WITHOUT OIDS'
        ]);
    }

    /**
     * {@inheritdoc}
     */
    public function formatColumns(Schema $schema) {
        $columns = [];

        foreach ($schema->getColumns() as $column => $options) {
            $dataType = AbstractType::factory($options['type'], $this->getDriver());

            $options = $options + $dataType->getDefaultOptions();
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

            // Primary and uniques can't be null
            if (!empty($options['primary']) || !empty($options['unique'])) {
                $output[] = $this->getKeyword(self::NOT_NULL);
            } else {
                $output[] = $this->getKeyword(empty($options['null']) ? self::NOT_NULL : self::NULL);
            }

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
    public function formatSelectFields(array $fields, $alias = null) {
        $columns = [];

        if (empty($fields)) {
            $columns[] = ($alias ? $this->quote($alias) . '.' : '') . '*';

        } else {
            foreach ($fields as $field) {
                if ($field instanceof Func) {
                    $columns[] = $this->formatFunction($field);

                } else if ($field instanceof Expr) {
                    $columns[] = $this->formatExpression($field);

                } else if ($field instanceof RawExpr) {
                    $columns[] = $field->getValue();

                } else if ($field instanceof SubQuery) {
                    $columns[] = $this->formatSubQuery($field);

                } else if (preg_match('/^(.*?)\s+AS\s+(.*?)$/i', $field, $matches)) {
                    $columns[] = sprintf($this->getClause(self::AS_ALIAS), $alias . $this->quote($matches[1]), $this->quote($matches[2]));

                // Alias the field since PgSQL doesn't support PDO::getColumnMeta()
                } else if ($alias) {
                    $columns[] = sprintf($this->getClause(self::AS_ALIAS), $this->quote($alias) . '.' . $this->quote($field), $alias . '__' . $field);

                } else {
                    $columns[] = $this->quote($field);
                }
            }
        }

        return $columns;
    }

}