<?php

namespace Olegpro\BitrixSphinx\Entity;

use Bitrix\Main\Entity\Base;
use Bitrix\Main\Entity\Query;
use Bitrix\Main;
use Bitrix\Main\Entity\QueryChain;
use Olegpro\BitrixSphinx\DB\SphinxConnection;
use Olegpro\BitrixSphinx\DB\SphinxSqlHelper;
use Bitrix\Main\NotSupportedException;
use Bitrix\Main\Config\Configuration;
use Bitrix\Main\Application;

/**
 * @property Base $entity
 */
class SphinxQuery extends Query
{
    private bool $disableEscapeMatch = false;
    private ?bool $useConnectionMasterOnly = null;
    private bool $disableQuoteAliasSelect = false;
    protected array|string $match = [];
    protected array $option = [];

    /**
     * For disable table alias
     *
     * @var string
     */
    protected $custom_base_table_alias = '';

    /**
     * @throws Main\ArgumentException
     */
    public function __construct(Base|Query|string $source)
    {
        parent::__construct($source);

        $settingsBitrixSphinx = Configuration::getValue('olegpro_bitrix_sphinx');

        if (
            is_array($settingsBitrixSphinx)
            && isset($settingsBitrixSphinx['disable_quite_alias_select'])
            && is_bool($settingsBitrixSphinx['disable_quite_alias_select'])
        ) {
            $this->disableQuoteAliasSelect = $settingsBitrixSphinx['disable_quite_alias_select'];
        }
    }

    /**
     * Sets a list of fields for SELECT clause
     */
    public function setSelect(array $select): static
    {
        return parent::setSelect($select);
    }

    /**
     * Sets a list of filters for WHERE clause
     */
    public function setFilter(array $filter): static
    {
        return parent::setFilter($filter);
    }

    /**
     * Sets a limit for LIMIT n clause
     *
     * @param int $limit
     */
    public function setLimit($limit): static
    {
        return parent::setLimit($limit);
    }

    /**
     * Sets an offset for LIMIT n, m clause
     *
     * @param int $offset
     */
    public function setOffset($offset): static
    {
        return parent::setOffset($offset);
    }

    /**
     * @param bool|null $count
     */
    public function countTotal($count = null): static|null
    {
        return parent::countTotal($count);
    }

    /**
     * Sets a list of fields for ORDER BY clause
     *
     * @param mixed $order
     */
    public function setOrder($order): static
    {
        return parent::setOrder($order);
    }

    /**
     * Sets a list of fileds in GROUP BY clause
     *
     * @param mixed $group
     */
    public function setGroup($group): static
    {
        return parent::setGroup($group);
    }

    /**
     * @param array|string $match
     *
     * @return SphinxQuery
     * @throws Main\ArgumentException
     */
    public function setMatch(array|string $match): static
    {
        $this->match = $match;

        return $this;
    }

    /**
     * Sets a list of fields for OPTION clause
     *
     * @throws Main\ArgumentException
     */
    public function setOption(array $option): static
    {
        $this->option = $option;

        return $this;
    }

    /**
     * @throws Main\SystemException
     */
    protected function buildSelect(): string
    {
        $sql = [];

        foreach ($this->select_chains as $chain) {
            $sql[] = $this->getSqlDefinitionSelect(
                $chain,
                ($chain->getLastElement()->getValue()->getColumnName() !== 'id'),
            );
        }

        if (empty($sql)) {
            $sql[] = 1;
        }

        $sql = "\n\t" . implode(",\n\t", $sql);

        return $sql;
    }

    /**
     * @return mixed|string
     * @throws Main\SystemException
     */
    private function getSqlDefinitionSelect(QueryChain $chain, bool $withAlias = false)
    {
        $sqlDef = $chain->getLastElement()->getSqlDefinition();

        if ($withAlias) {
            $helper = $chain->getLastElement()->getValue()->getEntity()->getConnection()->getSqlHelper();
            $sqlDef .= ' AS ' . ($this->isDisableQuoteAliasSelect() ? $chain->getAlias() : $helper->quote($chain->getAlias()));
        }

        return $sqlDef;
    }

    protected function buildWhere(): string
    {
        $sql = parent::buildWhere();

        /** @var SphinxConnection $connection */
        $connection = $this->entity->getConnection();

        /** @var SphinxSqlHelper $helper */
        $helper = $connection->getSqlHelper();

        if (!empty($this->match)) {
            $match = is_array($this->match) ? reset($this->match) : $this->match;

            $match = trim($match);

            if (!empty($match)) {
                $sql = sprintf(
                    (!empty($sql) ? "MATCH('%s')\nAND %s" : "MATCH('%s')"),
                    $this->isDisableEscapeMatch() ? $match : $helper->escape($match),
                    $sql,
                );
            }
        }

        return $sql;
    }

    protected function buildOption(): string
    {
        $connection = $this->entity->getConnection();

        $helper = $connection->getSqlHelper();

        $sql = [];

        foreach ($this->option as $key => $value) {
            $sql[] = sprintf('%s = %s', $helper->forSql($key), ($value));
        }

        return implode(', ', $sql);
    }

    protected function buildOrder(): string
    {
        $sql = [];

        foreach ($this->order_chains as $chain) {
            //Рандомная сортировка при использовании
            //registerRuntimeField('RAND', new \Bitrix\Main\Entity\ExpressionField('RAND', 'RAND()'));
            if ($chain->getSqlDefinition() === 'RAND()') {
                $sql[] = $chain->getSqlDefinition();
            } else {
                $sort = $this->order[$chain->getDefinition()] ?? $this->order[$chain->getAlias()];
                $connection = $this->entity->getConnection();
                $helper = $connection->getSqlHelper();
                $sqlDefinition = $helper->quote($chain->getAlias());
                $sql[] = $sqlDefinition . ' ' . $sort;
            }
        }

        return implode(', ', $sql);
    }

    /**
     * @throws NotSupportedException
     */
    protected function buildJoin()
    {
        throw new NotSupportedException('Sphinx does not support joins');
    }

    protected function buildQuery($forceObjectPrimary = true)
    {
        $connection = $this->entity->getConnection();
        $helper = $connection->getSqlHelper();

        if ($this->query_build_parts === null) {
            foreach ($this->select as $key => $value) {
                $this->addToSelectChain($value, is_numeric($key) ? null : $key);
            }

            $this->setFilterChains($this->filter);
            $this->divideFilter();

            foreach ($this->group as $value) {
                $this->addToGroupChain($value);
            }

            foreach ($this->order as $key => $value) {
                $this->addToOrderChain($key);
            }

            $sqlSelect = $this->buildSelect();
            $sqlWhere = $this->buildWhere();
            $sqlGroup = $this->buildGroup();
            $sqlHaving = $this->buildHaving();
            $sqlOrder = $this->buildOrder();

            $sqlFrom = $this->quoteTableSource($this->entity->getDBTableName());

            $this->query_build_parts = array_filter([
                'SELECT' => $sqlSelect,
                'FROM' => $sqlFrom,
                'WHERE' => $sqlWhere,
                'GROUP BY' => $sqlGroup,
                'HAVING' => $sqlHaving,
                'ORDER BY' => $sqlOrder,
            ]);
        }

        $build_parts = $this->query_build_parts;

        foreach ($build_parts as $k => &$v) {
            $v = $k . ' ' . $v;
        }

        $query = implode("\n", $build_parts);

        [$query, $replaced] = $this->replaceSelectAliases($query);
        $this->replaced_aliases = $replaced;

        if ($this->limit > 0) {
            $query = $helper->getTopSql($query, $this->limit, $this->offset);
        }

        $sqlOption = $this->buildOption();

        if (!empty($sqlOption)) {
            $query = sprintf("%s\nOPTION %s", trim($query), $sqlOption);
        }

        // Fix empty artefacts empty table alias
        $query = str_replace(sprintf('%s.', $helper->getLeftQuote() . $helper->getLeftQuote()), '', $query);

        return $query;
    }

    /**
     * @param $query
     *
     * @return Main\DB\Result|null
     */
    protected function query($query)
    {
        $connection = $this->entity->getConnection();

        /** @var Main\DB\Result $result */
        $result = null;

        if ($result === null) {
            if ($this->isEnableConnectionMasterOnly()) {
                Application::getInstance()->getConnectionPool()->useMasterOnly(true);
            }

            $result = $connection->query($query);
            $result->setReplacedAliases($this->replaced_aliases);

            if ($this->countTotal) {
                $cnt = null;

                foreach ($connection->query('SHOW META;')->fetchAll() as $metaRow) {
                    if (
                        isset($metaRow['Variable_name'], $metaRow['Value'])
                        && $metaRow['Variable_name'] === 'total'
                    ) {
                        $cnt = (int)$metaRow['Value'];

                        break;
                    }
                }

                $result->setCount($cnt);
            }

            if ($this->isEnableConnectionMasterOnly()) {
                Application::getInstance()->getConnectionPool()->useMasterOnly(false);
            }

            static::$last_query = $query;
        }

        if ($this->isFetchModificationRequired()) {
            $result->addFetchDataModifier([$this, 'fetchDataModificationCallback']);
        }

        return $result;
    }

    /**
     * Set disableEscapeMatch enable flag
     */
    public function disableEscapeMatch(): static
    {
        $this->disableEscapeMatch = true;

        return $this;
    }

    /**
     * Set disableEscapeMatch enable flag
     */
    public function enableEscapeMatch(): static
    {
        $this->disableEscapeMatch = false;

        return $this;
    }

    public function isDisableEscapeMatch(): bool
    {
        return $this->disableEscapeMatch;
    }

    public function isEnableConnectionMasterOnly(): bool
    {
        $masterOnly = $this->useConnectionMasterOnly;

        if ($masterOnly === null) {
            $settingsBitrixSphinx = Configuration::getValue('olegpro_bitrix_sphinx');

            if (is_array($settingsBitrixSphinx) && isset($settingsBitrixSphinx['use_connection_master_only'])) {
                $masterOnly = $settingsBitrixSphinx['use_connection_master_only'];
            }
        }

        return ($masterOnly === true);
    }

    public function disableConnectionMasterOnly(): static
    {
        $this->useConnectionMasterOnly = false;

        return $this;
    }

    public function enableConnectionMasterOnly(): static
    {
        $this->useConnectionMasterOnly = true;

        return $this;
    }

    public function isDisableQuoteAliasSelect(): bool
    {
        return $this->disableQuoteAliasSelect;
    }

    public function disableQuoteAliasSelect(): static
    {
        $this->disableQuoteAliasSelect = true;

        return $this;
    }

    public function enableQuoteAliasSelect(): static
    {
        $this->disableQuoteAliasSelect = false;

        return $this;
    }
}
