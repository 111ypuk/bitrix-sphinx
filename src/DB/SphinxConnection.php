<?php

namespace Olegpro\BitrixSphinx\DB;

use Bitrix\Main\DB\MysqliConnection;

class SphinxConnection extends MysqliConnection
{
    protected function createSqlHelper(): SphinxSqlHelper
    {
        return new SphinxSqlHelper($this);
    }
}
