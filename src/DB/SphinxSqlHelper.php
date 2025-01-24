<?php

namespace Olegpro\BitrixSphinx\DB;

use Bitrix\Main\DB\MysqliSqlHelper;

class SphinxSqlHelper extends MysqliSqlHelper
{
    /**
     * Escapes special characters in a string for use in match.
     *
     * @param string $value Value to be escaped.
     */
    public function escape(string $value): string
    {
        static $search = [
            "\\",
            "'",
            "*",
            "/",
            ")",
            "(",
            "$",
            "~",
            "!",
            "@",
            "^",
            "-",
            "|",
            "<",
            "\x0",
            "=",
        ];

        static $replace = [
            "\\\\",
            "\\'",
            "\\\\\\\\*",
            "\\\\/",
            "\\\\)",
            "\\\\(",
            "\\\\\$",
            "\\\\~",
            "\\\\!",
            "\\\\@",
            "\\\\^",
            "\\\\-",
            "\\\\|",
            "\\\\<",
            " ",
            " ",
        ];

        $value = str_replace($search, $replace, $value);

        $stat = count_chars($value, 1);

        if (
            isset($stat[ord('"')])
            && $stat[ord('"')] % 2 === 1
        ) {
            $value = str_replace('"', '\\\"', $value);
        }

        return $value;
    }

}
