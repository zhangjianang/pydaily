#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlparse

sql="replace into company (name,age) values ('ang',18)"
parsed=sqlparse.parse(sql)[0]
for item in parsed.tokens:
    print item
# sqlparse.sql.Token.match(sql,)