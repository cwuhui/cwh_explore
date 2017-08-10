# coding:utf-8
'''
Created on 2017年6月15日

@author: Administrator
'''
import asyncio
import logging
from macpath import curdir

import aiomysql


async def create_pool(loop, **kw):
    logging.info("create database from connection pool...")
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minszie=kw.get('minsize', 1),
        loop=loop
    )


async def select(sql, args, size=None):
    from cgi import log
    '''输出sql语句'''
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned {}'.format(len(rs)))
        return rs

