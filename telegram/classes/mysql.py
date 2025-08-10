# telegram/classes/mysql.py
# Класс для работы с базой данных MySQL

import asyncmy

class MySQL:
    
    def __init__(self, main):
        
        self.main = main

    async def connect(self):
        return await asyncmy.connect(
            host=self.main.env.get('MYSQL_HOST'),
            user=self.main.env.get('MYSQL_USER'),
            password=self.main.env.get('MYSQL_PASSWORD'),
            database=self.main.env.get('MYSQL_DB')
        )

    async def execute(self, sql: str, params: tuple = None, fetch: str = None):
        """
        :param sql: SQL-запрос
        :param params: параметры запроса
        :param fetch: None | 'one' | 'all'
        :return: результат выборки или None
        """
        conn = await self.connect()
        async with conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, params or ())

                if fetch == 'one':
                    return await cursor.fetchone()
                elif fetch == 'all':
                    return await cursor.fetchall()

            await conn.commit()
        return None

    async def fetch_all(self, query, params=None):
        conn = await self.connect()
        async with conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchall()
                return result

    async def fetch_one(self, query, params=None):
        conn = await self.connect()
        async with conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchone()
                return result