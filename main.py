import aiohttp
import asyncpg

from config import PG_DSN
from json_data import get_people, health_check
from pg_table import get_async_session, insert_data
import asyncio
from more_itertools import chunked

MAX = 100
PARTITION = 10


async def main():
    await get_async_session(True, True)
    p_list = []
    tasks = []
    pool = await asyncpg.create_pool(PG_DSN, min_size=20, max_size=20)
    async with aiohttp.ClientSession() as session:
        health_check_task = asyncio.create_task(health_check(session))
        print(health_check_task)
        async for people in get_people(range(1, MAX + 1), PARTITION, session):
            if len(people) > 1:
                people['films'] = ', '.join(people['films'])
                people['species'] = ', '.join(people['species'])
                people['vehicles'] = ', '.join(people['vehicles'])
                people['starships'] = ', '.join(people['starships'])
                p_list.append(list(people.values())[:13])
                print(people)

    for p_chunk in chunked(p_list, 20):
        tasks.append(asyncio.create_task(insert_data(pool, p_chunk)))

    await asyncio.gather(*tasks)
    await pool.close()
    print('Data upload to database completed successfully')

if __name__ == '__main__':
    asyncio.run(main())