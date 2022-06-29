import asyncio
import aiohttp
from more_itertools import chunked

URL = 'https://swapi.dev/api/people/'

MAX = 100
PARTITION = 10
SLEEP_TIME = 1


async def health_check(session):
    try:
        async with session.get('https://swapi.dev/api/') as response:
            if response.status == 200:
                print('OK')
            else:
                print(response.status)
    except Exception as er:
        print(er)
    await asyncio.sleep(1)


async def get_person(person_id, session):
    async with session.get(f'{URL}{person_id}') as response:
        return await response.json()


async def get_people(all_ids, partition, session):
    for chunk_ids in chunked(all_ids, partition):
        tasks = [asyncio.create_task(get_person(person_id, session)) for person_id in chunk_ids]
        for task in tasks:
            task_result = await task
            yield task_result


async def main():
    async with aiohttp.ClientSession() as session:
        health_check_task = asyncio.create_task(health_check(session))
        print(health_check_task)
        async for people in get_people(range(1, MAX + 1), PARTITION, session):
            if len(people) > 1:
                print(people)


if __name__ == '__main__':
    asyncio.run(main())
