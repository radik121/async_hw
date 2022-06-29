import asyncio
import asyncpg
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from config import PG_DSN_ALC

engine = create_async_engine(PG_DSN_ALC, echo=True)
Base = declarative_base()


class Contacts(Base):
    __tablename__ = 'contacts'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True)
    height = Column(String)
    mass = Column(String)
    hair_color = Column(String)
    skin_color = Column(String)
    eye_color = Column(String)
    birth_year = Column(String)
    gender = Column(String)
    homeworld = Column(String)
    films = Column(String)
    species = Column(String)
    vehicles = Column(String)
    starships = Column(String)


async def get_async_session(drop: bool = False, create: bool = False):
    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            await conn.run_sync(Base.metadata.create_all)

    async_session_maker = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    return async_session_maker


async def insert_data(pool: asyncpg.Pool, data_list):
    query = "INSERT INTO contacts (" \
            "name, height, mass, hair_color, skin_color, " \
            "eye_color, birth_year, gender, homeworld, films, species, vehicles, starships)" \
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, data_list)


async def main():
    await get_async_session(True, True)


if __name__ == '__main__':
    asyncio.run(main())
