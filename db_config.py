from sqlalchemy import Column, Integer, String, ForeignKey, Text, JSON, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Users(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    context = Column(JSON)
    tables = relationship("Tables", back_populates="user")


class Tables(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True)
    userid = Column(Integer, ForeignKey("users.id"))
    table_name = Column(String)

    __table_args__ = (UniqueConstraint("userid", "table_name"),)

    user = relationship("Users", back_populates="tables")
    columns = relationship("Columns", back_populates="table")


class Columns(Base):
    __tablename__ = "columns"

    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey("tables.id"))
    column_name = Column(String)
    type = Column(String)

    __table_args__ = (UniqueConstraint("table_id", "column_name"),)

    table = relationship("Tables", back_populates="columns")
    data = relationship("Data", back_populates="column")


class Data(Base):
    __tablename__ = "data"

    row_id = Column(Integer)
    column_id = Column(Integer, ForeignKey("columns.id"))
    data = Column(String)

    __table_args__ = (PrimaryKeyConstraint("row_id", "column_id"),)

    column = relationship("Columns", back_populates="data")


class Migration:
    @staticmethod
    async def up(engine):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @staticmethod
    async def down(engine):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)


async def drop_tables():
    engine = create_async_engine("sqlite+aiosqlite:///./test.db", echo=True)
    await Migration.down(engine)


if __name__ == '__main__':
    import asyncio
    asyncio.run(drop_tables())