from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, update, delete, func

from db_config import Users, Tables, Columns, Data
from typing import Optional, Dict, List


class CRUD:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def _exists(self, model, record_id: int) -> bool:
        result = await self.session.execute(select(model).where(model.id == record_id))
        return result.scalar_one_or_none() is not None

    async def _user_owned(self, model, user_id: int, record_id: int) -> bool:
        if model == Columns:
            stmt = select(Columns.table_id).where(Columns.id == record_id)
            result = await self.session.execute(stmt)
            table_id = result.scalar_one_or_none()
            if table_id is None:
                return False

            return await self._user_owned(Tables, user_id, table_id)

        elif model == Tables:
            stmt = select(Tables.userid).where(Tables.id == record_id)
            result = await self.session.execute(stmt)
            owner_id = result.scalar_one_or_none()
            return owner_id == user_id

        return False

    async def _is_unique(self, model, field_name: str, value, **kwargs) -> bool:
        if not hasattr(model, field_name):
            raise AttributeError(f"Модель {model.__name__} не имеет поля '{field_name}'")

        stmt = select(func.count()).select_from(model).where(getattr(model, field_name) == value)
        for key, val in kwargs.items():
            if hasattr(model, key):
                stmt = stmt.where(getattr(model, key) == val)
        count = await self.session.execute(stmt)
        return count.scalar() == 0

    async def create(self, model, data: dict, user_id: int = None) -> int:
        if model == Tables and user_id is not None:
            data['userid'] = user_id

        for field, value in data.items():
            if hasattr(model, field):
                if model == Columns and field == 'column_name':
                    if not await self._is_unique(Columns, 'column_name', value, table_id=data.get('table_id')):
                        return f"Колонка '{value}' уже существует в этой таблице."
                elif model == Tables and field == 'table_name':
                    if not await self._is_unique(Tables, 'table_name', value, user_id=user_id):
                        return f"Таблица с названием '{value}' уже существует."
                elif model == Users and field == 'username':
                    if not await self._is_unique(Users, 'username', value):
                        return f"Пользователь с именем '{value}' уже существует."
        if model == Data:
            stmt = insert(model).values(**data)
            await self.session.execute(stmt)
            result = f'row_id: {data["row_id"]}, column_id: {data["column_id"]}'
        else:
            stmt = insert(model).values(**data).returning(model.id)
            result = await self.session.execute(stmt)
            result = result.scalar()
        await self.session.commit()
        return result

    async def get(self, model, record_id: int, user_id: int = None, column_id: int = None):
        stmt = None
        if model == Tables:
            stmt = select(model).where(model.id == record_id, model.userid == user_id)
        elif model == Columns:
            if await self._user_owned(Columns, user_id, record_id):
                stmt = select(model).where(model.id == record_id)
        elif model == Data:
            if record_id and column_id:
                if await self._user_owned(Columns, user_id, column_id):
                    stmt = select(Data).where(
                        Data.row_id == record_id,
                        Data.column_id == column_id
                    )
        if stmt:
            result = await self.session.execute(stmt)
            record = result.scalars().first()
            if record:
                return {col.name: getattr(record, col.name) for col in record.__table__.columns}
        return None

    async def update(self, model, record_id: int, data: dict, user_id: int = None, column_id: int = None) -> bool:
        if not await self._exists(model, record_id):
            return False

        if user_id is not None:
            if model == Columns:
                if not await self._user_owned(Columns, user_id, record_id):
                    return False
            elif model == Tables:
                if not await self._user_owned(Tables, user_id, record_id):
                    return False
            elif model == Data:
                if not await self._user_owned(Columns, user_id, column_id):
                    return False
        if model != Data:
            stmt = update(model).where(model.id == record_id).values(**data)
        else:
            stmt = update(model).where(model.row_id == record_id, model.column_id == column_id).values(**data)
        await self.session.execute(stmt)
        await self.session.commit()
        return True

    async def delete(self, model, record_id: int, user_id: int = None, column_id: int = None) -> bool:
        if not await self._exists(model, record_id):
            return False

        if model == Tables:
            if not await self._user_owned(Tables, user_id, record_id):
                return False
            stmt = delete(model).where(model.id == record_id)

        elif model == Data:
            if not await self._user_owned(Columns, user_id, column_id):
                return False
            stmt = delete(model).where(
                Data.row_id == record_id,
                Data.column_id == column_id
            )

        else:
            if not await self._user_owned(Columns, user_id, record_id):
                return False
            stmt = delete(model).where(model.id == record_id)

        await self.session.execute(stmt)
        await self.session.commit()
        return True

    async def list_all(self, model, user_id: int = None) -> List[Dict]:
        if user_id is not None and hasattr(model, 'userid'):
            stmt = select(model).where(model.userid == user_id)
        else:
            stmt = select(model)

        result = await self.session.execute(stmt)
        records = result.scalars().all()
        return [{col.name: getattr(r, col.name) for col in r.__table__.columns} for r in records]