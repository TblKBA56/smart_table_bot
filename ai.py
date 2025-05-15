import ollama
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import select, result_tuple
from crud import CRUD
from db_config import Users, Tables, Columns, Data, Base
from typing import Dict, Any, List

engine = create_async_engine("sqlite+aiosqlite:///./test.db", echo=False)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


class AIHandler:
    # ai = ollama.Client('26.91.15.215:11434')
    # model = 'qwen3:14b'
    ai = ollama.Client('127.0.0.1:11434')
    model = 'qwen3'

    def __init__(self):
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "tables_create",
                    "description": "Создаёт новую таблицу.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table_name": {"type": "string", "description": "Название таблицы"},
                        },
                        "required": ["table_name"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "tables_get",
                    "description": "Получает таблицу по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID таблицы"}
                        },
                        "required": ["record_id"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "tables_update",
                    "description": "Обновляет название таблицы.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID таблицы"},
                            "table_name": {"type": "string", "description": "Новое название таблицы"}
                        },
                        "required": ["record_id", "table_name"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "tables_delete",
                    "description": "Удаляет таблицу по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID таблицы"}
                        },
                        "required": ["record_id"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "columns_create",
                    "description": "Создаёт новую колонку в указанной таблице.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table_id": {"type": "integer", "description": "ID таблицы"},
                            "column_name": {"type": "string", "description": "Название колонки"},
                            "type": {"type": "string", "description": "Тип данных (TEXT, INTEGER и т.д.)"}
                        },
                        "required": ["table_id", "column_name", "type"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "columns_get",
                    "description": "Получает колонку по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID колонки"}
                        },
                        "required": ["record_id"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "columns_update",
                    "description": "Обновляет колонку по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID колонки"},
                            "column_name": {"type": "string", "description": "Новое название колонки"},
                            "type": {"type": "string", "description": "Новый тип данных"}
                        },
                        "required": ["record_id", "column_name", "type"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "columns_delete",
                    "description": "Удаляет колонку по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID колонки"}
                        },
                        "required": ["record_id"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "data_create",
                    "description": "Создаёт запись данных в колонке.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "column_id": {"type": "integer", "description": "ID колонки"},
                            "row_id": {"type": "integer", "description": "Номер строки"},
                            "data": {"type": "string", "description": "Значение для записи"}
                        },
                        "required": ["column_id", "row_id", "data"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "data_get",
                    "description": "Получает данные по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID данных"}
                        },
                        "required": ["record_id"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "data_update",
                    "description": "Обновляет данные по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID данных"},
                            "column_id": {"type": "integer", "description": "ID данных"},
                            "data": {"type": "string", "description": "Новое значение"}
                        },
                        "required": ["record_id", "column_id", "data"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "data_delete",
                    "description": "Удаляет данные по ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "integer", "description": "ID данных"},
                            "column_id": {"type": "integer", "description": "ID данных"}
                        },
                        "required": ["record_id", "column_id"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "list_records",
                    "description": "Возвращает список всех записей из указанной таблицы для текущего пользователя. "
                                   "Доступные таблицы: Tables, Columns, Data. Рекомендуется использовать только Tables."
                                   " Для поиска других записей смотреть следующую функцию.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "model": {"type": "string", "description": "Название таблицы"}
                        },
                        "required": ["model"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "list_records_with_filters",
                    "description": "Возвращает список записей из указанной таблицы с применением фильтров. "
                                   "Поддерживает фильтрацию по любым полям модели. "
                                   "Доступные таблицы (модели): Tables, Columns, Data. "
                                   "Формат фильтров поддерживает следующие операции: \n"
                                   "- 'like': частичное совпадение строки\n"
                                   "- 'eq': точное равенство значения\n"
                                   "- 'gt': значение больше заданного\n"
                                   "- 'lt': значение меньше заданного\n"
                                   "Примеры:\n"
                                   "1. Поиск всех колонок с названием 'Имя' в таблице с ID 1: "
                                   "{ 'model': 'Columns', 'filters': { 'table_id': { 'eq': 1 }, 'column_name': { 'like': 'Имя' } } }\n"
                                   "2. Найти все данные в колонке 5, где значение содержит 'Влад': "
                                   "{ 'model': 'Data', 'filters': { 'column_id': { 'eq': 5 }, 'data': { 'like': 'Влад' } } }\n"
                                   "3. Получить все записи с ID > 10 в таблице Data: "
                                   "{ 'model': 'Data', 'filters': { 'id': { 'gt': 10 } } }",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "model": {
                                "type": "string",
                                "description": "Название таблицы для фильтрации. Доступные значения: Tables, Columns, Data."
                            },
                            "filters": {
                                "type": "object",
                                "description": "Фильтры для выборки данных. Поддерживаются операторы: like, eq, gt, lt. Пример: { 'column_name': { 'like': 'Имя' }, 'table_id': { 'eq': 1 } }"
                            }
                        },
                        "required": ["model", "filters"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "task_end",
                    "description": "Если ты уверен в выполнении задачи и всех её требований - тебе необходимо "
                                   "выполнить данную функцию. Если ты абсолютно не понимаешь, что требует "
                                   "пользователь - выполни данную функцию. Если был создан бесконечный цикл и "
                                   "количество повторений автоматических запросов чуть больше или равно 5 - выполняй "
                                   "данную функцию. Эту функцию НЕЛЬЗЯ выполнять, когда задача ясна и её легко можно "
                                   "выполнить.",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                }
            }
        ]

    async def handle_query(self, query: str, user_id: int) -> str:
        try:
            response = await self._call_ollama(query, user_id)
            return response
        except Exception as e:
            return f"Ошибка: {str(e)}"

    @staticmethod
    async def _get_user_context(user_id: int) -> dict:
        async with async_session_maker() as session:
            stmt = select(Users).where(Users.id == user_id)
            result = await session.execute(stmt)
            user = result.scalars().first()
            return user.context if user and user.context else {}

    @staticmethod
    async def _set_user_context(user_id: int, context: dict):
        async with async_session_maker() as session:
            stmt = select(Users).where(Users.id == user_id)
            result = await session.execute(stmt)
            user = result.scalars().first()

            if user:
                user.context = context
            else:
                session.add(Users(id=user_id, context=context))
            await session.commit()

    async def _tools_calling(self, function_name, user_id, arguments):
        repeat = True
        if function_name == "tables_create":
            result = await self._tables_create(arguments, user_id)
        elif function_name == "tables_get":
            result = await self._tables_get(arguments, user_id)
        elif function_name == "tables_update":
            result = await self._tables_update(arguments, user_id)
        elif function_name == "tables_delete":
            result = await self._tables_delete(arguments, user_id)
        elif function_name == "columns_create":
            result = await self._columns_create(arguments, user_id)
        elif function_name == "columns_get":
            result = await self._columns_get(arguments, user_id)
        elif function_name == "columns_update":
            result = await self._columns_update(arguments, user_id)
        elif function_name == "columns_delete":
            result = await self._columns_delete(arguments, user_id)
        elif function_name == "data_create":
            result = await self._data_create(arguments, user_id)
        elif function_name == "data_get":
            result = await self._data_get(arguments, user_id)
        elif function_name == "data_update":
            result = await self._data_update(arguments, user_id)
        elif function_name == "data_delete":
            result = await self._data_delete(arguments, user_id)
        elif function_name == "list_records":
            result = await self._list_records(arguments, user_id)
        elif function_name == "list_records_with_filters":
            result = await self._list_records_with_filters(arguments, user_id)
        elif function_name == "task_end":
            repeat = False
            result = "Задача завершена."
        else:
            result = "Неизвестная функция."
        return repeat, result


    async def _call_ollama(self, query: str, user_id: int) -> str:
        system_prompt = (
            "Ты - помощник в управлении таблицами и базой данных. Тебе нужно использовать доступные инструменты "
            "(Functions/Tools) для решения задач пользователя. Если требуется выполнить действие: поиск, создание, "
            "удаление, редактирование — ОБЯЗАТЕЛЬНО используй tools. Не выводи обычный текст в ответах, только вызовы "
            "функций. При работе с идентификаторами и данными используй только те значения, которые были получены "
            "через функции или указаны пользователем. Если нужной информации нет, но она необходима для выполнения "
            "задачи - используй функции поиска. Если задача выполнена - используй tool под названием task_end. При "
            "создании таблицы НИКОГДА не добавляй столбцы в одной итерации. Отправь выполнение функции на создание "
            "таблицы, а в следующей итерации выполняй действия с полученным id. Завершение задачи должно выполняться"
            " через task_end. Если функция вернула что-то кроме 'Не найдено' - действие выполнено и повторно его "
            "выполнять не требуется. Если больше задач нет и все действия выполнены - используй task_end."
        )

        context = await self._get_user_context(user_id)
        history = context.get("history", [])

        history.append({"role": "user", "content": query})
        messages = [
            {"role": "system", "content": system_prompt},
            *history
        ]

        await self._set_user_context(user_id, {**context, "history": history[-20:]})
        new_messages = [{"role": "system", "content": "Тебе необходимо составить отчёт по выполнению задания "
                                                      "пользователя. Вся информация о выполнении также будет указана в "
                                                      "автоматизированных запросах пользователя. Нужно указывать "
                                                      "только итоговый результат операций. Итоговый результат "
                                                      "находится под указателем user. Отвечай на изначальный "
                                                      "запрос пользователя. Если требуемый результат не был достигнут, "
                                                      "так и пиши! Врать нельзя. Также можешь попросить уточнить "
                                                      "запрос. Если была найдена какая-то ошибка - укажи её. Если же "
                                                      "пользователь попросил получить какие-то данные и дополнительно "
                                                      "обработать их - твой ответ должен не ограничиваться отчётом. "
                                                      "Нужно выполнить форматирование в требуемом виде."}]
        repeat_state = True
        repeats_count = 0
        while repeat_state:

            response = AIHandler.ai.chat(
                model=AIHandler.model,
                messages=messages,
                tools=self.tools
            )

            new_messages.append({"role": "assistant", "content": response["message"]["content"]})
            new_messages.append({"role": "user", "content": "Это автоматически сгенерированный ответ с выводом функций."})
            messages.append({"role": "assistant", "content": response["message"]["content"]})
            messages.append({"role": "user", "content": "Это автоматически сгенерированный ответ с выводом функций."})

            if "tool_calls" in response["message"]:
                tool_calls = response["message"]["tool_calls"]

                for tool_call in tool_calls:
                    print(tool_call)
                    tool_call = dict(tool_call['function'])
                    function_name = tool_call["name"]
                    arguments = tool_call["arguments"]

                    tool_call_result = await self._tools_calling(function_name, user_id, arguments)
                    repeat, result = tool_call_result
                    repeats_count += 1
                    if not repeat or repeats_count >= 6:
                        repeat_state = False

                    messages[-1]['content'] += '\n' + function_name + ': ' + result
                    new_messages[-1]['content'] += '\n' + function_name + ': ' + result
                messages[-1]['content'] += f'\n\nВсего было использовано {len(tool_calls)} функций.'
                new_messages[-1]['content'] += f'\n\nВсего было использовано {len(tool_calls)} функций.'
            else:
                messages[-1]['content'] += '\n\nTOOLS НЕ БЫЛИ ИСПОЛЬЗОВАНЫ.'
                new_messages[-1]['content'] += '\n\nTOOLS НЕ БЫЛИ ИСПОЛЬЗОВАНЫ.'
        else:
            response = AIHandler.ai.chat(
                model=AIHandler.model,
                messages=new_messages
            )
            history.append({'role': 'assistant', 'content': response['message']['content']})
            await self._set_user_context(user_id, {**context, "history": history[-20:]})
            return response['message']['content']

    @staticmethod
    async def _tables_create(args: Dict[str, Any], user_id: int) -> str:
        table_name = args.get("table_name")
        async with async_session_maker() as session:
            crud = CRUD(session)
            record_id = await crud.create(Tables, {"table_name": table_name}, user_id=user_id)
            return f"Таблица создана с ID: {record_id}" if isinstance(record_id, int) else record_id

    @staticmethod
    async def _tables_get(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        async with async_session_maker() as session:
            crud = CRUD(session)
            record = await crud.get(Tables, record_id, user_id=user_id)
            return str(record)

    @staticmethod
    async def _tables_update(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        table_name = args.get("table_name")
        async with async_session_maker() as session:
            crud = CRUD(session)
            success = await crud.update(Tables, record_id, {"table_name": table_name}, user_id=user_id)
            return "Обновлено" if success else "Не найдено"

    @staticmethod
    async def _tables_delete(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        async with async_session_maker() as session:
            crud = CRUD(session)
            success = await crud.delete(Tables, record_id, user_id=user_id)
            return "Удалено" if success else "Не найдено"

    @staticmethod
    async def _columns_create(args: Dict[str, Any], user_id: int) -> str:
        table_id = args.get("table_id")
        column_name = args.get("column_name")
        type = args.get("type")
        async with async_session_maker() as session:
            crud = CRUD(session)
            record_id = await crud.create(
                Columns,
                {"table_id": table_id, "column_name": column_name, "type": type},
                user_id=user_id
            )
            return f"Колонка создана с ID: {record_id}" if isinstance(record_id, int) else record_id

    @staticmethod
    async def _columns_get(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        async with async_session_maker() as session:
            crud = CRUD(session)
            record = await crud.get(Columns, record_id, user_id=user_id)
            return str(record)

    @staticmethod
    async def _columns_update(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        column_name = args.get("column_name")
        new_type = args.get("type")
        data = {}
        if column_name:
            data['column_name'] = column_name
        if new_type:
            data['type'] = new_type

        async with async_session_maker() as session:
            crud = CRUD(session)
            success = await crud.update(
                Columns,
                record_id,
                data,
                user_id=user_id
            )
            return "Обновлено" if success else "Запись не найдена"

    @staticmethod
    async def _columns_delete(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        async with async_session_maker() as session:
            crud = CRUD(session)
            success = await crud.delete(Columns, record_id, user_id=user_id)
            return "Удалено" if success else "Не найдено"

    @staticmethod
    async def _data_create(args: Dict[str, Any], user_id: int) -> str:
        column_id = args.get("column_id")
        row_id = args.get("row_id")
        data_value = args.get("data")
        async with async_session_maker() as session:
            crud = CRUD(session)
            record_id = await crud.create(
                Data,
                {"column_id": column_id, "row_id": row_id, "data": data_value},
                user_id=user_id
            )
            return f"Данные созданы с двойным ID: {record_id}" if isinstance(record_id, int) else record_id

    @staticmethod
    async def _data_get(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        async with async_session_maker() as session:
            crud = CRUD(session)
            record = await crud.get(Data, record_id, user_id=user_id)
            return str(record)

    @staticmethod
    async def _data_update(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        column_id = args.get("column_id")
        new_data = args.get("data")
        async with async_session_maker() as session:
            crud = CRUD(session)
            success = await crud.update(Data, record_id, {"data": new_data}, user_id=user_id, column_id=column_id)
            return "Обновлено" if success else "Не найдено"

    @staticmethod
    async def _data_delete(args: Dict[str, Any], user_id: int) -> str:
        record_id = args.get("record_id")
        column_id = args.get("column_id")
        async with async_session_maker() as session:
            crud = CRUD(session)
            success = await crud.delete(Data, record_id, user_id=user_id, column_id=column_id)
            return "Удалено" if success else "Не найдено"

    async def _list_records(self, args: Dict[str, Any], user_id: int) -> str:
        model_name = args.get("model")
        filters = args.get("filters", {})

        model_map = {
            "Users": Users,
            "Tables": Tables,
            "Columns": Columns,
            "Data": Data
        }

        model = model_map.get(model_name)
        if not model:
            return "Неверное имя таблицы."

        async with async_session_maker() as session:
            crud = CRUD(session)
            records = await crud.list_all(model, user_id)

            filtered_records = []
            for record in records:
                match = True
                for key, value in filters.items():
                    if key not in record or not await self._match_filter(record[key], value):
                        match = False
                        break
                if match:
                    filtered_records.append(record)

            return str(filtered_records)

    async def _list_records_with_filters(self, args: Dict[str, Any], user_id: int) -> str:
        model_name = args.get("model")
        filters = args.get("filters", {})

        model_map = {
            "Users": Users,
            "Tables": Tables,
            "Columns": Columns,
            "Data": Data
        }

        model = model_map.get(model_name)
        if not model:
            return "Неверное имя таблицы."

        async with async_session_maker() as session:
            crud = CRUD(session)
            records = await crud.list_all(model, user_id)

            filtered_records = []
            for record in records:
                match = True
                for key, value in filters.items():
                    if key not in record or not await self._match_filter(record[key], value):
                        match = False
                        break
                if match:
                    filtered_records.append(record)

            return str(filtered_records)

    async def _match_filter(self, field_value: Any, filter_value: Any) -> bool:
        if isinstance(filter_value, dict):
            if "like" in filter_value:
                return isinstance(field_value, str) and filter_value["like"] in field_value
            elif "eq" in filter_value:
                return field_value == filter_value["eq"]
            elif "gt" in filter_value:
                return field_value > filter_value["gt"]
            elif "lt" in filter_value:
                return field_value < filter_value["lt"]
        return field_value == filter_value