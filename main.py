import logging
import asyncio
from ai import AIHandler
from os import getenv
from aiogram import Bot, Dispatcher, html, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.utils.chat_action import ChatActionSender
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ReactionTypeEmoji
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from db_config import Users, Tables, Columns, Data, Migration
from dotenv import load_dotenv

load_dotenv()
TOKEN = getenv("BOT_TOKEN")

engine = create_async_engine("sqlite+aiosqlite:///./test.db", echo=False)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)

dp = Dispatcher()

ai_handler = AIHandler()

async def create_tables():
    await Migration.up(engine)


async def user_exists(message: Message):
    user_id = message.from_user.id

    async with async_session_maker() as session:
        result = await session.execute(
            Users.__table__.select().where(Users.id == user_id)
        )
        existing_user = result.scalar_one_or_none()
        return existing_user


@dp.message(CommandStart())
async def start_message(message: Message):
    username = message.from_user.username or "Пользователь"
    if not await user_exists(message):
        async with async_session_maker() as session:
            new_user = Users(id=message.from_user.id, username=username)
            session.add(new_user)
            await session.commit()
            await message.answer(f"Привет, {username}! Вы успешно зарегистрированы.")
    else:
        await message.answer(f"Привет, {username}! Вы уже зарегистрированы.")

    keyboard = [
        [InlineKeyboardButton(text="Возможности", callback_data="info")]
    ]
    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await message.answer(
        f"Привет, {username}! Вы можете создавать таблицы и работать с данными.",
        reply_markup=reply_markup
    )


@dp.message(Command('clear'))
async def clear_context(message: Message):
    if not await user_exists(message):
        await message.reply("Для начала используйте /start")
        return False
    user_id = message.from_user.id
    await ai_handler._set_user_context(user_id, {"history": []})
    await message.answer("Контекст успешно очищен.")


@dp.callback_query(lambda c: c.data == "info")
async def info_button(callback: CallbackQuery):
    info = """Данный бот поможет создавать таблицы, управлять ими, производить поиск данных и генерировать их."""
    await callback.message.answer(info)


@dp.message()
async def handle_message(message: Message):
    if not await user_exists(message):
        await message.reply("Для начала используйте /start")
        return False
    if message.text.startswith('/'):
        return False
    await message.react([ReactionTypeEmoji(emoji='⚡')])
    async with ChatActionSender.typing(message.chat.id, message.bot):
        response = await ai_handler.handle_query(message.text, message.from_user.id)
        if response.startswith('<think>'):
            response = response.split('</think>')[-1]
        await message.reply(response)


async def main():
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await create_tables()
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())