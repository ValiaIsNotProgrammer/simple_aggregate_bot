import asyncio
import json

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command

from filter import JSONFilter
from aggregate import aggregate


API_TOKEN = '6987399347:AAF5OPEpdtsHuDqgR_IyUPRtcj_b5RNK_qI'
bot = Bot(token=API_TOKEN)
dp = Dispatcher()
not_valid_text = 'Невалидный запрос. Пример запроса:\n{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'


@dp.message(Command("start"))
async def start(msg: types.Message):
    print(msg.from_user.id)
    await msg.reply("Привет! Отправь мне JSON данные, и я проверю их на вхождение определенного объекта.")


@dp.message(JSONFilter())
async def aggregate_json(msg: types.Message):
    try:
        aggregated = aggregate(msg.text)
    except Exception as e:
        return await msg.reply(not_valid_text)
    json_msg = json.dumps(aggregated)
    await msg.answer(json_msg)


@dp.message()
async def send_json(msg: types.Message):
    await msg.reply(not_valid_text)


async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())