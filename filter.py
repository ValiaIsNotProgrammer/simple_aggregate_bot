import json

from aiogram.filters import Filter
from aiogram.types import Message


class JSONFilter(Filter):
    """
    Фильтр для определения json сообщений
    """
    async def __call__(self, message: Message) -> bool:
        try:
            json.loads(message.text)
            return True
        except json.JSONDecodeError:
            return False