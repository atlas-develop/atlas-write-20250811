# classes/env.py

import os
from dotenv import load_dotenv

class Env:
    '''
    Класс для работы с переменными окружения
    Загружает переменные окружения из файла `.env` и предоставляет методы для их получения.
    '''

    def __init__(self, main=None):
        '''
        МЕТОД: конструктор
        Загружает переменные окружения из файла '.env'
        '''
        self.main = main        
        load_dotenv('.env')

    def get(self, key):
        '''
        МЕТОД: возвращает значение из .env
        Возвращает значение переменной окружения по ключу.
        '''
        return os.getenv(key)

    def get_int(self, name: str, default: int = 0) -> int:
        value = os.getenv(name)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    
    def get_float(self, name: str, default: float = 0.0) -> float:
        value = os.getenv(name)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default    