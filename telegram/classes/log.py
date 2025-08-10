# telegram/classes/log.py
# Логирование событий

import asyncio
from aiologger import Logger
from aiologger.handlers.files import AsyncFileHandler
import os
import datetime
import logging  # Для синхронного логирования


class Log:
    """
    Класс для настройки и использования логирования.
    Поддерживаются:
    - Асинхронное логирование с помощью aiologger
    - Синхронная обёртка для вызова асинхронных методов
    - Полностью синхронное логирование через logging
    """

    def __init__(self):
        self.logger = Logger(name="app_logger")
        self.console = Logger.with_default_handlers(name="app_logger")
        self.log_dir = 'log'

        os.makedirs(self.log_dir, exist_ok=True)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.setup_logger())
        except RuntimeError:
            asyncio.run(self.setup_logger())

    async def setup_logger(self):
        now = datetime.datetime.now()
        log_path = os.path.join(self.log_dir, f"{now.year}/{now.strftime('%m')}/{now.strftime('%d')}.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        handler = AsyncFileHandler(filename=log_path, mode="a")
        self.logger.add_handler(handler)
        self.console.add_handler(handler)

    async def log_info(self, target='', message='', data={}, is_console=False):
        data = self.safe_serialize(data)

        log_data = {'datetime': datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')}
        if target:
            log_data['target'] = target
        if message:
            log_data['message'] = message
        if data:
            log_data['data'] = data

        if is_console:
            await self.console.info('\n' + str(log_data))
        else:
            await self.logger.info('\n' + str(log_data))
            
    def safe_serialize(self, obj):
        """Рекурсивно сериализует сложные объекты в словари."""
        if isinstance(obj, dict):
            return {k: self.safe_serialize(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self.safe_serialize(v) for v in obj]
        elif hasattr(obj, '__dict__'):
            return {k: self.safe_serialize(v) for k, v in vars(obj).items() if not k.startswith('_')}
        elif hasattr(obj, 'model_dump'):  # Pydantic-like, если попадётся
            return self.safe_serialize(obj.model_dump())
        elif hasattr(obj, '__str__'):
            return str(obj)
        else:
            return obj            

    async def log_error(self, target='', message='', data={}, is_console=False):
        error_message = f"ERROR: {message}"
        await self.log_info(target, error_message, data, is_console)

    def log_info_sync(self, target='', message='', data={}, is_console=False):
        """
        Полностью синхронное логирование без asyncio/aiologger.
        """
        now = datetime.datetime.now()
        log_dir = os.path.join(self.log_dir, f"{now.year}/{now.strftime('%m')}")
        log_path = os.path.join(log_dir, f"{now.strftime('%d')}.log")

        os.makedirs(log_dir, exist_ok=True)

        log_data = {'datetime': now.strftime('%d.%m.%Y %H:%M:%S')}
        if target:
            log_data['target'] = target
        if message:
            log_data['message'] = message
        if hasattr(data, '__dict__'):
            data = {k: v for k, v in vars(data).items() if not k.startswith('_')}
        if data:
            log_data['data'] = data

        logger = logging.getLogger('sync_logger')
        logger.setLevel(logging.INFO)

        handler = logging.FileHandler(log_path, mode='a')
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(handler)

        logger.info('\n' + str(log_data))

        if is_console:
            print(message)

    def log_error_sync(self, target='', message='', data={}, is_console=False):
        error_message = f"ERROR: {message}"
        self.log_info_sync(target, error_message, data, is_console)

    async def shutdown(self):
        await self.logger.shutdown()
