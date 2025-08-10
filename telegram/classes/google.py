# classes/google.py
# Класс для работы с Google Sheets

# Импорт классов
import asyncio  # для асинхронных функций
import aiohttp  # для асинхронных HTTP-запросов
import re  # для обработки ссылок
from google.oauth2.service_account import Credentials  # для работы с сервисным аккаунтом Google
from googleapiclient.discovery import build  # для работы с Google Drive API
import gspread_asyncio  # для асинхронной работы с Google Sheets
from gspread_asyncio import AsyncioGspreadClientManager

class Google:
    """
    Класс для работы с Google API.
    """
    
    def __init__(self, main):
        """
        Инициализация объекта Google API.

        Параметры:
        - **main**: Объект главного приложения, содержащий основные настройки, такие как доступ к API Google.
        """
        self.main = main      
           
        # Авторизация в Google Sheets API
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.credentials = Credentials.from_service_account_file(self.main.env.get('GOOGLE_KEY'), scopes=scopes)
        self.client = gspread_asyncio.AsyncioGspreadClientManager(self.get_credentials)

        # Авторизация в Google Drive API
        self.drive_service = build('drive', 'v3', credentials=self.credentials)

        # Словарь для хранения кэшированных листов
        self.sheets = {}
        
        self.queue = asyncio.Queue()  # Очередь задач
        self.processing_task = None  # Переменная для хранения задачи

        # Кэш клиента Google Sheets
        self._gspread_client = None     

    def get_credentials(self):
        """ Возвращает учетные данные для gspread_asyncio """
        return self.credentials

    async def get_client(self):
        """ Создает и возвращает кэшированного асинхронного клиента Google Sheets. """
        if not self._gspread_client:
            agcm = AsyncioGspreadClientManager(lambda: self.credentials)
            self._gspread_client = await agcm.authorize()
        return self._gspread_client
       
    async def row_insert(self, data):
        """Добавляет данные в очередь для последующей вставки"""
        await self.queue.put((data))
        
        # Если задача еще не запущена, запускаем её
        if not self.processing_task:
            self.processing_task = asyncio.create_task(self._process_queue())        
        
    async def _process_queue(self):
        """Фоновая корутина для обработки очереди вставки данных"""
        while True:
            data = await self.queue.get()  # Получаем данные из очереди
            try:
                sheet = await self.get_worksheet(data['filename'])
                all_values = await sheet.get_all_values()

                # Ищем первую пустую строку
                next_row = len(all_values) + 1
                for i, row_data in enumerate(all_values):
                    if not any(row_data):
                        next_row = i + 1
                        break

                # Вставляем данные в первую пустую строку
                await sheet.insert_row(data['row'], next_row, value_input_option='USER_ENTERED')
                
                # Логирование добавления записи
                await self.main.log.log_info('telegram', 'Добавлена запись в Google Sheet', data, True)
                
            except Exception as e:
                await self.main.log.log_info('telegram', 'Ошибка вставки данных', e, True)

            self.queue.task_done()  # Отмечаем задачу как выполненную                 

    async def get_worksheet(self, filename):
        """
        Возвращает первый лист Google Sheets для указанного файла.

        :param filename: Имя файла.
        :return: Объект Worksheet.
        """
        if filename in self.sheets:
            return self.sheets[filename]['sheet']

        await self._get_or_create_file(filename)
        return self.sheets[filename]['sheet']

    async def _get_or_create_file(self, filename):
        """
        Проверяет, существует ли файл в Google Drive, иначе копирует шаблон.

        :param filename: Имя файла.
        """
        files = await self.list_files()

        for file in files:
            if file['name'] == filename:
                doc_url = f"https://docs.google.com/spreadsheets/d/{file['id']}"
                await self.main.log.log_info('telegram', 'Открытие файла', {'filename': filename}, True)  
                await self._cache_file(filename, doc_url)
                return

        if files:
            await self._copy_template_file(filename, files[0]['id'])
        else:
            raise ValueError("Нет файлов для копирования в указанной папке.")

    async def _copy_template_file(self, filename, template_file_id):
        """
        Копирует шаблонный файл Google Sheets.

        :param filename: Имя нового файла.
        :param template_file_id: ID шаблонного файла.
        """
        await self.main.log.log_info('telegram', 'Старт метода _copy_template_file()', {'filename': filename}, True)        
        folder_id = self._extract_folder_id(self.main.env.get('GOOGLE_TEST_FOLDER'))

        async with aiohttp.ClientSession() as session:
            copy_url = f"https://www.googleapis.com/drive/v3/files/{template_file_id}/copy"
            copy_data = {"name": filename, "parents": [folder_id]}
            headers = {"Authorization": f"Bearer {self.credentials.token}", "Content-Type": "application/json"}

            async with session.post(copy_url, headers=headers, json=copy_data) as copy_response:
                copy_result = await copy_response.json()
                if "id" not in copy_result:
                    raise ValueError(f"Ошибка копирования: {copy_result}")

                doc_url = f"https://docs.google.com/spreadsheets/d/{copy_result['id']}"
                await self.main.log.log_info('telegram', 'Создание файла Google Sheet', {'filename': filename}, True)
                await self._cache_file(filename, doc_url)
                await self.clear_and_format_sheet(copy_result['id'])

    async def _cache_file(self, filename, doc_url):
        """
        Кэширует объект книги и листа.

        :param filename: Имя файла.
        :param doc_url: URL Google Sheets.
        """
        client = await self.get_client()
        book_obj = await client.open_by_url(doc_url)
        sheet_obj = await book_obj.get_worksheet(0)

        self.sheets[filename] = {'book': book_obj, 'sheet': sheet_obj}

    async def list_files(self):
        """
        Возвращает список файлов в папке Google Drive.

        :return: Список файлов.
        """
        folder_id = self._extract_folder_id(self.main.env.get('GOOGLE_TEST_FOLDER'))
        query = f"'{folder_id}' in parents and trashed=false"

        results = await self._drive_files_list(query)
        self.files = results.get('files', [])  # Кэшируем список файлов
        return self.files        

    async def _drive_files_list(self, query):
        """ Асинхронно выполняет запрос к API Google Drive для получения файлов. """
        request = self.drive_service.files().list(q=query, fields='files(id, name)')
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, request.execute)
        return response

    async def clear_and_format_sheet(self, file_id):
        """
        Очищает строки во втором ряду и удаляет все строки после второго ряда.
        """
        worksheet = await self.get_worksheet_from_id(file_id)
        all_values = await worksheet.get_all_values() if worksheet else []

        header_row = all_values[0] if all_values else []
        last_header_column = len([cell for cell in header_row if cell])

        if len(all_values) > 2:
            await worksheet.delete_rows(3, len(all_values))

        if len(all_values) > 1:
            update_range = f"A2:{chr(65 + last_header_column - 1)}2"
            await worksheet.update(update_range, [[""] * last_header_column])

    async def get_worksheet_from_id(self, file_id):
        """
        Получает лист по ID файла.

        :param file_id: ID файла.
        :return: Объект Worksheet.
        """
        client = await self.get_client()
        sheet = await client.open_by_key(file_id)
        return await sheet.get_worksheet(0)

    def _extract_folder_id(self, url):
        """ Извлекает ID папки из ссылки Google Drive. """
        match = re.search(r'folders/([a-zA-Z0-9_-]+)', url)
        if match:
            return match.group(1)
        raise ValueError("Невалидная ссылка на папку Google Drive.")
    
    async def get_url(self, client_id: str) -> str:
            """
            Возвращает URL на файл журнала по client_id. Ищет файл, имя которого начинается с "{client_id} -".

            :param client_id: Идентификатор клиента.
            :return: Ссылка на файл Google Sheets, либо '' если файл не найден.
            """
            prefix = f"{client_id} -"

            # Используем кэшированный список, если есть
            files = getattr(self, 'files', None)
            if files is None:
                files = await self.list_files()

            for file in files:
                if file['name'].startswith(prefix):
                    return f"https://docs.google.com/spreadsheets/d/{file['id']}"

            return ''    
        
    async def shutdown(self):
        """Останавливает обработку очереди и ожидает завершения задачи."""
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
            self.processing_task = None
        
