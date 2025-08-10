# controllers/base_ext/find.py
# Класс Поиск чанков

import os
from langchain.text_splitter import MarkdownHeaderTextSplitter
from openai import OpenAI
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
import asyncio

class Chunks:

    def __init__(self, main):
        """
        Конструктор класса.

        Инициализирует основные атрибуты и вызывает метод для создания базы знаний.

        Аргументы:
            main: Объект основного приложения.
        """
        self.main = main

        # инициализация локального индекса
        self.local_index = None        

    # МЕТОД: поиск в локальной индексной базе знаний
    #   question - вопрос к базе знаний
    async def find_local(self, question: str) -> str:
        """
        Асинхронный метод поиска в локальной базе знаний с использованием FAISS.
        """

        def load_and_search():
            os.environ['OPENAI_API_KEY'] = self.main.env.get('OPENAI_API_KEY')

            if self.local_index is None:
                self.local_index = FAISS.load_local(
                    'base/base08.faiss',
                    OpenAIEmbeddings(),
                    allow_dangerous_deserialization=True
                )

            results = self.local_index.similarity_search(question, k=5)
            return "\n\n".join(doc.page_content for doc in results)

        loop = asyncio.get_running_loop()
        context = await loop.run_in_executor(None, load_and_search)
        return context