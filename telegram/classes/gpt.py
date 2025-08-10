# telegram/classes/gpt.py
# Класс для работы с GPT

from openai import AsyncOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.docstore.document import Document
import json
import re
import os
import time

class Gpt():
    def __init__(self, main):
        self.main = main

    def getKey(self):
        return self.main.env.get('OPENAI_API_KEY')

    def createDocum(self, chunk, metadata):
        return Document(page_content=chunk, metadata=metadata)

    def createIndex(self, chunks):
        AsyncOpenAI.api_key = os.environ['OPENAI_API_KEY'] = self.getKey()
        embeddings = OpenAIEmbeddings()
        return FAISS.from_documents(chunks, embeddings)

    async def find_chunks(self, db, question, count):
        if count:
            docs = await db.asimilarity_search_with_score(question, k=count)
        else:
            docs = await db.asimilarity_search_with_score(question)
        return docs

    async def get_chunks(self, db, filter, count):
        if count > 0:
            docs = await db.asimilarity_search('', k=count, filter=filter)
        else:
            docs = await db.asimilarity_search('', filter=filter)
        return docs

    async def request(self, messages, model: str = 'gpt-4o-mini', format: dict = None, temperature: int = 0.5):
        client = AsyncOpenAI(api_key=self.getKey())

        if False:
            await self.main.log.log_info('telegram', 'Запрос в OpenAI', {
                'model': model,
                'temperature': temperature,
                'messages': messages,
            }, True)

        try:
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                response_format=format
            )

            if 'headers' in response:
                headers = response.headers

                # RPM/RPD контроль
                remaining_requests = int(headers.get('x-ratelimit-remaining-requests', 0))
                reset_requests = headers.get('x-ratelimit-reset-requests')
                if remaining_requests <= 0:
                    if reset_requests:
                        wait_time = float(reset_requests.strip('s'))
                        time.sleep(wait_time)
                    return 'Превышен лимит запросов в минуту. Попробуйте позже.'

                # TPM контроль
                remaining_tokens = int(headers.get('x-ratelimit-remaining-tokens', 0))
                reset_tokens = headers.get('x-ratelimit-reset-tokens')
                if remaining_tokens <= 0:
                    if reset_tokens:
                        wait_time = float(reset_tokens.strip('s'))
                        time.sleep(wait_time)
                    return 'Превышен лимит токенов в минуту. Попробуйте позже.'

            if response.choices:
                await self.main.log.log_info('telegram', 'Получен ответ', response, True)
                return response
            else:
                await self.main.log.log_info('telegram', 'Не удалось получить ответ от модели.')
                return 'Не удалось получить ответ от модели.'

        except Exception as e:
            await self.main.log.log_info('telegram', 'Ошибка при запросе', str(e))
            return f'Ошибка при запросе в OpenAI: {e}'
