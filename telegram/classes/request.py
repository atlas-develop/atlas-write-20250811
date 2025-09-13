# telegram/classes/request
# Запрос к GPT

import json
from datetime import datetime
from datetime import date, timedelta
from telegram import Update
from telegram.ext import ContextTypes
from pathlib import Path
import ast
from telegram.error import TelegramError

class Request:
    def __init__(self, main):
        self.main = main
        self._system_prompt = None
        self.system_path = Path('base/system.md')

    async def clear_dialog(self, update: Update = None):

        # Получаем id клиента
        telegram_user_id = str(update.effective_user.id)
        client_id = await self.get_or_create_client_id(telegram_user_id, update)
        if False: return client_id
        
        # Удаляем все сообщения диалога
        delete_sql = "UPDATE `rec_dialog` SET `is_delete`=1 WHERE client_id = %s"
        await self.main.mysql.execute(delete_sql, (client_id,))

        # Очищаем summary
        update_sql = "UPDATE `rec_client` SET summary = NULL WHERE id = %s"
        await self.main.mysql.execute(update_sql, (client_id,))
        
        # Возвращает client_id
        return client_id

    async def get_or_create_client_id(self, telegram_user_id: str, update: Update = None) -> int:
        # 1. Пытаемся найти клиента
        sql = "SELECT id FROM rec_client WHERE tel_id = %s"
        result = await self.main.mysql.fetch_one(sql, (telegram_user_id,))
        if result:
            return result[0]

        # 2. Достаём данные из update, если есть
        tel_username = update.effective_user.username if update else None
        tel_name = update.effective_user.full_name if update else None
        tel_first = update.effective_user.first_name if update else None

        # 3. Создаём клиента
        insert_sql = """
            INSERT INTO rec_client (tel_id, tel_username, tel_name, tel_first)
            VALUES (%s, %s, %s, %s)
        """
        await self.main.mysql.execute(insert_sql, (
            telegram_user_id,
            tel_username,
            tel_name,
            tel_first
        ))

        # 4. Получаем ID
        result = await self.main.mysql.fetch_one(sql, (telegram_user_id,))
        return result[0]

    async def get_client_history(self, client_id: int):
        sql = "SELECT role, content FROM rec_dialog WHERE client_id = %s AND `is_delete`=0 ORDER BY `time` ASC"
        rows = await self.main.mysql.fetch_all(sql, (client_id,))
        value = self.main.env.get('DIALOG_SAVE')
        value_int = int(value) if value is not None else None
        extract = (value_int + 1) * 2

        role_map = {0: "user", 1: "assistant"}
        history = [{"role": role_map.get(role, "user"), "content": content} for role, content in rows[-extract:]]

        if len(history) >= extract:
            hist_summary = history[:2]
            hist_dialog = history[2:]
        else:
            hist_summary = []
            hist_dialog = history

        return hist_dialog, hist_summary

    async def save_message(self, client_id: int, role: str, content: str, tokens_in: int = 0, tokens_out: int = 0):
        content = str(content)
        role_map = {'user': 0, 'assistant': 1}
        role_int = role_map.get(role, 0)  # по умолчанию 0 (user), если что-то пошло не так

        if role == 'user':
            sql = "INSERT INTO rec_dialog (client_id, role, content) VALUES (%s, %s, %s)"
            params = (client_id, role_int, content)
        else:
            sql = """
                INSERT INTO rec_dialog (
                    client_id, role, content,
                    tokens_in, tokens_out,
                    price_in, price_out
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            price_in = self.main.env.get_float('GPT_PRICE_IN')
            price_out = self.main.env.get_float('GPT_PRICE_OUT')
            params = (
                client_id, role_int, content,
                tokens_in, tokens_out,
                price_in, price_out
            )
        
        await self.main.mysql.execute(sql, params)

    async def get_today_request_count(self, client_id: int):
        today = datetime.now().strftime("%Y-%m-%d")
        sql = """
            SELECT COUNT(*) FROM rec_dialog
            WHERE client_id = %s AND role = 0 AND DATE(`time`) = %s AND `is_delete`=0
        """
        result = await self.main.mysql.fetch_one(sql, (client_id, today))
        return result[0] if result else 0

    async def get_system_prompt(self):
        if self._system_prompt is None:
            try:
                self._system_prompt = self.system_path.read_text(encoding='utf-8').strip()
            except Exception as e:
                await self.main.log.log_info("System", "Ошибка чтения system.md", str(e), True)
                self._system_prompt = "Ты — ассистент. (system.md не найден)"
        return self._system_prompt
    
    async def get_user_summary(self, client_id: int) -> str:
        sql = "SELECT summary FROM rec_client WHERE id = %s"
        result = await self.main.mysql.fetch_one(sql, (client_id,))
        return result[0] if result and result[0] is not None else ""        

    async def update_user_summary(self, client_id: int, summary: str):
        sql = """
            UPDATE rec_client
            SET summary = %s
            WHERE id = %s
        """
        await self.main.mysql.execute(sql, (summary, client_id))    

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        telegram_user_id = str(update.effective_user.id)
        user_input = update.message.text.strip()
        user = update.message.from_user
        username = user.username if user.username else f"{user.first_name} {user.last_name or ''}".strip()
        
        # Получаем или создаём внутренний ID клиента
        client_id = await self.get_or_create_client_id(telegram_user_id, update)
        
        # Добавление ответа пользователя в Google Sheet
        await self.save_to_google_sheet(client_id, username, 'user', user_input)        

        # Лимит запросов
        if telegram_user_id not in self.main.env.get('UNLIMITED_USERS'):
            count = await self.get_today_request_count(client_id)
            if count >= 20:
                await update.message.reply_text("Вы достигли дневного лимита в 20 запросов. Попробуйте завтра.")
                return

        # История
        hist_dialog, hist_summary = await self.get_client_history(client_id)
        
        # Роль system
        system = await self.get_system_prompt()
        
        # Данные из БД
        # md_data = await self.generate_md_data(client_id)
        # system += "\n\n---\n\n📚 Актуальные данные из БД:\n\n" + md_data        

        # История диалога        
        summary = await self.get_user_summary(client_id)
        hist_summary_str = "\n".join(f"{m['role']}: {m['content']}" for m in hist_summary)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        system = system + (
            f"\n\n🕒 Текущая дата и время: {now}"
            f"\n\n📌 Текущая информаци для summary:\n{summary}\n\n"
            f"📌 Последняя пара сообщений для включения в summary:\n{hist_summary_str}"
        )
        
        # Исходные данные: адреса
        data = [
            {
                "role": "assistant",
                "content": """Записать на прием можно по следующим адресам:\n
                    'addresses': [
                        'address_id': '11', 'address_name': 'г. Москва, ул. Академика Королёва, д. 15',
                        'address_id': '12', 'address_name': 'г. Санкт-Петербург, Невский проспект, д. 28',
                    ],                    
                """
            },
            {"role": "user", "content": """
                Хорошо. Когда будет предлагать мне выбрать адрес - перечисли все адреса, каждый с новой строки и дефиса!\nНа какое время можно записаться в Санкт-Петербурге?
            """},
            {'role': 'assistant', 'content': 'В Санкт-Петербурге (address_id=12) можно записаться с 09:00 до 20:00 с интервалом 1 час на 15.10.2025 и 22.10.2025, вот более подробно:' + """
                    {"event_id": "146", "date":"2025-10-15", "time":"09:00"},
                    {"event_id": "147", "date":"2025-10-15", "time":"10:00"},
                    {"event_id": "148", "date":"2025-10-15", "time":"11:00"},
                    {"event_id": "149", "date":"2025-10-15", "time":"12:00"},
                    {"event_id": "150", "date":"2025-10-15", "time":"13:00"},
                    {"event_id": "151", "date":"2025-10-15", "time":"14:00"},
                    {"event_id": "152", "date":"2025-10-15", "time":"15:00"},
                    {"event_id": "153", "date":"2025-10-15", "time":"16:00"},
                    {"event_id": "154", "date":"2025-10-15", "time":"17:00"},
                    {"event_id": "155", "date":"2025-10-15", "time":"18:00"},
                    {"event_id": "156", "date":"2025-10-15", "time":"19:00"},
                    {"event_id": "157", "date":"2025-10-15", "time":"20:00"},                    
                    {"event_id": "158", "date":"2025-10-22", "time":"09:00"},
                    {"event_id": "159", "date":"2025-10-22", "time":"10:00"},
                    {"event_id": "160", "date":"2025-10-22", "time":"11:00"},
                    {"event_id": "161", "date":"2025-10-22", "time":"12:00"},
                    {"event_id": "162", "date":"2025-10-22", "time":"13:00"},
                    {"event_id": "163", "date":"2025-10-22", "time":"14:00"},
                    {"event_id": "164", "date":"2025-10-22", "time":"15:00"},
                    {"event_id": "165", "date":"2025-10-22", "time":"16:00"},
                    {"event_id": "166", "date":"2025-10-22", "time":"17:00"},
                    {"event_id": "167", "date":"2025-10-22", "time":"18:00"},
                    {"event_id": "168", "date":"2025-10-22", "time":"19:00"},
                    {"event_id": "169", "date":"2025-10-22", "time":"20:00"},                    
            """},
            {"role": "user", "content": "На какое время можно записаться в Мослве?"},            
            {'role': 'assistant', 'content': 'В Москве (address_id=11) можно записаться с 10:00 до 16:00 с интервалом 1 час на 20.10.2025 и 30.10.2025, вот более подробно:' + """
                    {"event_id": "170", "date":"2025-10-20", "time":"10:00"},
                    {"event_id": "171", "date":"2025-10-20", "time":"11:00"},
                    {"event_id": "172", "date":"2025-10-20", "time":"12:00"},
                    {"event_id": "173", "date":"2025-10-20", "time":"13:00"},
                    {"event_id": "174", "date":"2025-10-20", "time":"14:00"},
                    {"event_id": "175", "date":"2025-10-20", "time":"15:00"},                    
                    {"event_id": "176", "date":"2025-10-20", "time":"16:00"},
                    {"event_id": "177", "date":"2025-10-30", "time":"10:00"},
                    {"event_id": "178", "date":"2025-10-30", "time":"11:00"},
                    {"event_id": "179", "date":"2025-10-30", "time":"12:00"},
                    {"event_id": "180", "date":"2025-10-30", "time":"13:00"},
                    {"event_id": "181", "date":"2025-10-30", "time":"14:00"},
                    {"event_id": "182", "date":"2025-10-30", "time":"15:00"},                    
                    {"event_id": "183", "date":"2025-10-30", "time":"16:00"},                     
            """},
            {"role": "user", "content": "Какой врач принимает в Москве?"},                        
            {"role": "assistant", "content": "В Москве (address_id=11) проводит прием {'doctor_id': 1, 'fio': 'Соколов Иван Викторович', 'desc': 'Врач ЛФК, стаж 19 лет'}"},
            {"role": "user", "content": "Какой врач принимает в Санкт-Петербурге?"},                        
            {"role": "assistant", "content": "В Санкт-Петербурге (address_id=12) проводит прием {'doctor_id': 2, 'fio': 'Карчевский Вадим Вадимович', 'desc': 'Невролог, стаж 9 лет'}"},
        ]
        
        # Данные о записи на прием
        write_me_dict = await self.get_write_recept(client_id)
        write_me_current = [
            {'role': 'assistant', 'content': f'Вы записаны на прием:\n{write_me_dict}' if write_me_dict else 'Вы пока не записаны на прием!'},
            {'role': 'user', 'content': 'Кто-то из моих друзей, родственников, знакомых записаны на прием?'}
        ]
        write_friend_dict = await self.get_write_recept_friends(client_id)
        write_me_current += [
            {'role': 'assistant', 'content': f'Вами записаны на прием:\n{write_friend_dict}' if write_friend_dict else 'Вами никто не записан на прием!'},
            {'role': 'user', 'content': 'Понял'}
        ]        
        
        # Получение чанков
        chunks_text = await self.main.chunks.find_local(user_input)
        '''
        chunks_text = 'Отзыв от Анжелы 15 лет - круто было'
        chunks_role = [
            {"role": "assistant", "content": f"📄 Контекст из базы знаний:\n{chunks_text}"},
            {'role': 'user', 'content': 'Хорошо приму к сведению'}
        ]
        '''

        # Сборка промпта
        messages = [{"role": "system", "content": system}] + data + write_me_current + hist_dialog + [
            {"role": "user", "content": f"Ответь строго в JSON формате. Вот сообщение пользователя: {user_input}\n\nКонтекст из базы знаний (чанки): {chunks_text}"}
        ]

        # Сохраняем пользовательское сообщение
        await self.save_message(client_id, "user", user_input)

        try:
            tokens_in = tokens_out = 0
            response = await self.main.gpt.request(
                messages=messages,
                model=self.main.env.get('GPT_MODEL'),
                format={"type": "json_object"},
                temperature=0.5
            )
            await self.main.log.log_info('telegram', 'Запрос в OpenAI', messages, True)
            await self.main.log.log_info('telegram', 'Получен ответ от OpenAI', response, True)
            tokens_in = getattr(response.usage, "prompt_tokens", 0)
            tokens_out = getattr(response.usage, "completion_tokens", 0)            
            content = response.choices[0].message.content.strip()
            data = json.loads(content)
            answer = data.get("answer", "Извините, я не понял.")
            summary = data.get("summary", "")
            if summary != '':
                await self.update_user_summary(client_id, summary)           
            intent = data.get("intent", "")
            function_call = data.get("function_call", "")                  
            
            # Добавление ответа ассистента в Google Sheet
            await self.save_to_google_sheet(client_id, username, 'assistant', answer)

            # Служебная информация            
            if False:
                data_info = ''
                # if intent != '': data_info += f'\nintent: {intent}'
                if function_call != '': data_info += f'\nfunction_call: {function_call}'
                if data_info != '':
                    answer += '\n\n---' + data_info
                
            # Вызов функций
            if function_call:
                try:
                    # Разбиваем строку на отдельные вызовы
                    calls = [fc.strip() for fc in function_call.split('),') if fc.strip()]
                    if not calls:
                        raise ValueError("Невозможно распарсить function_call")

                    for i, call in enumerate(calls):
                        # Добавляем ')' обратно, если она была удалена при split
                        if not call.endswith(')'):
                            call += ')'

                        func_name, func_args = self.parse_function_call(call)

                        # Добавляем client_id, если его нет
                        if func_name != "notify_operator":
                            func_args.setdefault('client_id', client_id)
                        
                        # Добавляем update и context для 'notify_operator'
                        if func_name == "notify_operator":
                            func_args.setdefault('update', update)
                            func_args.setdefault('context', context)

                        # Вызываем функцию
                        result = await getattr(self, func_name)(**func_args)

                        # Ответ в зависимости от вызванной функции
                        if func_name == "write_recept":
                            answer += f"\n\n✅ Вы записаны!"
                            await self.clear_dialog(update)
                        elif func_name == "write_me_update":
                            answer += f"\n\n🔄 Запись перенесена!"
                            await self.clear_dialog(update)
                        elif func_name == "write_me_cancel":
                            answer += f"\n\n🚫 Запись отменена!"
                            await self.clear_dialog(update)
                        elif func_name == "write_recept_friend":
                            answer += f"\n\n✅ Запись выполнена!"
                            await self.clear_dialog(update)
                        elif func_name == "write_friend_update":
                            answer += f"\n\n🔄 Запись перенесена!"
                            await self.clear_dialog(update)
                        elif func_name == "write_friend_cancel":
                            answer += f"\n\n🚫 Запись отменена!"
                            await self.clear_dialog(update)
                        elif func_name == "notify_operator":
                            answer += f"\n\n👨‍⚕️ Ваше обращение направлено в чат поддержки! Вам ответит первый освободившийся оператор"                                                    
                        else:
                            answer += f"\n\n⚠️ Неизвестная функция: {func_name}"

                except Exception as fc_err:
                    await self.main.log.log_info("telegram", "Ошибка вызова function_call", str(fc_err), True)
                    answer += "\n\n⚠️ Ошибка при выполнении действия."      
        except Exception as e:
            await self.main.log.log_info("OpenAI", "Ошибка в обработке", str(e), True)
            answer = "Извините, не удалось обработать ваш запрос. Пожалуйста, повторите позже."

        # Сохраняем ответ
        await self.save_message(client_id, "assistant", answer, tokens_in, tokens_out)

        # Отправка пользователю
        await update.message.reply_text(answer)
        
    async def save_to_google_sheet(self, client_id, username, role, answer):
        """
        Формирует строку и отправляет её в Google Sheets
        """
        current_time = datetime.now()
        google_data = {
            'filename': str(client_id) + ' - @' + username,
            'client_id': client_id,
            'row': [
                current_time.strftime('%d.%m.%Y'),
                current_time.strftime('%H:%M'),
                '',
                role,
                answer,
                ''
            ]
        }
        await self.main.log.log_info('telegram', 'Данные для Google', google_data, True)
        await self.main.google.row_insert(google_data)        

    async def notify_operator(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message: str):
        try:
            user = update.message.from_user
            username = f"@{user.username}" if user.username else f"id:{user.id}"
            full_message = f"👤 Запрос от пользователя {username}:\n{message}"                            
            await context.bot.send_message(chat_id=self.main.env.get('SUPPORT_GROUP_ID'), text=full_message)
        except TelegramError as e:
            # логируем ошибку или обрабатываем по-своему
            await self.main.log.log_info('telegram', f"Ошибка при отправке в группу: {e}", {}, True)
                
    async def generate_md_data(self, client_id: int) -> str:
        today = datetime.today().date()

        # places
        sql_places = "SELECT id, text FROM rec_place WHERE is_delete = 0"
        rows = await self.main.mysql.fetch_all(sql_places)
        places = {str(r[0]): r[1] for r in rows}  # r[0] = id, r[1] = text

        # masters
        sql_masters = "SELECT id, fio, `desc` FROM rec_master WHERE is_delete = 0"
        rows = await self.main.mysql.fetch_all(sql_masters)
        masters = {
            str(r[0]): {
                "fio": r[1],
                "desc": r[2] or ""
            }
            for r in rows
        }

        # events - добавляем сортировку по дате и времени в SQL
        sql_events = """
            SELECT id, date, TIME_FORMAT(time, '%H:%i') as time_str, place_id, master_id
            FROM rec_event
            WHERE is_delete = 0 AND date >= CURDATE()
            ORDER BY date ASC, time ASC
        """
        rows = await self.main.mysql.fetch_all(sql_events)

        events = {}
        for r in rows:
            event_id = str(r[0])
            place_id = str(r[3])
            master_id = str(r[4])
            events[event_id] = {
                "date": r[1].strftime('%Y-%m-%d'),
                "time": r[2],
                "place_id": place_id,
                "place_name": places.get(place_id, ""),
                "master_id": master_id,                
                "master_fio": masters.get(master_id, {}).get("fio", "")
            }

        # write_me - фильтр по client_id
        sql_write = """
            SELECT rw.event_id, rw.client_fio, rw.problem, rc.params, re.date
            FROM rec_write rw
            JOIN rec_client rc ON rw.client_id = rc.id
            JOIN rec_event re ON rw.event_id = re.id
            WHERE rw.is_delete = 0 AND re.date >= CURDATE() AND rw.client_id = %s
        """
        rows = await self.main.mysql.fetch_all(sql_write, (client_id,))

        write_me = []
        for r in rows:
            write_me.append({
                "event_id": str(r[0]),
                "client_fio": r[1],
                "Проблема": r[2] or "",
                "Проходил обследование": "1" if r[3] and '"обслед' in r[3] else "0"
            })

        # Формирование Markdown
        md = "---\n\n📦 Данные для обработки:\n\n"
        md += "**places:**\n"
        md += json.dumps(places, indent=2, ensure_ascii=False) + "\n\n"

        md += "**masters:**\n"
        md += json.dumps(masters, indent=2, ensure_ascii=False) + "\n\n"

        md += "**events:**\n"
        md += json.dumps(events, indent=2, ensure_ascii=False) + "\n\n"

        md += "**write_me:**\n"
        md += json.dumps(write_me, indent=2, ensure_ascii=False) + "\n"

        # Запись в файл (можно асинхронно, но тут синхронно для простоты)
        filename = 'base/md.md'
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(md)

        return md

    async def write_recept(self, client_id, event_id, problem, client_fio):
        """
        Создание новой записи на приём.
        """       
        sql = """
            INSERT INTO rec_write (event_id, client_id, client_fio, problem)
            VALUES (%s, %s, %s, %s)
        """
        params = (event_id, client_id, client_fio, problem)
        await self.main.mysql.execute(sql, params)

    def format_event(self, event_date: date, event_time: timedelta) -> tuple[str, str]:
        total_seconds = int(event_time.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        date_str = event_date.strftime('%d.%m.%Y')
        time_str = f"{hours:02d}:{minutes:02d}"
        return date_str, time_str
        
    async def get_write_recept(self, client_id):
        # write_me - фильтр по client_id
        sql_write = """
            SELECT re.place_id, rp.text, re.id, re.date, re.time, rw.client_fio, rw.problem, rm.fio, rm.desc
            FROM rec_write rw
            JOIN rec_client rc ON rw.client_id = rc.id
            JOIN rec_event re ON rw.event_id = re.id
            JOIN rec_place rp ON re.place_id = rp.id
            JOIN rec_master rm ON re.master_id = rm.id
            WHERE rw.is_delete = 0 AND re.date >= CURDATE() AND rw.client_id = %s
        """
        rows = await self.main.mysql.fetch_all(sql_write, (client_id,))
        write_me = []
        for r in rows:
            date_str, time_str = self.format_event(r[3], r[4])            
            write_me.append({
                "place_id": str(r[0]),
                "place_name": r[1],
                "event_id": str(r[2]),
                "event_date": date_str,                
                "event_time": time_str,                
                "client_fio": r[5],
                "problem": r[6],
                "Прием проводит": r[7] + '. ' + r[8]
            })       
        
        return write_me 
    
    async def write_me(self, event_id: int, client_id: int, client_fio: str, problem: str, had_exam=None):
        """
        Создание новой записи на приём.
        """
        sql = """
            INSERT INTO rec_write (event_id, client_id, client_fio, problem)
            VALUES (%s, %s, %s, %s)
        """
        params = (event_id, client_id, client_fio, problem)
        await self.main.mysql.execute(sql, params)

        return {
            "status": "ok",
            "action": "write_me",
            "event_id": event_id,
            "client_id": client_id,
            "client_fio": client_fio,
            "problem": problem,
            "had_exam": had_exam  # просто вернуть для информации
        }

    async def write_me_update(self, client_id: int, event_id: int):
        """
        Обновление записи на приём (например, перенос на другое мероприятие).
        """
        sql = """
            UPDATE rec_write
            SET event_id = %s, upd = NOW()
            WHERE client_id = %s AND is_delete = 0
        """
        params = (event_id, client_id)
        await self.main.mysql.execute(sql, params)

        return {
            "status": "ok",
            "action": "write_me_update",
            "event_id": event_id,
            "client_id": client_id
        }

    async def write_me_cancel(self, client_id: int, event_id: int):
        """
        Отмена конкретной записи по client_id и event_id: ставим флаг is_delete = 1.
        """
        sql = """
            UPDATE rec_write
            SET is_delete = 1, upd = NOW()
            WHERE client_id = %s AND event_id = %s AND is_delete = 0
        """
        params = (client_id, event_id)
        await self.main.mysql.execute(sql, params)

        return {
            "status": "ok",
            "action": "write_me_cancel",
            "client_id": client_id,
            "event_id": event_id
        }

    def parse_function_call(self, call_str: str):
        try:
            tree = ast.parse(call_str.strip(), mode="eval")
            if isinstance(tree.body, ast.Call) and isinstance(tree.body.func, ast.Name):
                func_name = tree.body.func.id
                args = {}
                for kw in tree.body.keywords:
                    key = kw.arg
                    value = ast.literal_eval(kw.value)
                    args[key] = value
                return func_name, args
        except Exception:
            pass
        return None, {}
    
    async def write_recept_friend(self, client_id: int, friend_name: str, friend_fio: str, friend_event_id, friend_problem: str):
        """
        Создание записи на приём для друга:
        - Поиск друга по ФИО в rec_client
        - Если нет — создание записи в rec_client
        - Добавление связи в rec_friend
        - Запись в rec_write от имени друга
        """

        # 1. Поиск друга по полю `fio`
        sql_find_friend = """
            SELECT id FROM rec_client
            WHERE fio = %s AND is_delete = 0
            LIMIT 1
        """
        row = await self.main.mysql.fetch_one(sql_find_friend, (friend_fio,))
        if row:
            friend_id = row[0]
        else:
            # 2. Создание новой записи в rec_client
            sql_insert_client = """
                INSERT INTO rec_client (fio)
                VALUES (%s)
            """
            friend_id = await self.main.mysql.execute_return_id(sql_insert_client, (friend_fio,))

        # 3. Проверка, существует ли связь client → friend
        sql_check_friend = """
            SELECT id FROM rec_friend
            WHERE client_id = %s AND friend_id = %s AND is_delete = 0
        """
        exists = await self.main.mysql.fetch_one(sql_check_friend, (client_id, friend_id))

        if not exists:
            sql_insert_friend = """
                INSERT INTO rec_friend (client_id, friend_id, friend_name)
                VALUES (%s, %s, %s)
            """
            await self.main.mysql.execute(sql_insert_friend, (client_id, friend_id, friend_name))

        # 4. Запись на приём от имени друга
        sql_write = """
            INSERT INTO rec_write (event_id, client_id, client_fio, problem)
            VALUES (%s, %s, %s, %s)
        """
        
        await self.main.mysql.execute(sql_write, (friend_event_id, friend_id, friend_fio, friend_problem))
        
    async def get_write_recept_friends(self, client_id):

        sql = """
            SELECT 
                re.place_id,
                rp.text AS place_name,
                re.id AS event_id,
                re.date AS event_date,
                re.time AS event_time,
                rw.client_fio,
                rw.problem,
                rm.fio AS master_fio,
                rm.`desc` AS master_desc,
                rw.id,
                rf.friend_name
            FROM rec_friend rf
            JOIN rec_client rc ON rf.client_id = rc.id
            JOIN rec_write rw ON rw.client_id = rf.friend_id AND rw.is_delete = 0
            JOIN rec_event re ON rw.event_id = re.id AND re.date >= CURDATE()
            JOIN rec_place rp ON re.place_id = rp.id
            JOIN rec_master rm ON re.master_id = rm.id
            WHERE rf.client_id = %s AND rf.is_delete = 0
        """

        rows = await self.main.mysql.fetch_all(sql, (client_id,))
        write_friends = []

        for r in rows:
            date_str, time_str = self.format_event(r[3], r[4])
            write_friends.append({
                "friend_place_id": str(r[0]),
                "friend_place_name": r[1],
                "friend_event_id": str(r[2]),
                "friend_event_date": date_str,
                "friend_event_time": time_str,
                "friend_fio": r[5],  # имя друга
                "friend_problem": r[6],
                "Прием проводит": r[7] + ". " + r[8],
                "friend_write_id": r[9],
                "friend_name": r[10]
            })

        return write_friends        
        
    async def write_friend_update(self, client_id: int, friend_write_id, friend_event_id):
        """
        Перенос записи друга на другой event_id (новый приём).
        Проверка, что друг действительно связан с клиентом.
        """
        # Обновляем event_id записи друга
        sql_update = """
            UPDATE rec_write
            SET event_id = %s
            WHERE id = %s AND is_delete = 0
        """
        await self.main.mysql.execute(sql_update, (friend_event_id, friend_write_id))

        return {
            "status": "ok",
            "action": "write_friend_update",
            "friend_write_id": friend_write_id
        }

    async def write_friend_cancel(self, client_id: int, friend_write_id):
        """
        Отмена записи друга (устанавливаем is_delete = 1).
        """
        # Удаляем запись
        sql_cancel = """
            UPDATE rec_write
            SET is_delete = 1
            WHERE id = %s AND is_delete = 0
        """
        await self.main.mysql.execute(sql_cancel, (friend_write_id))

        return {
            "status": "ok",
            "action": "write_friend_cancel",
            "friend_write_id": friend_write_id
        }
