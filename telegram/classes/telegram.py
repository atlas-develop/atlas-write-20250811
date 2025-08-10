# classes/telegram.py
# Класс "Телеграм-бот"

# импорт модулей
from telegram import Update                         # объект события
from telegram.ext import Application                # основной класс приложения для телеграм-бота
from telegram.ext import CommandHandler             # класс для обработки команд
from telegram.ext import MessageHandler             # класс для обработки сообщений
from telegram.ext import filters                    # фильтры для сообщений
from telegram.ext import ContextTypes               # класс для типов контекста
import asyncio                                      # запуск асинхронного кода

# класс
class Telegram():
      
    # МЕТОД: инициализация
    #   main - главный объект
    def __init__(self, main):
        
        # запоминаем main
        self.main = main
        
        # запуск бота
        self.run()        
        
    # МЕТОД: обработчик команды /start
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):   
        if update.message.chat.id == int(self.main.env.get('SUPPORT_GROUP_ID')): return
        
        # Очистка диалога
        client_id = await self.main.request.clear_dialog(update)
        
        # Приветственное сообщение
        text = 'Здравствуйте! Я консультант медицинского центра. Я помогу вам записаться на групповую консультацию.'
        text += 'Выберите из предложенных удобное вам дату и время для записи и ответьте на ряд вопросов.'
        text += 'Если вы решите перенести запись на другое время или отменить запись, то я тоже готов вам помочь.'
        text += '\n\nВы хотите записаться на прием?'
        await update.message.reply_text(text)
        
        # Сохранение в диалог
        await self.main.request.save_message(client_id, "assistant", text, 0, 0)
 
    # функция-обработчик текстовых сообщений
    async def text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):       
        if update.message.chat.id == int(self.main.env.get('SUPPORT_GROUP_ID')): return
        
        # Обработка текстового сообщения
        await self.main.request.handle_message(update, context)
        
    # МЕТОД: получение имени бота
    async def get_bot_name(self, app:Application) -> str:
        bot = app.bot
        me = await bot.get_me()
        return me.username              
                
    def run(self):
        # создаем приложение
        app = Application.builder().token(self.main.env.get('TELEGRAM_TOKEN')).build()

        # добавление обработчиков
        app.add_handler(CommandHandler('start', self.start, block=False))
        app.add_handler(MessageHandler(filters.TEXT, self.text, block=False))

        # определение имени бота
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        bot_name = loop.run_until_complete(self.get_bot_name(app))

        # запуск бота (нажать Ctrl+C для остановки)
        self.main.log.log_info_sync('telegram', f'Телеграм бот {bot_name} запущен...', {}, True)
        try:
            app.run_polling()
        finally:        
            if hasattr(self.main, 'google') and self.main.google.processing_task:
                loop.run_until_complete(self.main.google.shutdown())
            self.main.log.log_info_sync('telegram', '\nБот остановлен', {}, True)
