# main.py 
# Бот "Телеграм"

# импорт классов приложения
from classes.env import Env             # класс для работы с переменными окружения
from classes.log import Log             # класс для работы с журналом
from classes.mysql import MySQL         # класс для работы с MySQL
from classes.chunks import Chunks       # класс для поиска релевантных чанков
from classes.gpt import Gpt             # класс для работы с GPT
from classes.google import Google       # класс для работы с Google
from classes.request import Request     # класс для работы с запросами
from classes.telegram import Telegram   # класс для работы с Телеграм-ботом

# КЛАСС: main
class Main:

    # МЕТОД: конструктор
    def __init__(self):
    
        # создание объектов
        self.env = Env()                # объект класса для работы с переменными окружения
        self.log = Log()                # объект класса для работы с журналом
        self.mysql = MySQL(self)        # объект класса для работы с MySQL
        self.chunks = Chunks(self)      # объект класса для поиска релевантных чанков
        self.gpt = Gpt(self)            # объект класса для работы с GPT
        self.google = Google(self)      # объект класса для работы с Google
        self.request = Request(self)    # объект класса для работы с запросами
        self.telegram = Telegram(self)  # объект класса для работы с Телеграм-ботом
              
# запуск приложения  
if __name__ == '__main__':
    
    # создание приложения
    app = Main()
