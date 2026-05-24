from telethon.sync import TelegramClient

# Ваши данные
API_ID = 25874957
API_HASH = "c89ef6fd9ba5c8a479abb1f4d2de248d"
PHONE_NUMBER = "+18402542479"

# Имя файла сессии (расширение .session добавится автоматически)
SESSION_NAME = "TIDA_report"

def main():
    # Инициализация клиента
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    print(f"Подключение к Telegram. Ожидание кода подтверждения для {PHONE_NUMBER}...")
    
    # start() автоматически запросит код в консоли, а при необходимости и 2FA пароль
    client.start(phone=PHONE_NUMBER)
    
    print(f"Авторизация успешна! Файл сессии '{SESSION_NAME}.session' создан в текущей папке.")
    
    # Завершаем работу клиента
    client.disconnect()

if __name__ == '__main__':
    main()