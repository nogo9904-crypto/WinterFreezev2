import os
import subprocess
import sys

# Список модулей
REQUIRED_PACKAGES = [
    "aiogram>=3.0.0",
    "telethon",
    "aiofiles",
    "aiosqlite",
    "pysocks",
    "requests"
]

def run_command(cmd, description):
    print(f"→ {description}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка: {e.stderr.strip()}")
        return False

def main():
    project_path = os.path.dirname(os.path.abspath(__file__))
    venv_path = os.path.join(project_path, "venv")
    bot_file = os.path.join(project_path, "Winter_Freeze_RES.py")

    print("🚀 Запуск установки Winter Freeze...\n")

    # 1. Создаём виртуальное окружение
    if not os.path.exists(venv_path):
        print("📦 Создаём виртуальное окружение...")
        if not run_command(f"python3 -m venv {venv_path}", "Создание venv"):
            return
    else:
        print("✅ Виртуальное окружение уже существует")

    pip_path = os.path.join(venv_path, "bin", "pip")
    python_path = os.path.join(venv_path, "bin", "python")

    # 2. Обновляем pip
    run_command(f"{pip_path} install --upgrade pip setuptools wheel", "Обновление pip")

    # 3. Устанавливаем модули
    print("📥 Устанавливаем зависимости...")
    packages_str = " ".join(REQUIRED_PACKAGES)
    if run_command(f"{pip_path} install {packages_str}", "Установка модулей"):
        print("\n✅ Все модули успешно установлены!")
    else:
        print("\n⚠️ Установка прошла с ошибками")

    # 4. Проверяем наличие основного скрипта
    if not os.path.exists(bot_file):
        print(f"❌ Файл {bot_file} не найден!")
        print("Создай файл Winter_Freeze_RES.py перед запуском установки.")
        return

    # 5. Запускаем бот
    print("\n▶️ Запускаем Winter_Freeze_RES.py ...")
    try:
        subprocess.run(f"{python_path} Winter_Freeze_RES.py", shell=True, cwd=project_path)
    except KeyboardInterrupt:
        print("\n\n⛔ Бот остановлен вручную (Ctrl+C)")
    except Exception as e:
        print(f"❌ Ошибка при запуске бота: {e}")

    print("\n✅ Установка и запуск завершены!")

if __name__ == "__main__":
    main()