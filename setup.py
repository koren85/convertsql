#!/usr/bin/env python3
"""
Скрипт для настройки окружения проекта
"""

import os
import sys
import subprocess
import platform
import shutil
from pathlib import Path

def check_python_version():
    """Проверяет версию Python"""
    print("Проверка версии Python...")
    required_version = (3, 7)
    current_version = sys.version_info
    
    if current_version < required_version:
        print(f"Ошибка: требуется Python {required_version[0]}.{required_version[1]} или выше")
        print(f"Текущая версия: {current_version[0]}.{current_version[1]}")
        return False
    
    print(f"OK: Python {current_version[0]}.{current_version[1]}.{current_version[2]}")
    return True

def check_docker():
    """Проверяет наличие Docker"""
    print("\nПроверка Docker...")
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"OK: {result.stdout.strip()}")
            return True
        else:
            print("Ошибка: Docker не установлен или не запущен")
            return False
    except FileNotFoundError:
        print("Ошибка: Docker не установлен или не найден в PATH")
        return False

def setup_virtualenv():
    """Создает виртуальное окружение и устанавливает зависимости"""
    print("\nНастройка виртуального окружения...")
    
    # Проверяем, существует ли уже venv
    venv_dir = Path("venv")
    if venv_dir.exists():
        response = input("Виртуальное окружение уже существует. Пересоздать? (y/n): ")
        if response.lower() == 'y':
            shutil.rmtree(venv_dir)
        else:
            print("Пропускаем создание виртуального окружения")
            return True
    
    # Создаем виртуальное окружение
    print("Создание виртуального окружения...")
    try:
        subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)
    except subprocess.CalledProcessError:
        print("Ошибка: не удалось создать виртуальное окружение")
        return False
    
    # Определяем путь к pip и скрипту активации
    if platform.system() == "Windows":
        pip_path = os.path.join("venv", "Scripts", "pip")
        activate_path = os.path.join("venv", "Scripts", "activate")
    else:  # Unix/Linux/MacOS
        pip_path = os.path.join("venv", "bin", "pip")
        activate_path = os.path.join("venv", "bin", "activate")
    
    # Обновляем pip
    print("Обновление pip...")
    try:
        subprocess.run([pip_path, "install", "--upgrade", "pip"], check=True)
    except subprocess.CalledProcessError:
        print("Предупреждение: не удалось обновить pip")
    
    # Устанавливаем зависимости
    print("Установка зависимостей...")
    try:
        subprocess.run([pip_path, "install", "-r", "requirements.txt"], check=True)
    except subprocess.CalledProcessError:
        print("Ошибка: не удалось установить зависимости")
        return False
    
    print(f"OK: виртуальное окружение настроено")
    print(f"Для активации используйте: source {activate_path} (Unix/MacOS) или {activate_path} (Windows)")
    return True

def setup_docker():
    """Настраивает Docker контейнер с PostgreSQL"""
    print("\nНастройка Docker контейнера...")
    
    # Проверяем, запущен ли уже контейнер
    try:
        result = subprocess.run(["docker", "ps", "-q", "-f", "name=postgres_test"], 
                               capture_output=True, text=True)
        if result.stdout.strip():
            print("Контейнер postgres_test уже запущен")
            return True
    except subprocess.CalledProcessError:
        print("Ошибка при проверке запущенных контейнеров")
    
    # Запускаем контейнер
    print("Запуск контейнера PostgreSQL...")
    try:
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        print("OK: контейнер успешно запущен")
        return True
    except subprocess.CalledProcessError:
        print("Ошибка при запуске контейнера")
        return False

def main():
    """
    Основная функция для настройки окружения
    """
    print("="*80)
    print("Настройка окружения для проекта SQL Converter")
    print("="*80)
    
    # Проверяем версию Python
    if not check_python_version():
        return 1
    
    # Проверяем Docker
    if not check_docker():
        print("\nПредупреждение: Docker не найден или не запущен")
        print("Для работы конвертера требуется Docker с PostgreSQL")
        print("Установите Docker и запустите docker-compose up -d вручную")
    else:
        # Настраиваем Docker-контейнер
        setup_docker()
    
    # Настраиваем виртуальное окружение
    if not setup_virtualenv():
        return 1
    
    print("\n" + "="*80)
    print("Настройка окружения завершена успешно!")
    print("="*80)
    print("\nДля запуска конвертера используйте:")
    print("1. Активируйте виртуальное окружение")
    print("2. python main.py <путь_к_скриптам> --report")
    print("\nДля тестирования одного скрипта:")
    print("python test_converter.py <путь_к_скрипту>")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
