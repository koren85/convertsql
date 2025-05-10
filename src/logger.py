import os
import json
import logging
from datetime import datetime

class Logger:
    def __init__(self, config):
        self.config = config
        self.log_dir = config.LOGS_DIR
        
        # Настройка логгера для файлов
        self.logger = logging.getLogger('sql_migrator')
        self.logger.setLevel(logging.INFO)
        
        # Проверяем, есть ли уже обработчики, чтобы избежать дублирования
        if not self.logger.handlers:
            # Логирование в консоль
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
            
            # Логирование в файл
            log_file = os.path.join(self.log_dir, f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def log_script_processing(self, script_name, stage, status, details=None):
        """
        Логирует процесс обработки скрипта
        """
        log_entry = {
            'script': script_name,
            'stage': stage,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details
        }
        
        # Логировать в консоль и файл
        if status == 'success':
            self.logger.info(f"{script_name} - {stage}: {status}")
        else:
            self.logger.error(f"{script_name} - {stage}: {status}")
            if details:
                self.logger.error(f"Details: {details}")
        
        # Сохранить детальную информацию в JSON
        json_log_file = os.path.join(self.log_dir, f'{script_name}.json')
        self._append_json_log(json_log_file, log_entry)
    
    def _append_json_log(self, json_file, log_entry):
        """
        Добавляет запись лога в JSON файл
        """
        entries = []
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r') as f:
                    entries = json.load(f)
                    if not isinstance(entries, list):
                        entries = [entries]
            except json.JSONDecodeError:
                entries = []
        
        entries.append(log_entry)
        
        with open(json_file, 'w') as f:
            json.dump(entries, f, indent=2)
