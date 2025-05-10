import re
import sqlparse

class SQLParser:
    def __init__(self, config):
        self.config = config
        
    def parse_script(self, script_content):
        """
        Парсит SQL скрипт, выделяя параметры и структуру запроса
        """
        # Найти все параметры в формате {param_name}
        params = re.findall(r'\{([^\}]+)\}', script_content)
        unique_params = list(set(params))
        
        # Разбить скрипт на отдельные запросы
        statements = sqlparse.split(script_content)
        
        return {
            'params': unique_params,
            'statements': statements,
            'original': script_content
        }
    
    def replace_params(self, script_content, params_dict=None):
        """
        Заменяет параметры в скрипте на значения по умолчанию или из словаря
        """
        if params_dict is None:
            params_dict = getattr(self.config, 'DEFAULT_PARAMS', {})
            
        # 1. Заменить параметры в формате {param}
        for param in re.findall(r'\{([^\}]+)\}', script_content):
            param_name = param
            
            if param.startswith('params.'):
                # Если параметр в формате {params.param}
                param_name = param.replace('params.', '')
            
            # Определяем, нужно ли значение в кавычках или без
            if param_name in params_dict:
                # Используем значение из словаря
                param_value = params_dict.get(param_name)
                script_content = script_content.replace(f'{{{param}}}', str(param_value))
            else:
                # Если параметр неизвестен, проверяем тип параметра по его имени
                if 'id' in param_name.lower() or 'ouid' in param_name.lower() or 'code' in param_name.lower():
                    # Для ID используем числовое значение без кавычек
                    script_content = script_content.replace(f'{{{param}}}', '1')
                else:
                    # Для других параметров используем строковое значение
                    script_content = script_content.replace(f'{{{param}}}', f"'default_{param_name}'")
        
        # 2. Заменить параметры в формате params.param или default_params.param
        for param_prefix in ['params.', 'default_params.']:
            param_pattern = rf'{param_prefix}([a-zA-Z0-9_]+)'
            for match in re.finditer(param_pattern, script_content):
                param_name = match.group(1)
                full_param = match.group(0)  # Полное совпадение: params.name или default_params.name
                
                if param_name in params_dict:
                    # Используем значение из словаря
                    param_value = params_dict.get(param_name)
                    script_content = script_content.replace(full_param, str(param_value))
                else:
                    # Если параметр неизвестен, проверяем тип параметра по его имени
                    if 'id' in param_name.lower() or 'ouid' in param_name.lower() or 'code' in param_name.lower():
                        # Для ID используем числовое значение без кавычек
                        script_content = script_content.replace(full_param, '1')
                    else:
                        # Для других параметров используем строковое значение
                        script_content = script_content.replace(full_param, f"'default_{param_name}'")
                
        return script_content
