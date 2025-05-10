import os
import json
import pandas as pd
from datetime import datetime

class ReportGenerator:
    def __init__(self, config):
        self.config = config
        self.logs_dir = config.LOGS_DIR
        self.reports_dir = config.REPORTS_DIR
    
    def generate_summary_report(self):
        """
        Генерирует итоговый отчет о конвертации всех скриптов
        """
        logs = self._collect_all_logs()
        
        if not logs:
            print("No logs found. Cannot generate report.")
            return
        
        # Создаем DataFrame из логов
        df = pd.DataFrame(logs)
        
        # Группируем по имени скрипта
        grouped = df.groupby('script').agg({
            'status': lambda x: 'success' if all(s == 'success' for s in x) else 'failed',
            'timestamp': 'max'
        }).reset_index()
        
        # Переименовываем колонки
        grouped.columns = ['Script', 'Status', 'Last Updated']
        
        # Дополнительная информация
        total_scripts = grouped.shape[0]
        successful_scripts = sum(grouped['Status'] == 'success')
        failed_scripts = total_scripts - successful_scripts
        
        # Создаем отчет в HTML
        report_html = f"""
        <html>
        <head>
            <title>SQL Migration Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #2c3e50; }}
                .summary {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .success {{ color: green; }}
                .failed {{ color: red; }}
            </style>
        </head>
        <body>
            <h1>SQL Migration Report</h1>
            <div class="summary">
                <h2>Summary</h2>
                <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                <p>Total Scripts: {total_scripts}</p>
                <p>Successful: <span class="success">{successful_scripts}</span></p>
                <p>Failed: <span class="failed">{failed_scripts}</span></p>
                <p>Success Rate: {(successful_scripts / total_scripts * 100) if total_scripts > 0 else 0:.2f}%</p>
            </div>
            
            <h2>Script Details</h2>
            <table>
                <tr>
                    <th>Script</th>
                    <th>Status</th>
                    <th>Last Updated</th>
                </tr>
        """
        
        # Добавляем строки таблицы
        for _, row in grouped.iterrows():
            status_class = "success" if row['Status'] == 'success' else "failed"
            report_html += f"""
                <tr>
                    <td>{row['Script']}</td>
                    <td class="{status_class}">{row['Status']}</td>
                    <td>{row['Last Updated']}</td>
                </tr>
            """
        
        report_html += """
            </table>
        </body>
        </html>
        """
        
        # Сохраняем отчет
        report_path = os.path.join(self.reports_dir, f'migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html')
        with open(report_path, 'w') as f:
            f.write(report_html)
        
        # Создаем также Excel-отчет
        excel_path = os.path.join(self.reports_dir, f'migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
        grouped.to_excel(excel_path, index=False)
        
        # Подробный отчет в формате JSON
        detailed_report = {
            'summary': {
                'total_scripts': total_scripts,
                'successful_scripts': successful_scripts,
                'failed_scripts': failed_scripts,
                'success_rate': (successful_scripts / total_scripts * 100) if total_scripts > 0 else 0,
                'generated_at': datetime.now().isoformat()
            },
            'scripts': self._get_detailed_script_info()
        }
        
        json_path = os.path.join(self.reports_dir, f'migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(json_path, 'w') as f:
            json.dump(detailed_report, f, indent=2)
        
        return {
            'html_report': report_path,
            'excel_report': excel_path,
            'json_report': json_path
        }
    
    def _collect_all_logs(self):
        """
        Собирает все логи из файлов
        """
        logs = []
        for filename in os.listdir(self.logs_dir):
            if filename.endswith('.json'):
                try:
                    with open(os.path.join(self.logs_dir, filename), 'r') as f:
                        file_logs = json.load(f)
                        if isinstance(file_logs, list):
                            logs.extend(file_logs)
                        else:
                            logs.append(file_logs)
                except Exception as e:
                    print(f"Error reading log file {filename}: {str(e)}")
        return logs
    
    def _get_detailed_script_info(self):
        """
        Собирает детальную информацию о каждом скрипте
        """
        script_info = {}
        for filename in os.listdir(self.logs_dir):
            if filename.endswith('.json'):
                script_name = filename[:-5]  # удаляем .json
                try:
                    with open(os.path.join(self.logs_dir, filename), 'r') as f:
                        logs = json.load(f)
                        script_info[script_name] = logs
                except Exception as e:
                    print(f"Error reading log file {filename}: {str(e)}")
        return script_info
