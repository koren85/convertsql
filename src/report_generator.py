import os
import json
import pandas as pd
from datetime import datetime

class ReportGenerator:
    def __init__(self, config):
        self.config = config
        self.reports_dir = config.REPORTS_DIR

    def generate_summary_report(self, batch_report_path):
        """
        Генерирует итоговый отчет о конвертации всех скриптов по итоговому json-отчёту
        """
        # Читаем итоговый json-отчёт
        with open(batch_report_path, 'r', encoding='utf-8') as f:
            batch_report = json.load(f)
        results = batch_report.get('results', [])
        if not results:
            print("Нет данных для отчёта (results пустой)")
            return None
        # Готовим DataFrame
        df = pd.DataFrame(results)
        df['Status'] = df.apply(lambda row: 
                            'missing_table' if row.get('missing_table', False) else 
                            ('needs_manual' if row.get('manual_processing', False) else 
                            ('success' if row['success'] else 'failed')), axis=1)
        df['Error'] = df['error'].fillna('')
        # Для красоты
        df['Script'] = df['script']
        # Считаем статистику
        total_scripts = len(df)
        successful_scripts = (df['Status'] == 'success').sum()
        failed_scripts = (df['Status'] == 'failed').sum()
        manual_scripts = (df['Status'] == 'needs_manual').sum()
        missing_table_scripts = (df['Status'] == 'missing_table').sum()
        
        # Используем статистику из отчета, если она доступна
        if 'success_count' in batch_report and 'manual_count' in batch_report and 'missing_table_count' in batch_report:
            successful_scripts = batch_report['success_count']
            manual_scripts = batch_report['manual_count']
            missing_table_scripts = batch_report['missing_table_count']
            failed_scripts = batch_report.get('failed_count', failed_scripts)
        
        # HTML отчёт
        report_html = f"""
        <html>
        <head>
            <meta charset="utf-8">
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
                .needs_manual {{ color: orange; }}
                .missing_table {{ color: blue; }}
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
                <p>Needs Manual Processing: <span class="needs_manual">{manual_scripts}</span></p>
                <p>Missing Tables: <span class="missing_table">{missing_table_scripts}</span></p>
                <p>Success Rate: {(successful_scripts / total_scripts * 100) if total_scripts > 0 else 0:.2f}%</p>
            </div>
            <h2>Script Details</h2>
            <table>
                <tr>
                    <th>Script</th>
                    <th>Status</th>
                    <th>Error / Message</th>
                </tr>
        """
        for _, row in df.iterrows():
            status_class = row['Status']  # success, needs_manual, failed, missing_table
            
            if status_class == 'success':
                status_text = "Success"
            elif status_class == 'needs_manual':
                status_text = "Needs Manual Processing"
            elif status_class == 'missing_table':
                status_text = "Missing Table"
            else:
                status_text = "Failed"
                
            report_html += f"""
                <tr>
                    <td>{row['Script']}</td>
                    <td class=\"{status_class}\">{status_text}</td>
                    <td>{row['Error']}</td>
                </tr>
            """
        report_html += """
            </table>
        </body>
        </html>
        """
        # Сохраняем отчёты
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_html_path = os.path.join(self.reports_dir, f'migration_report_{ts}.html')
        with open(report_html_path, 'w', encoding='utf-8') as f:
            f.write(report_html)
        excel_path = os.path.join(self.reports_dir, f'migration_report_{ts}.xlsx')
        df[['Script', 'Status', 'Error']].to_excel(excel_path, index=False)
        json_path = os.path.join(self.reports_dir, f'migration_report_{ts}.json')
        df[['Script', 'Status', 'Error']].to_json(json_path, orient='records', force_ascii=False, indent=2)
        return {
            'html_report': report_html_path,
            'excel_report': excel_path,
            'json_report': json_path
        }
