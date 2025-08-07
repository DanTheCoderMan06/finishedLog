import os
import sys
import shutil
import main
import traceback
from tqdm import tqdm
import json
from datetime import datetime, timedelta

def batch_parse(report_dir, start_dir, max_files=None):
    results = []
    processed_files = 0
    dir_list = os.listdir(start_dir)
    
    cache_path = os.path.join(os.path.dirname(report_dir), 'cache.json')
    try:
        with open(cache_path, 'r') as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {}

    now = datetime.now()
    
    ten_days_ago = now - timedelta(days=10)
    cache = {
        dir_name: data
        for dir_name, data in cache.items()
        if 'last_accessed' in data and datetime.fromisoformat(data['last_accessed']) >= ten_days_ago
    }

    template_path = os.path.join(os.path.dirname(__file__), 'html_assets', 'template', 'batch_report.html')
    with open(template_path, 'r') as f:
        template_html = f.read()
        
    css_path = os.path.join(os.path.dirname(__file__), 'html_assets', 'template', 'batch_report.css')
    shutil.copy(css_path, os.path.join(report_dir, 'style.css'))

    try:
        with tqdm(total=len(dir_list), desc="Processing directories") as pbar:
            for dir_name in dir_list:
                pbar.update(1)
                if not "snr" in dir_name:
                    continue
                if max_files is not None and processed_files >= max_files:
                    print(f"Reached file limit of {max_files}. Exiting.")
                    break
                full_path = os.path.join(start_dir, dir_name)
                if os.path.isdir(full_path):
                    diag_path = os.path.join(full_path, 'diag')
                    if os.path.exists(diag_path) and os.path.isdir(diag_path) and os.path.exists(os.path.join(diag_path, 'rdbms')):
                        try:
                            log_contents = main.parseLog(report_dir, full_path)
                            processed_files += 1
                            is_new = dir_name not in cache
                            days_existed = (now - datetime.fromisoformat(cache[dir_name]['date'])).days if dir_name in cache else 0
                            
                            details = ""
                            if log_contents:
                                if any('blowout' in str(val) for val in log_contents.values()):
                                    details += "Found 'blowout' in logs.<br>"
                                if any('sdbcr' in str(val) for val in log_contents.values()):
                                    details += "Found 'sdbcr' in logs.<br>"
                                if log_contents.get('trace_errors'):
                                    error_files = [os.path.basename(error.get('file', 'N/A')) for error in log_contents['trace_errors']]
                                    details += f"Incidents: {', '.join(error_files)}<br>"

                            results.append({'dir': dir_name, 'status': 'Success', 'details': details, 'log_contents': log_contents, 'is_new': is_new, 'days_existed': days_existed})
                            if dir_name not in cache:
                                cache[dir_name] = {'date': now.isoformat()}
                            cache[dir_name]['last_accessed'] = now.isoformat()
                        except Exception as e:
                            error_message = f"{e}\n{traceback.format_exc()}"
                            results.append({'dir': dir_name, 'status': 'Failed', 'details': error_message, 'log_contents': None, 'is_new': False, 'days_existed': 0})
    except KeyboardInterrupt:
        print("\nInterrupted by user. Stopping batch processing.")

    table_rows = ""
    processed_dirs = {result['dir'] for result in results}

    for dir_name, data in cache.items():
        if dir_name not in processed_dirs:
            report_path = os.path.join(report_dir, dir_name, 'index.html')
            log_path = os.path.join(start_dir, dir_name)
            if os.path.exists(report_path) and os.path.isdir(log_path):
                days_existed = (now - datetime.fromisoformat(data['date'])).days
                results.append({
                    'dir': dir_name,
                    'status': 'Cached',
                    'details': 'Report loaded from cache.',
                    'log_contents': None,
                    'is_new': False,
                    'days_existed': days_existed
                })


    for result in results:
        dir_name = result['dir']
        original_status = result['status']
        details = result['details']
        
        if original_status == 'Cached':
            status_class = 'status-cached'
        elif original_status == 'Success':
            status_class = 'status-success'
        else: # Failed
            status_class = 'status-failure'
        
        status = original_status
        is_new = result.get('is_new')
        row_class = 'new-folder' if is_new else ''

        if is_new:
            if original_status == 'Success':
                status = '(New) Success'
            elif original_status == 'Failed':
                status = '(New) Fail'

        if original_status == 'Success' or original_status == 'Cached':
            link = f'<a href="{dir_name}/index.html">{dir_name}</a>'
        else:
            link = dir_name
            
        days_existed = result.get('days_existed', 0)
        table_rows += f"""
        <tr class="{row_class}">
            <td>{link}</td>
            <td class="{status_class}">{status}</td>
            <td>{days_existed}</td>
            <td>{details}</td>
        </tr>
        """
    
    final_html = template_html.replace('{table_rows}', table_rows)
    
    with open(os.path.join(report_dir, 'index.html'), 'w') as f:
        f.write(final_html)

    with open(cache_path, 'w') as f:
        json.dump(cache, f, indent=4)

    # cleanup_folders(report_dir, start_dir)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError("Usage: python batch_report.py <report_directory> <start_directory> [max_files]")
    report_directory = sys.argv[1]
    start_directory = sys.argv[2]
    max_files_arg = None
    if len(sys.argv) > 3:
        try:
            max_files_arg = int(sys.argv[3])
        except ValueError:
            raise ValueError("max_files must be an integer.")
        

    batch_parse(report_directory, start_directory, max_files_arg)