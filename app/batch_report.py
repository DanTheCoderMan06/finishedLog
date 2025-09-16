import os
import sys
import shutil
import main
import traceback
from tqdm import tqdm
import json
from datetime import datetime, timedelta
import pandas as pd

def batch_parse(report_dir, start_dir, max_files=None, show_errors=False):
    results = []
    processed_files = 0

    dir_list = os.listdir(start_dir)
    
    cache_path = os.path.join(os.path.dirname(report_dir), 'cache.json')
    try:
        with open(cache_path, 'r') as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {}

    # Load clean run errors cache
    clean_run_cache_path = os.path.join(os.path.dirname(report_dir), 'clean_run_errors_cache.parquet')
    clean_run_errors = {}
    try:
        df = pd.read_parquet(clean_run_cache_path)
        clean_run_errors = {}
        for _, row in df.iterrows():
            ruid = row['ruid']
            shardgroup = row['shard_group']
            error = dict(row)
            if ruid not in clean_run_errors:
                clean_run_errors[ruid] = {}
            if shardgroup not in clean_run_errors[ruid]:
                clean_run_errors[ruid][shardgroup] = []
            clean_run_errors[ruid][shardgroup].append(error)
    except (FileNotFoundError, pd.errors.EmptyDataError):
        clean_run_errors = {}
        print("No clean run errors cache found.")

    now = datetime.now()
    

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
                            if dir_name in cache:
                                old_last_accessed = cache[dir_name].get('last_accessed')
                                if old_last_accessed:
                                    cache[dir_name]['lastReset'] = old_last_accessed
                                else:
                                    cache[dir_name]['lastReset'] = cache[dir_name]['date']
                                cache[dir_name]['last_accessed'] = now.isoformat()
                                last_reset = datetime.fromisoformat(cache[dir_name]['lastReset'])
                                if (now - last_reset).days >= 10:
                                    continue  # drop it
                                else:
                                    days_existed = (now - datetime.fromisoformat(cache[dir_name]['date'])).days
                            else:
                                cache[dir_name] = {'date': now.isoformat(), 'lastReset': now.isoformat()}
                                cache[dir_name]['last_accessed'] = now.isoformat()
                                days_existed = 0
                            
                            details = ""
                            if log_contents:
                                if any('blowout' in str(val) for val in log_contents.values()):
                                    details += "Found 'blowout' in logs.<br>"
                                if any('sdbcr' in str(val) for val in log_contents.values()):
                                    details += "Found 'sdbcr' in logs.<br>"
                                if show_errors and log_contents.get('trace_errors'):
                                    error_links = []
                                    for error in log_contents['trace_errors']:
                                        file_path = error.get('ospFile') if error.get('ospFile') else error.get('file')
                                        line_number = error.get('line')
                                        if file_path and os.path.exists(file_path):
                                            link = f'<a href="{os.path.join(dir_name, os.path.basename(file_path))}#line{line_number}" target="_blank">{os.path.basename(file_path)}</a>'
                                            error_links.append(link)
                                    if error_links:
                                        details += f"Incidents: {', '.join(error_links)}<br>"

                                # Add clean run diff info to details
                                clean_run_diff = log_contents.get('clean_run_diff', [])
                                if clean_run_diff:
                                    details += f"New errors since clean run: {len(clean_run_diff)}<br>"
                                else:
                                    details += "No new errors since clean run.<br>"

                            new_errors_count = len(log_contents.get('clean_run_diff', []))
                            results.append({'dir': dir_name, 'status': 'Success', 'details': details, 'log_contents': log_contents, 'is_new': is_new, 'days_existed': days_existed, 'first_seen': cache[dir_name]['date'], 'last_prev_seen': cache[dir_name]['lastReset'], 'current_date': cache[dir_name]['last_accessed'], 'new_errors': new_errors_count})
                        except Exception as e:
                            error_message = f"{e}\n{traceback.format_exc()}"
                            results.append({'dir': dir_name, 'status': 'Failed', 'details': error_message, 'log_contents': None, 'is_new': False, 'days_existed': 0, 'first_seen': now.isoformat(), 'last_prev_seen': now.isoformat(), 'current_date': now.isoformat()})
    except KeyboardInterrupt:
        print("\nInterrupted by user. Stopping batch processing.")

    table_rows = ""
    processed_dirs = {result['dir'] for result in results}

    for dir_name, data in cache.items():
        if dir_name not in processed_dirs:
            report_path = os.path.join(report_dir, dir_name, 'index.html')
            log_path = os.path.join(start_dir, dir_name)
            if os.path.exists(report_path) and os.path.isdir(log_path):
                last_reset_str = data.get('lastReset', data['date'])
                last_reset = datetime.fromisoformat(last_reset_str)
                if (now - last_reset).days >= 10:
                    continue  # drop it
                else:
                    days_existed = (now - datetime.fromisoformat(data['date'])).days
                results.append({
                    'dir': dir_name,
                    'status': 'Cached',
                    'details': 'Report loaded from cache.',
                    'log_contents': None,
                    'is_new': False,
                    'days_existed': days_existed,
                    'first_seen': data['date'],
                    'last_prev_seen': last_reset_str,
                    'current_date': data['last_accessed']
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
        
        is_new = result.get('is_new')
        row_class = 'new-folder' if is_new else ''

        if original_status == 'Success':
            status = 'New' if is_new else 'Existing'
        elif original_status == 'Failed':
            status = 'New Failed' if is_new else 'Failed'
        else:
            status = original_status

        if original_status == 'Success' or original_status == 'Cached':
            link = f'<a href="{dir_name}/index.html">{dir_name}</a>'
        else:
            link = dir_name
            
        days_existed = result.get('days_existed', 0)
        first_seen = result.get('first_seen', '')
        last_prev_seen = result.get('last_prev_seen', '')
        current_date = result.get('current_date', '')
        new_errors_count = result.get('new_errors', 0)
        table_rows += f"""
        <tr class="{row_class}">
            <td>{link}</td>
            <td class="{status_class}">{status}</td>
            <td>{first_seen}</td>
            <td>{last_prev_seen}</td>
            <td>{current_date}</td>
            <td>{days_existed}</td>
            <td>{new_errors_count}</td>
            <td>{details}</td>
        </tr>
        """
    
    new_reports_count = sum(1 for r in results if r.get('is_new'))

    # Generate error tables for each LRG with new errors
    error_tables = ""
    for result in results:
        if result.get('status') == 'Success' and result.get('log_contents') and 'clean_run_diff' in result['log_contents'] and result['log_contents']['clean_run_diff']:
            dir_name = result['dir']
            error_tables += f"""
    <h2>New Errors for {dir_name}</h2>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th>Timestamp</th>
                    <th>Error Code</th>
                    <th>Message</th>
                    <th>File</th>
                </tr>
            </thead>
            <tbody>
"""
            for error in result['log_contents']['clean_run_diff']:
                timestamp = error.get('timestamp', '')
                code = str(error.get('code', ''))
                message = error.get('original', '')
                file_cell = "N/A"
                if 'ospFile' in error and error['ospFile']:
                    file_path = os.path.basename(error['ospFile'])
                    link = f'<a href="{dir_name}/{file_path}" target="_blank">{file_path}</a>'
                    if 'scrollIndex' in error:
                        link = f'<a href="{dir_name}/{file_path}#line{error["scrollIndex"]}" target="_blank">{file_path}</a>'
                    file_cell = link
                error_tables += f"""
                <tr>
                    <td>{timestamp}</td>
                    <td>{code}</td>
                    <td>{message}</td>
                    <td>{file_cell}</td>
                </tr>
"""
            error_tables += """
            </tbody>
        </table>
    </div>
"""

    # Generate aggregate table for all new errors from new directories
    new_errors_table = ""
    all_new_errors = []
    for result in results:
        if result.get('is_new') and result.get('log_contents') and 'clean_run_diff' in result['log_contents']:
            all_new_errors.extend(result['log_contents']['clean_run_diff'])

    if all_new_errors:
        new_errors_table = """
    <h2>All New Errors from New Directories</h2>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th>Directory</th>
                    <th>Timestamp</th>
                    <th>Error Code</th>
                    <th>Message</th>
                    <th>File</th>
                </tr>
            </thead>
            <tbody>
"""
        for result in results:
            if result.get('is_new') and result.get('log_contents') and 'clean_run_diff' in result['log_contents']:
                dir_name = result['dir']
                for error in result['log_contents']['clean_run_diff']:
                    timestamp = error.get('timestamp', '')
                    code = str(error.get('code', ''))
                    message = error.get('original', '')
                    file_cell = "N/A"
                    if 'ospFile' in error and error['ospFile']:
                        file_path = os.path.basename(error['ospFile'])
                        link = f'<a href="{dir_name}/{file_path}" target="_blank">{file_path}</a>'
                        if 'scrollIndex' in error:
                            link = f'<a href="{dir_name}/{file_path}#line{error["scrollIndex"]}" target="_blank">{file_path}</a>'
                        file_cell = link
                    new_errors_table += f"""
                <tr>
                    <td>{dir_name}</td>
                    <td>{timestamp}</td>
                    <td>{code}</td>
                    <td>{message}</td>
                    <td>{file_cell}</td>
                </tr>
"""
        new_errors_table += """
            </tbody>
        </table>
    </div>
"""

    final_html = template_html.replace('{table_rows}', table_rows)
    final_html = final_html.replace('{source_dir}', start_dir)
    final_html = final_html.replace('{new_reports_count}', str(new_reports_count))
    final_html = final_html.replace('<th>Details</th>', '<th>New Errors</th><th>Details</th>')
    final_html = final_html.replace('{error_tables}', error_tables)
    final_html = final_html.replace('{new_errors_table}', new_errors_table)
    
    with open(os.path.join(report_dir, 'index.html'), 'w') as f:
        f.write(final_html)

    with open(cache_path, 'w') as f:
        json.dump(cache, f, indent=4)

    # cleanup_folders(report_dir, start_dir)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError("Usage: python batch_report.py <report_directory> <start_directory> [max_files] [show_errors]")
    report_directory = sys.argv[1]
    start_directory = sys.argv[2]
    max_files_arg = None
    show_errors_arg = False

    if len(sys.argv) > 3:
        try:
            max_files_arg = int(sys.argv[3])
        except (ValueError, IndexError):
            max_files_arg = None

    if len(sys.argv) > 4:
        try:
            show_errors_arg = sys.argv[4].lower() == 'true'
        except (ValueError, IndexError):
            show_errors_arg = False

    batch_parse(report_directory, start_directory, max_files_arg, show_errors_arg)