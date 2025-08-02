import os
import sys
import shutil
import main
import traceback
from tqdm import tqdm

def batch_parse(report_dir, start_dir, max_files=None):
    results = []
    processed_files = 0
    dir_list = os.listdir(start_dir)
    
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
                            main.parseLog(report_dir, full_path)
                            processed_files += 1
                            results.append({'dir': dir_name, 'status': 'Success', 'details': ''})
                        except Exception as e:
                            error_message = f"{e}\n{traceback.format_exc()}"
                            results.append({'dir': dir_name, 'status': 'Failed', 'details': error_message})
    except KeyboardInterrupt:
        print("\nInterrupted by user. Stopping batch processing.")

    table_rows = ""
    for result in results:
        dir_name = result['dir']
        status = result['status']
        details = result['details']
        
        if status == 'Success':
            link = f'<a href="{dir_name}/index.html">{dir_name}</a>'
            status_class = 'status-success'
        else:
            link = dir_name
            status_class = 'status-failure'
            
        table_rows += f"""
        <tr>
            <td>{link}</td>
            <td class="{status_class}">{status}</td>
            <td><pre>{details}</pre></td>
        </tr>
        """
    
    final_html = template_html.replace('{table_rows}', table_rows)
    
    with open(os.path.join(report_dir, 'index.html'), 'w') as f:
        f.write(final_html)

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