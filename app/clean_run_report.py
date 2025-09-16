import os
import sys
from datetime import datetime
import json
import pandas as pd
import main
import traceback
from tqdm import tqdm
import random

def clean_run_report(report_dir, start_dir, test=False):
    """
    Generate a clean run HTML report listing LRGs (subfolders) that do not have a watson.dif file,
    and cache all errors from these LRGs in a parquet file.

    Args:
        report_dir: Directory to save the HTML report
        start_dir: Directory containing LRG subfolders to scan
        test: If True, randomly remove some errors for testing watson.dif generation
    """
    results = []
    all_errors = []

    if not os.path.exists(start_dir):
        raise ValueError(f"Start directory {start_dir} does not exist.")

    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # List all subdirectories in start_dir that contain "snr" (similar to batch_report)
    subdirs = [d for d in os.listdir(start_dir) if os.path.isdir(os.path.join(start_dir, d)) and "snr" in d]

    with tqdm(total=len(subdirs), desc="Processing directories for clean run") as pbar:
        for subdir in subdirs:
            pbar.update(1)
            full_path = os.path.join(start_dir, subdir)
            watson_dif_path = os.path.join(full_path, 'watson.dif')
            diag_path = os.path.join(full_path, 'diag')
            # If LRG has watson.dif, ignore it completely
            if os.path.exists(watson_dif_path) and not test:
                continue

            if os.path.exists(diag_path) and os.path.isdir(diag_path) and os.path.exists(os.path.join(diag_path, 'rdbms')):
                # Parse log to get errors (assuming it's a clean run)
                try:
                    log_contents = main.parseLog(report_dir, full_path, True)
                    #breakpoint()
                    # Extract errors from the history structure
                    #breakpoint()
                    for ruid, shard_groups in log_contents['history'].items():
                        #breakpoint()
                        for shard_group, events in shard_groups.items():
                            for term in range(len(events)):
                                termEvents = events[term]
                                for event in termEvents.get('errors'):
                                    error_entry = event.copy()
                                    error_entry['ruid'] = ruid
                                    error_entry['shard_group'] = shard_group
                                    error_entry['term'] = termEvents.get('term')
                                    error_entry['lrg'] = subdir
                                    all_errors.append(error_entry)

                    results.append({'dir': subdir, 'status': 'Clean run processed', 'error_count': len(all_errors)})
                except Exception as e:
                    error_message = f"{e}\n{traceback.format_exc()}"
                    results.append({'dir': subdir, 'status': 'Failed to parse', 'error_count': 0})
                    print(f"Failed to parse {subdir}: {error_message}")
            else:
                results.append({'dir': subdir, 'status': 'Invalid structure', 'error_count': 0})

    # Save all errors to parquet cache
    if all_errors:
        df = pd.DataFrame(all_errors)

        
    
        # Save cache in parent directory of start_dir
        cache_path = os.path.join(os.path.dirname(report_dir), 'clean_run_errors_cache.parquet')
        df.to_parquet(cache_path, index=False)
        print(f"Saved {len(df)} errors to {cache_path}")
    else:
        print("No errors found to cache.")

    # Load template
    template_path = os.path.join(os.path.dirname(__file__), 'html_assets', 'template', 'clean_run.html')
    with open(template_path, 'r') as f:
        template_html = f.read()

    # Copy CSS
    css_path = os.path.join(os.path.dirname(__file__), 'html_assets', 'template', 'batch_report.css')
    import shutil
    shutil.copy(css_path, os.path.join(report_dir, 'style.css'))

    # Generate table rows
    table_rows = ""
    for result in results:
        dir_name = result['dir']
        status = result['status']
        error_count = result.get('error_count', 0)

        # Determine CSS class based on status
        if status == 'Clean run processed':
            status_class = 'status-success'
        elif status in ['Failed to parse', 'Invalid structure']:
            status_class = 'status-failure'
        else:
            status_class = ''

        table_rows += f"""
        <tr>
            <td>{dir_name}</td>
            <td class="{status_class}">{status}</td>
            <td>{error_count}</td>
        </tr>
        """

    # Replace placeholders
    final_html = template_html.replace('{table_rows}', table_rows)
    final_html = final_html.replace('{source_dir}', start_dir)
    final_html = final_html.replace('{new_reports_count}', str(len(results)))

    # Change title for clean run
    final_html = final_html.replace('Batch Processing Report', 'Clean Run Report')
    final_html = final_html.replace('<title>Batch Report</title>', '<title>Clean Run Report</title>')

    # Adjust table headers for clean run
    final_html = final_html.replace(
        '<th>Log Directory</th>\n                    <th>Status</th>\n                    <th>First Seen</th>\n                    <th>Last time previously Seen</th>\n                    <th>Current report date</th>\n                    <th>Days Existed</th>\n                    <th>Details</th>',
        '<th>LRG Directory</th>\n                    <th>Status</th>\n                    <th>Error Count</th>'
    )

    # Write HTML file
    html_path = os.path.join(report_dir, 'clean_run.html')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(final_html)

    print(f"Clean run report generated at {html_path}")
    print(f"Found {len(results)} LRGs missing watson.dif files.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError("Usage: python clean_run_report.py <report_directory> <start_directory> [--test]")
    report_directory = sys.argv[1]
    start_directory = sys.argv[2]
    test_mode = False
    print(sys.argv)
    if len(sys.argv) > 3 and sys.argv[3] == '--test':
        test_mode = True

    clean_run_report(report_directory, start_directory, test_mode)