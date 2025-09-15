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
<<<<<<< HEAD
            if os.path.exists(watson_dif_path) and not test:
=======
            if os.path.exists(watson_dif_path):
>>>>>>> 7d256a2621250710c5482ddefa3d241c64504aec
                continue

            if os.path.exists(diag_path) and os.path.isdir(diag_path) and os.path.exists(os.path.join(diag_path, 'rdbms')):
                # Parse log to get errors (assuming it's a clean run)
                try:
                    log_contents = main.parseLog(report_dir, full_path)
<<<<<<< HEAD
=======
                    trace_errors = log_contents.get('trace_errors', [])
                    watson_errors = log_contents.get('watson_errors', [])
                    gsm_errors = log_contents.get('gsm_errors', [])
>>>>>>> 7d256a2621250710c5482ddefa3d241c64504aec

                    # Extract errors from the history structure
                    if 'history' in log_contents and log_contents['history']:
                        for ruid, shard_groups in log_contents['history'].items():
                            for shard_group, events in shard_groups.items():
<<<<<<< HEAD
                                for term in range(len(events)):
                                    termEvents = events[term]
                                    for event in termEvents.get('errors'):
                                        error_entry = event.copy()
                                        error_entry['ruid'] = ruid
                                        error_entry['shard_group'] = shard_group
                                        error_entry['term'] = term
                                        error_entry['lrg'] = subdir
                                        all_errors.append(error_entry)

                    results.append({'dir': subdir, 'status': 'Clean run processed', 'error_count': len(all_errors)})
=======
                                for event in events:
                                    if 'errors' in event and event['errors']:
                                        for error in event['errors']:
                                            error['ruid'] = ruid
                                            error['shard_group'] = shard_group
                                            error['term'] = event['term']
                                            trace_errors.append(error)

                    # Add LRG name to each error
                    for error in trace_errors:
                        error['lrg'] = subdir
                    for error in watson_errors:
                        error['lrg'] = subdir
                    for error in gsm_errors:
                        error['lrg'] = subdir

                    all_errors.extend(trace_errors + watson_errors + gsm_errors)
                    results.append({'dir': subdir, 'status': 'Clean run processed', 'error_count': len(trace_errors) + len(watson_errors) + len(gsm_errors)})
>>>>>>> 7d256a2621250710c5482ddefa3d241c64504aec
                except Exception as e:
                    error_message = f"{e}\n{traceback.format_exc()}"
                    results.append({'dir': subdir, 'status': 'Failed to parse', 'error_count': 0})
                    print(f"Failed to parse {subdir}: {error_message}")
            else:
                results.append({'dir': subdir, 'status': 'Invalid structure', 'error_count': 0})

    # Save all errors to parquet cache
    if all_errors:
        df = pd.DataFrame(all_errors)

        # Apply test mode: randomly remove some errors
        if test and len(df) > 0:
            # Remove 30-70% of errors randomly for testing
<<<<<<< HEAD
            remove_percentage = 0 #random.uniform(0.3, 0.7)
=======
            remove_percentage = random.uniform(0.3, 0.7)
>>>>>>> 7d256a2621250710c5482ddefa3d241c64504aec
            num_to_remove = int(len(df) * remove_percentage)
            if num_to_remove > 0:
                indices_to_remove = random.sample(range(len(df)), num_to_remove)
                df = df.drop(indices_to_remove).reset_index(drop=True)
                print(f"Test mode: Removed {num_to_remove} errors ({remove_percentage:.1%}) for watson.dif testing")

        # Save cache in parent directory of start_dir
        cache_path = os.path.join(os.path.dirname(report_dir), 'clean_run_errors_cache.parquet')
        df.to_parquet(cache_path, index=False)
        print(f"Saved {len(df)} errors to {cache_path}")
    else:
        print("No errors found to cache.")

    # Load template
<<<<<<< HEAD
    template_path = os.path.join(os.path.dirname(__file__), 'html_assets', 'template', 'clean_run.html')
=======
    template_path = os.path.join(os.path.dirname(__file__), 'html_assets', 'template', 'batch_report.html')
>>>>>>> 7d256a2621250710c5482ddefa3d241c64504aec
    with open(template_path, 'r') as f:
        template_html = f.read()

    # Copy CSS
    css_path = os.path.join(os.path.dirname(__file__), 'html_assets', 'template', 'batch_report.css')
    import shutil
    shutil.copy(css_path, os.path.join(report_dir, 'style.css'))

<<<<<<< HEAD

=======
>>>>>>> 7d256a2621250710c5482ddefa3d241c64504aec
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
<<<<<<< HEAD
    with open(html_path, 'w', encoding='utf-8') as f:
=======
    with open(html_path, 'w') as f:
>>>>>>> 7d256a2621250710c5482ddefa3d241c64504aec
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