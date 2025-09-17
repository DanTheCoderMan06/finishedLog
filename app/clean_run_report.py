import os
import sys
from datetime import datetime
import json
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
    current_lrg_errors = {}  # Dictionary to store errors for current LRGs
    all_errors = []

    # Load cache if it exists
    cache_path = os.path.join(os.path.dirname(report_dir), 'clean_run_errors_cache.json')
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                cached_errors = json.load(f)
            print(f"Loaded {len(cached_errors)} errors from {cache_path}")
        except Exception as e:
            print(f"Failed to load cache from {cache_path}: {e}")
            cached_errors = []  # Ensure cached_errors is an empty list in case of failure
    else:
        cached_errors = []

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
                    # Extract errors from the history structure
                    lrg_errors = []
                    for ruid, shard_groups in log_contents['history'].items():
                        for shard_group, events in shard_groups.items():
                            for term in range(len(events)):
                                termEvents = events[term]
                                for event in termEvents.get('errors'):
                                    error_entry = event.copy()
                                    error_entry['ruid'] = ruid
                                    error_entry['shard_group'] = shard_group
                                    error_entry['term'] = termEvents.get('term')
                                    error_entry['lrg'] = subdir
                                    lrg_errors.append(error_entry)
                    current_lrg_errors[subdir] = lrg_errors

                    error_count = len(lrg_errors)
                    results.append({'dir': subdir, 'status': 'Clean run processed', 'error_count': error_count})
                except Exception as e:
                    error_message = f"{e}\n{traceback.format_exc()}"
                    results.append({'dir': subdir, 'status': 'Failed to parse', 'error_count': 0})
                    print(f"Failed to parse {subdir}: {error_message}")
            else:
                results.append({'dir': subdir, 'status': 'Invalid structure', 'error_count': 0})

    # Merge cache and new errors
    # Remove errors from cache if LRG is in current_lrg_errors
    filtered_cache = [error for error in cached_errors if error['lrg'] not in current_lrg_errors]

    # Combine cache and new errors
    for lrg in current_lrg_errors:
        all_errors.extend(current_lrg_errors[lrg])
    all_errors = filtered_cache + all_errors

    # Save cache
    cache_path = os.path.join(os.path.dirname(report_dir), 'clean_run_errors_cache.json')

    # Apply test mode: randomly remove some errors BEFORE saving to cache
    if test and all_errors:
        remove_percentage = random.uniform(0.3, 0.7)
        num_to_remove = int(len(all_errors) * remove_percentage)
        if num_to_remove > 0:
            indices_to_remove = random.sample(range(len(all_errors)), num_to_remove)
            # Create a new list excluding the removed indices
            all_errors = [error for i, error in enumerate(all_errors) if i not in indices_to_remove]
            print(f"Test mode: Removed {num_to_remove} errors ({remove_percentage:.1%}) for watson.dif testing")

    if all_errors:
        with open(cache_path, 'w') as f:
            json.dump(all_errors, f, indent=4)
        print(f"Saved {len(all_errors)} errors to {cache_path}")
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