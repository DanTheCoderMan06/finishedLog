import os
import sys
import main
import traceback
from tqdm import tqdm

def batch_parse(report_dir, start_dir, max_files=None):
    results_file = os.path.join(report_dir, "results.txt")
    with open(results_file, 'w') as f:
        f.write("Batch Processing Results\n")
        f.write("="*30 + "\n")

    processed_files = 0
    dir_list = os.listdir(start_dir)
    try:
        with tqdm(total=len(dir_list), desc="Processing directories") as pbar:
            for dir_name in dir_list:
                if max_files is not None and processed_files >= max_files:
                    print(f"Reached file limit of {max_files}. Exiting.")
                    break
                full_path = os.path.join(start_dir, dir_name)
                if os.path.isdir(full_path):
                    diag_path = os.path.join(full_path, 'diag')
                    if os.path.exists(diag_path) and os.path.isdir(diag_path):
                        try:
                            main.parseLog(report_dir, full_path)
                            processed_files += 1
                            with open(results_file, 'a') as f:
                                f.write(f"Processing: {full_path}\n")
                                f.write(f"  -> Success\n")
                        except Exception as e:
                            if not "No gdsctl log file" in str(e):
                                with open(results_file, 'a') as f:
                                    f.write(f"Processing: {full_path}\n")
                                    f.write(f"  -> Failed: {e}\n")
                                    f.write(traceback.format_exc())
                                    f.write("\n")
                pbar.update(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Stopping batch processing.")

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