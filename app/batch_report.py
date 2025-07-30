import os
import sys
import main
import traceback

def batch_parse(start_dir, report_dir):
    print("Processing Batch")
    results_file = os.path.join(report_dir, "results.txt")
    with open(results_file, 'w') as f:
        f.write("Batch Processing Results\n")
        f.write("="*30 + "\n")

    for dir_name in os.listdir(start_dir):
        full_path = os.path.join(start_dir, dir_name)
        if os.path.isdir(full_path):
            diag_path = os.path.join(full_path, 'diag')
            if os.path.exists(diag_path) and os.path.isdir(diag_path):
                with open(results_file, 'a') as f:
                    f.write(f"Processing: {full_path}\n")
                try:
                    main.parseLog(report_dir, full_path)
                    with open(results_file, 'a') as f:
                        f.write(f"  -> Success\n")
                        print("Created File")
                except Exception as e:
                    with open(results_file, 'a') as f:
                        f.write(f"  -> Failed: {e}\n")
                        f.write(traceback.format_exc())
                        f.write("\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError("Usage: python batch_report.py <start_directory> <report_directory>")
    else:
        start_directory = sys.argv[2]
        report_directory = sys.argv[1]
        batch_parse(start_directory, report_directory)