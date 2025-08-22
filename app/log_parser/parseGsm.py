import os
import gzip
import shutil
import re

def find_gsm_log_dir(diag_path):
    """Finds the first non-empty subdirectory in the gsm log directory."""
    gsm_path = os.path.join(diag_path, 'gsm')
    if not os.path.exists(gsm_path):
        return None
    
    for dir_name in os.listdir(gsm_path):
        dir_path = os.path.join(gsm_path, dir_name)
        if os.path.isdir(dir_path) and os.listdir(dir_path):
            return dir_path
    return None

def extract_log_file(gsm_sub_dir, report_dir):
    """Extracts the first log file from a gsm subdirectory."""
    log_path = os.path.join(gsm_sub_dir, 'log')
    if not os.path.exists(log_path):
        return None

    for file_name in sorted(os.listdir(log_path)):
        if file_name.endswith('.log') or file_name.endswith('.log.gz'):
            log_file_path = os.path.join(log_path, file_name)
            dest_path = os.path.join(report_dir, os.path.basename(log_file_path))

            if file_name.endswith('.gz'):
                dest_path = dest_path[:-3] # Remove .gz
                if not os.path.exists(dest_path):
                    with gzip.open(log_file_path, 'rb') as f_in:
                        with open(dest_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                return dest_path
            else:
                if not os.path.exists(dest_path):
                    shutil.copy(log_file_path, dest_path)
                return dest_path
    return None

def parse_gsm_log(log_file_path):
    """Parses a GSM log file for errors."""
    errors = []
    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    for i, line in enumerate(lines):
        if "Request Done" in line and "Error" in line:
            id_match = re.search(r'Id=(\d+)', line)
            if not id_match:
                id_match = re.search(r'Id="(\d+)"', line)
            if not id_match:
                continue
            
            request_id = id_match.group(1)
            
            for j in range(i, -1, -1):
                if f'Id="{request_id}"' in lines[j] and "Catalog request" in lines[j]:
                    error_block_lines = lines[j:i+1]
                    error_block_text = "".join(error_block_lines)
                    
                    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z?)', error_block_text)
                    timestamp = timestamp_match.group(1) if timestamp_match else ''

                    request_type_match = re.search(r'Catalog request:"([^"]+)"', error_block_text)
                    request_type = request_type_match.group(1) if request_type_match else ''

                    payload_match = re.search(r'Payload:"([^"]+)"', error_block_text)
                    payload = payload_match.group(1) if payload_match else ''
                    
                    target_match = re.search(r'Target:"([^"]+)"', error_block_text)
                    target = target_match.group(1) if target_match else ''

                    message_match = re.search(r'message:"([^"]+)"', error_block_text, re.DOTALL)
                    message = message_match.group(1).replace('\n', ' ') if message_match else ''

                    errors.append({
                        'timestamp': timestamp,
                        'request_type': request_type,
                        'payload': payload,
                        'target': target,
                        'message': message,
                        'full_text': error_block_text
                    })
                    break
    return errors

def parse_gsm_logs(report_dir, full_path):
    """Main function to parse all GSM logs."""
    diag_path = os.path.join(full_path, 'diag')
    gsm_log_dir = find_gsm_log_dir(diag_path)
    if not gsm_log_dir:
        return []

    all_errors = []
    for gsm_dir_name in sorted(os.listdir(gsm_log_dir)):
        if gsm_dir_name.startswith('gsm'):
            gsm_sub_dir = os.path.join(gsm_log_dir, gsm_dir_name)
            log_file = extract_log_file(gsm_sub_dir, report_dir)
            if log_file:
                all_errors.extend(parse_gsm_log(log_file))
    
    return all_errors