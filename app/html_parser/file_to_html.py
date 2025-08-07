import os
import html

def convert_file_to_html(source_path, output_dir):
    """
    Converts a text file to an HTML file with each line in a <p> tag with an ID.
    """
    if not os.path.exists(source_path):
        return None

    base_name = os.path.basename(source_path)
    html_file_name = f"{base_name}.html"
    html_path = os.path.join(output_dir, html_file_name)

    try:
        with open(source_path, 'r', encoding='utf-8', errors='ignore') as f_in:
            lines = f_in.readlines()

        with open(html_path, 'w', encoding='utf-8') as f_out:
            f_out.write('<!DOCTYPE html>\n<html lang="en">\n<head>\n')
            f_out.write(f'<title>{html.escape(base_name)}</title>\n')
            f_out.write('<meta charset="UTF-8">\n')
            f_out.write('<style>p { margin: 0; padding: 0; }</style>\n')
            f_out.write('</head>\n<body>\n')
            for i, line in enumerate(lines):
                f_out.write(f'<p id="line{i+1}">{html.escape(line)}</p>\n')
            f_out.write('</body>\n</html>')
        
        return html_path
    except Exception as e:
        print(f"Error converting {source_path} to HTML: {e}")
        return None