import bs4
import os
import datetime
import uuid
import gzip
import shutil

def copy_file_to_report_dir(file_path, report_dir):
    if not file_path or 'file:///' in file_path:
        return file_path
    
    is_gzipped = file_path.endswith('.gz')

    if not os.path.exists(file_path):
        if os.path.exists(file_path + ".gz"):
            file_path += ".gz"
            is_gzipped = True
        else:
            return ''

    if is_gzipped:
        unzipped_base_name = os.path.basename(file_path[:-3])
        dest_path = os.path.join(report_dir, unzipped_base_name)
        
        if not os.path.exists(dest_path):
            with gzip.open(file_path, 'rb') as f_in:
                with open(dest_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        return './' + unzipped_base_name
    else:
        base_name = os.path.basename(file_path)
        dest_path = os.path.join(report_dir, base_name)
        
        if not os.path.exists(dest_path):
            shutil.copy(file_path, dest_path)
            
        return './' + base_name

# Generates an HTML log folder from a dictionary of results.
# Args:
#     results (dict): A dictionary containing the parsed log data.
def createLogFolder(results, results_dir):
    directoryName = results['logDirectory']
    logDirectory = results_dir
    os.makedirs(logDirectory, exist_ok=True)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    main_html_path = os.path.join(script_dir, '..', 'html_assets', 'template', 'main.html')
    ruLogPath = os.path.join(script_dir, '..', 'html_assets', 'template', 'emptyRULog.html')
    shardLogPath = os.path.join(script_dir, '..', 'html_assets', 'template', 'emptyShardLog.html')
    style_css_path = os.path.join(script_dir, '..', 'html_assets', 'template', 'style.css')
    history_log_path = os.path.join(script_dir, '..', 'html_assets', 'template', 'emptyShardLogHistory.html')

    with open(main_html_path, 'r', encoding='utf-8', errors='ignore') as f:
        shardListHTML = f.read()

    with open(style_css_path, 'r', encoding='utf-8', errors='ignore') as f:
        globalCSS = f.read()

    soup = bs4.BeautifulSoup(shardListHTML, 'html.parser')

    logTitle = soup.find("h1", class_="main-title")
    logTitle.string = "{} replication unit".format(os.path.basename(directoryName))

    tableBody = soup.find('tbody')
    tableBody.clear()

    ruid_error_flags = {ruid: False for ruid in results['allRUIDS']}
    for ruid in results['allRUIDS']:
        for shard_group in results['history'][ruid]:
            for event in results['history'][ruid][shard_group]:
                if event.get('errors'):
                    ruid_error_flags[ruid] = True
                    break
            if ruid_error_flags[ruid]:
                break

    for ruid in results['allRUIDS']:
        newRow = soup.new_tag('tr')
        if ruid_error_flags[ruid]:
            newRow['class'] = 'error-highlight'
        cell1 = soup.new_tag('td')
        link = soup.new_tag('a', attrs={'href': './RULog{}.html'.format(ruid)})
        link.string = "Replication Unit {}".format(ruid)
        cell1.append(link)
        newRow.append(cell1)

        error_cell = soup.new_tag('td')
        errors = set()
        for shard_group in results['history'][ruid]:
            for event in results['history'][ruid][shard_group]:
                if event.get('errors'):
                    for error in event.get('errors'):
                        errors.add(error.get('code'))
        error_cell.string = str(errors) if errors else "No Errors"
        newRow.append(error_cell)
        tableBody.append(newRow)

    if 'trace_errors' in results and results['trace_errors']:
        container = soup.find('div', class_='container')

        trace_title = soup.new_tag('h1', attrs={'class': 'main-title'})
        trace_title.string = "Trace Errors"
        container.append(trace_title)

        trace_container = soup.new_tag('div', attrs={'class': 'table-container'})
        
        trace_table = soup.new_tag('table')
        trace_thead = soup.new_tag('thead')
        trace_tr = soup.new_tag('tr')
        headers = ["Trace File", "Continued Log"]
        for header_text in headers:
            trace_th = soup.new_tag('th')
            trace_th.string = header_text
            trace_tr.append(trace_th)
        trace_thead.append(trace_tr)
        trace_table.append(trace_thead)
        
        trace_tbody = soup.new_tag('tbody')
        for item in results['trace_errors']:
            new_row = soup.new_tag('tr')
            
            # Trace File column
            file_cell = soup.new_tag('td')
            file_path = item.get('file')
            if file_path:
                link_path = copy_file_to_report_dir(file_path, logDirectory)
                file_link = soup.new_tag('a', attrs={'href': link_path, 'oncontextmenu': "navigator.clipboard.writeText(this.href); event.preventDefault(); alert('Path copied to clipboard!');"})
                file_link.string = os.path.basename(file_path)
                file_cell.append(file_link)
            else:
                file_cell.string = "N/A"
            new_row.append(file_cell)

            # Continued Log column
            log_cell = soup.new_tag('td')
            log_file_path = item.get('log_file')
            if log_file_path:
                link_path = copy_file_to_report_dir(log_file_path, logDirectory)
                log_link = soup.new_tag('a', attrs={'href': link_path, 'oncontextmenu': "navigator.clipboard.writeText(this.href); event.preventDefault(); alert('Path copied to clipboard!');"})
                log_link.string = os.path.basename(log_file_path)
                log_cell.append(log_link)
            else:
                log_cell.string = "N/A"
            new_row.append(log_cell)
            
            trace_tbody.append(new_row)
        
        trace_table.append(trace_tbody)
        trace_container.append(trace_table)
        
        container.append(trace_container)

    if 'watson_errors' in results and results['watson_errors']:
        container = soup.find('div', class_='container')

        watson_title = soup.new_tag('h1', attrs={'class': 'main-title'})
        watson_title.string = "Watson Errors"
        container.append(watson_title)

        watson_container = soup.new_tag('div', attrs={'class': 'table-container'})
        
        watson_table = soup.new_tag('table')
        watson_thead = soup.new_tag('thead')
        watson_tr = soup.new_tag('tr')
        headers = ["Dif File", "Log File"]
        for header_text in headers:
            watson_th = soup.new_tag('th')
            watson_th.string = header_text
            watson_tr.append(watson_th)
        watson_thead.append(watson_tr)
        watson_table.append(watson_thead)
        
        watson_tbody = soup.new_tag('tbody')
        for item in results['watson_errors']:

            difFile = item['dif_file']
            logFile = item['log_file']
            
            new_row = soup.new_tag('tr')
            
            dif_cell = soup.new_tag('td')
            dif_link = soup.new_tag('a', attrs={'href': copy_file_to_report_dir(difFile, logDirectory), 'oncontextmenu': "navigator.clipboard.writeText(this.href); event.preventDefault(); alert('Path copied to clipboard!');"})
            dif_link.string = os.path.basename(item['dif_file'])
            dif_cell.append(dif_link)
            new_row.append(dif_cell)

            log_cell = soup.new_tag('td')
            if item.get('log_file') and os.path.exists(item['log_file']):
                log_link = soup.new_tag('a', attrs={'href': copy_file_to_report_dir(logFile, logDirectory), 'oncontextmenu': "navigator.clipboard.writeText(this.href); event.preventDefault(); alert('Path copied to clipboard!');"})
                log_link.string = os.path.basename(item['log_file'])
                log_cell.append(log_link)
            else:
                log_cell.string = "N/A"
            new_row.append(log_cell)
            
            watson_tbody.append(new_row)
        
        watson_table.append(watson_tbody)
        watson_container.append(watson_table)
        
        container.append(watson_container)

    resultingHTML = soup.prettify()

    with open(os.path.join(logDirectory,"index.html"), 'w', encoding='utf-8') as f:
        f.write(resultingHTML)

    with open(os.path.join(logDirectory,"style.css"),'w', encoding='utf-8') as f:
        f.write(globalCSS)

    for ruid in results['allRUIDS']:
        with open(ruLogPath,'r', encoding='utf-8', errors='ignore') as fp:
            ruidSoup = bs4.BeautifulSoup(fp.read(), 'html.parser')

        mainRUIDTitle = ruidSoup.find("h1", class_ = "main-title")
        mainRUIDTitle.string = "Shard Groups for RUID: {}".format(ruid)
        shardGroupList = ruidSoup.find('tbody')
        shardGroupList.clear()
        for shardGroup in results['shardGroups']:
            shard_group_error = False
            for event in results['history'][ruid][shardGroup]:
                if event.get('errors'):
                    shard_group_error = True
                    break
            
            newRow = soup.new_tag('tr')
            if shard_group_error:
                newRow['class'] = 'error-highlight'

            cell1 = soup.new_tag('td')
            link = soup.new_tag('a', attrs={'href': './ShardGroupLog_{}_RUID_{}.html'.format(shardGroup, ruid)})
            link.string = "Shard Group {}".format(shardGroup)
            cell1.append(link)
            newRow.append(cell1)

            error_cell = soup.new_tag('td')
            errors = set()
            for event in results['history'][ruid][shardGroup]:
                if event.get('errors'):
                    for error in event.get('errors'):
                        errors.add(error.get('code'))
            error_cell.string = str(errors) if errors else "No Errors"
            newRow.append(error_cell)
            shardGroupList.append(newRow)

            with open(shardLogPath, 'r', encoding='utf-8', errors='ignore') as fp:
                shardGroupSoup = bs4.BeautifulSoup(fp.read(), 'html.parser')

            shardGroupTitle = shardGroupSoup.find("h1", class_="main-title")
            shardGroupTitle.string = "Leadership History for Shard Group {} for RU_ID {}".format(shardGroup, ruid)
            shardGroupHistory = list(results['history'][ruid][shardGroup])

            logResultList = shardGroupSoup.find('tbody')
            logResultList.clear()
            for logResult in shardGroupHistory:
                log_result_error = False
                if logResult.get('errors'):
                    log_result_error = True
                newRow = soup.new_tag('tr')
                if log_result_error:
                    newRow['class'] = 'error-highlight'

                history_filename = "history_{}.html".format(uuid.uuid4())
                cell1 = soup.new_tag('td')
                link = soup.new_tag('a', attrs={'href': './{}'.format(history_filename)})
                link.string = logResult['timestamp'].split('+')[0]
                cell1.append(link)
                newRow.append(cell1)

                cell2 = soup.new_tag('td')
                cell2.string = logResult['dbName']
                newRow.append(cell2)

                cell3 = soup.new_tag('td')
                cell3.string = str(logResult['dbId'])
                newRow.append(cell3)

                cell4 = soup.new_tag('td')
                cell4.string = "{}".format(logResult['term'])
                newRow.append(cell4)

                cell5 = soup.new_tag('td')
                cell5.string = "{:.2f}".format(logResult.get('recoveryTime', 0))
                newRow.append(cell5)

                logResultList.append(newRow)

                with open(history_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                    history_html = f.read()

                historySoup = bs4.BeautifulSoup(history_html, 'html.parser')
                history_title = historySoup.find("h1", class_="main-title")
                history_title.string = "History for Term {}".format(logResult['term'])
                history_table_body = historySoup.find('tbody')
                history_table_body.clear()

                all_events = logResult.get('history', []) + logResult.get('errors', [])
                all_events.sort(key=lambda result: datetime.datetime.fromisoformat(result['timestamp'].strip()).timestamp(), reverse=False)

                for history_item in all_events:
                    history_item_row = soup.new_tag('tr', attrs={'class': 'hoverable-row'})
                    if history_item.get('type') == 'error' and history_item.get('code') != 3113:
                        history_item_row['class'] = history_item_row.get('class', []) + ['error-highlight']

                    ts_cell = soup.new_tag('td')
                    if 'ospFile' in history_item and history_item['ospFile']:
                        osp_path = history_item['ospFile']
                        link_path = copy_file_to_report_dir(osp_path, logDirectory)
                        if 'scrollIndex' in history_item:
                            link_path += f"#line{history_item['scrollIndex']}"
                        error_file = soup.new_tag('a', attrs={'href': link_path, 'oncontextmenu': "navigator.clipboard.writeText(this.href); event.preventDefault(); alert('Path copied to clipboard!');"})
                        error_file.string = history_item['timestamp'].split('+')[0]
                        ts_cell.append(error_file)
                    else:
                        ts_cell.append(history_item['timestamp'].split('+')[0])

                    info_div = soup.new_tag('div', attrs={'class': 'row-info'})
                    parameter_info = "".join(history_item['parameters'])
                    print(history_item)
                    info_div.string = history_item['original'] + parameter_info
                    ts_cell.append(info_div)
                    history_item_row.append(ts_cell)

                    db_name_cell = soup.new_tag('td')
                    db_name_cell.string = history_item['dbName']
                    history_item_row.append(db_name_cell)

                    db_id_cell = soup.new_tag('td')
                    db_id_cell.string = str(history_item['dbId'])
                    history_item_row.append(db_id_cell)

                    event_cell = soup.new_tag('td')
                    targetReason = "N/A"
                    if 'reason' in history_item:
                        targetReason = history_item['reason']
                    if history_item['type'] == 'error':
                        event_cell.string = "Error: ({})".format(history_item['code'])
                    else:
                        event_cell.string = "{} / {}".format(history_item['type'], targetReason)
                    history_item_row.append(event_cell)

                    history_table_body.append(history_item_row)

                with open(os.path.join(logDirectory, history_filename), 'w', encoding='utf-8') as f:
                    f.write(historySoup.prettify())

            with open(os.path.join(logDirectory, './ShardGroupLog_{}_RUID_{}.html'.format(shardGroup, ruid)), 'w', encoding='utf-8') as fp:
                fp.write(shardGroupSoup.prettify())

        with open(os.path.join(logDirectory, './RULog{}.html'.format(ruid)), 'w', encoding='utf-8') as fp:
            fp.write(ruidSoup.prettify())
