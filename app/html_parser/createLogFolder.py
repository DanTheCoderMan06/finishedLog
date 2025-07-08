import bs4
import os
import datetime
import uuid
import pdb

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

    with open(main_html_path, 'r') as f:
        shardListHTML = f.read()

    with open(style_css_path, 'r') as f:
        globalCSS = f.read()

    soup = bs4.BeautifulSoup(shardListHTML, 'html.parser')

    logTitle = soup.find("h1", class_="main-title")
    logTitle.string = "{} log contents".format(os.path.basename(directoryName))

    tableBody = soup.find('tbody')
    tableBody.clear()

    for ruid in results['allRUIDS']:
        newRow = soup.new_tag('tr')
        cell1 = soup.new_tag('td')
        link = soup.new_tag('a', attrs={'href': './RULog{}.html'.format(ruid)})
        link.string = "Replication Unit {}".format(ruid)
        cell1.append(link)
        newRow.append(cell1)
        tableBody.append(newRow)

    if 'incidents' in results and results['incidents']:
        container = soup.find('div', class_='container')

        incident_title = soup.new_tag('h1', attrs={'class': 'main-title'})
        incident_title.string = "Incidents"
        container.append(incident_title)

        incident_container = soup.new_tag('div', attrs={'class': 'table-container'})
        
        incident_table = soup.new_tag('table')
        incident_thead = soup.new_tag('thead')
        incident_tr = soup.new_tag('tr')
        headers = ["Incident File", "RUID", "DB ID", "DB Log Folder Name"]
        for header_text in headers:
            incident_th = soup.new_tag('th')
            incident_th.string = header_text
            incident_tr.append(incident_th)
        incident_thead.append(incident_tr)
        incident_table.append(incident_thead)
        
        incident_tbody = soup.new_tag('tbody')
        for i in range(len(results['incidents'])):
            item = results['incidents'][i]
            name = item['folderName']
            path = item['fileName']
            new_row = soup.new_tag('tr')
            cell = soup.new_tag('td')
            link = soup.new_tag('a', attrs={'href': path})
            link.string = name
            cell.append(link)
            new_row.append(cell)

            ruid_cell = soup.new_tag('td')
            ruid_cell.string = str(item.get('ruid', 'N/A'))
            new_row.append(ruid_cell)

            db_id_cell = soup.new_tag('td')
            db_id_cell.string = str(item.get('dbId', 'N/A'))
            new_row.append(db_id_cell)

            db_log_folder_name_cell = soup.new_tag('td')
            db_log_folder_name_cell.string = item.get('dbLogFolderName', 'N/A')
            new_row.append(db_log_folder_name_cell)
            
            incident_tbody.append(new_row)
        
        incident_table.append(incident_tbody)
        incident_container.append(incident_table)
        
        container.append(incident_container)

    resultingHTML = soup.prettify()

    with open(os.path.join(logDirectory,"index.html"), 'w', encoding='utf-8') as f:
        f.write(resultingHTML)

    with open(os.path.join(logDirectory,"style.css"),'w') as f:
        f.write(globalCSS)

    for ruid in results['allRUIDS']:
        with open(ruLogPath,'r') as fp:
            ruidSoup = bs4.BeautifulSoup(fp.read(), 'html.parser')

        mainRUIDTitle = ruidSoup.find("h1", class_ = "main-title")
        mainRUIDTitle.string = "Shard Groups for RUID: {}".format(ruid)
        shardGroupList = ruidSoup.find('tbody')
        shardGroupList.clear()
        for shardGroup in results['shardGroups']:
            newRow = soup.new_tag('tr')
            cell1 = soup.new_tag('td')
            link = soup.new_tag('a', attrs={'href': './ShardGroupLog_{}_RUID_{}.html'.format(shardGroup, ruid)})
            link.string = "Shard Group {}".format(shardGroup)
            cell1.append(link)
            newRow.append(cell1)
            shardGroupList.append(newRow)

            with open(shardLogPath, 'r') as fp:
                shardGroupSoup = bs4.BeautifulSoup(fp.read(), 'html.parser')

            shardGroupTitle = shardGroupSoup.find("h1", class_="main-title")
            shardGroupTitle.string = "Leadership History for Shard Group {} for RU_ID {}".format(shardGroup, ruid)
            shardGroupHistory = list(results['history'][ruid][shardGroup])

            logResultList = shardGroupSoup.find('tbody')
            logResultList.clear()
            for logResult in shardGroupHistory:
                newRow = soup.new_tag('tr')

                history_filename = "history_{}.html".format(uuid.uuid4())
                cell1 = soup.new_tag('td')
                link = soup.new_tag('a', attrs={'href': './{}'.format(history_filename)})
                link.string = logResult['timestamp']
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
                cell5.string = "{}".format(logResult['recoveryTime'])
                newRow.append(cell5)

                logResultList.append(newRow)

                with open(history_log_path, 'r') as f:
                    history_html = f.read()

                historySoup = bs4.BeautifulSoup(history_html, 'html.parser')
                history_title = historySoup.find("h1", class_="main-title")
                history_title.string = "History for Term {}".format(logResult['term'])
                history_table_body = historySoup.find('tbody')
                history_table_body.clear()

                logResult['history'].sort(key=lambda result: datetime.datetime.fromisoformat(result['timestamp'].strip()).timestamp(), reverse=False)

                for history_item in logResult['history']:
                    history_item_row = soup.new_tag('tr', attrs={'class': 'hoverable-row'})

                    ts_cell = soup.new_tag('td')
                    if 'ospFile' in history_item and history_item['ospFile']:
                        error_file = soup.new_tag('a', attrs={'href': history_item['ospFile']})
                        error_file.string = history_item['timestamp']
                        ts_cell.append(error_file)
                    else:
                        ts_cell.append(history_item['timestamp'])

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

            with open(os.path.join(logDirectory, './ShardGroupLog_{}_RUID_{}.html'.format(shardGroup, ruid)), 'w') as fp:
                fp.write(shardGroupSoup.prettify())

        with open(os.path.join(logDirectory, './RULog{}.html'.format(ruid)), 'w') as fp:
            fp.write(ruidSoup.prettify())