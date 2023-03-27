import json
import os
import sys
import platform
import sqlite3
import threading
import requests  # pip install requests
import time
import schedule

username = os.getlogin()
if platform.system() == 'Linux':
    logs_folder = '/home/ub/pavlovserver/Pavlov/Saved/Logs/'
    server_name = 'local'
    url = "http://stats.piketz.ru/data_in"
    if username == 'pavlov':
        server_name = 'B.C'
        logs_folder = '/home/steam/pavlovserver/Pavlov/Saved/Logs/'
    if username == 'ub': url = "http://localhost:5000/data_in"
else:
    logs_folder = 'Logs'
    server_name = 'tst'
    url = "http://localhost:5000/data_in"

def_pavlov_logfile = logs_folder + '/Pavlov.log'


time_refresh_parsing = 300
database = 'temp.db'
conn = sqlite3.connect(database)





def create_db():
    c = conn.cursor()
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='match' ''')
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE match(
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      Timestamp TEXT UNIQUE NOT NULL,
                      server TEXT,
                      MapLabel TEXT,
                      GameMode INTEGER,
                      PlayerCount INTEGER,
                      bTeams INTEGER,
                      Team0Score INTEGER,
                      Team1Score INTEGER)''')

    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='match_users' ''')
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE match_users (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      Timestamp TEXT NOT NULL, 
                      uniqueId_player TEXT,
                      playerName TEXT,
                      teamId REAL,
                      Death INTEGER,
                      Assist INTEGER,
                      Kill INTEGER,
                      Headshot INTEGER,
                      BombPlanted INTEGER,
                      Experience INTEGER)''')
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='KillData' ''')
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE KillData (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event TEXT NOT NULL,
                        Timestamp TEXT NOT NULL,
                        server TEXT,
                        Killer TEXT,
                        KillerTeamID INTEGER,
                        Killed TEXT,
                        KilledTeamID INTEGER,
                        KilledBy TEXT,
                        Headshot BOOLEAN)''')
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='event' ''')
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE event (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event TEXT NOT NULL,
                        Timestamp TEXT NOT NULL,
                        server TEXT,
                        State TEXT,
                        Round INTEGER,
                        WinningTeam INTEGER)''')
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='BombData' ''')
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE BombData (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Timestamp TEXT NOT NULL,
                        server TEXT,
                        Player TEXT,
                        BombInteraction TEXT)''')
    conn.close()

def parse_log_files(filepath):
   # filepath = 'Pavlov.log'
    found_json = False
    text_json = ""
    data = None
    with open(filepath, "r") as file:
        lines = file.readlines()
        for line in lines:
            #нужен поиск строчки "PavlovLog: StartPlay was called" с таймштамт для сохранения начала матча
            if "StartPlay was called" in line:
               # print(f'FIND  - PavlovLog: StartPlay was called')
                Timestamp = line.split(":")[0].replace("[", "").replace("]", "")
                data = {"RoundState": {"State": "Start", "Timestamp": Timestamp}}
                #data["Timestamp"] = Timestamp
                save_event_data_to_db(data)
            if not found_json:
                if "StatManagerLog" in line:
                    if "{" in line:
                        found_json = True
                        Timestamp = line.split(":")[0].replace("[", "").replace("]", "")
                        text_json = "{"
            elif found_json:
                if line.rstrip() == "}" and line.endswith("}\n"):
                    data = json.loads(text_json + '}')
                   # print(f'data = {json.dumps(data)}')
                    if "allStats" in data:
                        data["Timestamp"] = Timestamp
                        save_match_data_to_db(data)
                        save_users_data_to_db(data)
                       #print(f'Find allStats = {json.dumps(data)}')
                    elif "RoundState" in data:
                        pass
                        save_event_data_to_db(data)
                        #print(f'Find RoundState = {json.dumps(data)}')
                    elif "KillData" in data:
                        data["Timestamp"] = Timestamp
                        save_Kill_data_to_db(data)
                        #print(f'Find KillData = {json.dumps(data)}')
                    elif "RoundEnd" in data:
                       # date_obj = datetime.datetime.strptime(Timestamp, "%Y.%m.%d-%H.%M.%S")
                       # date_obj -= datetime.timedelta(seconds=1)
                       # data["Timestamp"] = date_obj.strftime("%Y.%m.%d-%H.%M.%S")
                        data["Timestamp"] = Timestamp
                        save_event_data_to_db(data)
                        #print(f'Find RoundEnd = {json.dumps(data)}')
                    elif "BombData" in data:
                        data["Timestamp"] = Timestamp
                        #{"BombData": {"Player": "76561198124316469", "BombInteraction": "BombDefused"}}
                        save_Bomb_data_to_db(data)
                    else:
                        send_json(data, 'unknown')
                        #print(f'Unknown data = {json.dumps(data)}')
                    found_json = False
                else:
                    text_json = text_json + line
    return data


def save_match_data_to_db(data):
   # print(f'save_match_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM match WHERE Timestamp='{data['Timestamp']}'")
        result = c.fetchone()

        if result is None:
            if send_json(data, 'allStats'):
                print('save to bd match')
                c.execute(
                    "INSERT INTO match (Timestamp, server, MapLabel, GameMode, PlayerCount, bTeams, Team0Score, Team1Score) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (data['Timestamp'], server_name, data['MapLabel'], data['GameMode'], data['PlayerCount'],
                     data['bTeams'], data['Team0Score'], data['Team1Score']))
                conn.commit()
            #            c.execute(f"INSERT INTO match (Timestamp, server, MapLabel, GameMode, "
    #                      f"PlayerCount, bTeams, Team0Score, Team1Score) VALUES "
    #                      f"('{data['Timestamp']}', '{server_name}', '{data['MapLabel']}', '{data['GameMode']}', "
    #                      f"'{data['PlayerCount']}','{data['bTeams']}', '{data['Team0Score']}', '{data['Team1Score']}'")
    #            conn.commit()
            else:
                print('Error sending data match')


def save_users_data_to_db(data):
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        # print(data)
        for player in data["allStats"]:
            c.execute(f"SELECT * FROM match_users WHERE Timestamp='{data['Timestamp']}' AND uniqueId_player='{player['uniqueId']}'")
            result = c.fetchone()
            if result is None:
                if send_json(data, 'allStats'):
                    print('save to bd match_users')
                    stats = {stat["statType"]: stat["amount"] for stat in player["stats"]}
                    c.execute(
                        "INSERT INTO match_users (Timestamp, uniqueId_player, playerName, teamId, Death, Assist, Kill, Headshot, BombPlanted, Experience) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (data['Timestamp'], player['uniqueId'], player['playerName'], player['teamId'],
                         stats.get('Death', None), stats.get('Assist', None), stats.get('Kill', None),
                         stats.get('Headshot', None), stats.get('BombPlanted', None), stats.get('Experience', None)))
                    conn.commit()
                else:
                    print('Error sending data match_users')


#{"BombData": {"Player": "76561198124316469", "BombInteraction": "BombDefused"}}
def save_Bomb_data_to_db(data):
    print(f'save_Bomb_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM BombData WHERE Timestamp='{data['Timestamp']}'")
        result = c.fetchone()

        if result is None:
            if send_json(data, 'BombData'):
                print(f'save to bd BombData')

                """ {"BombData": {"Player": "76561198124316469", "BombInteraction": "BombDefused"}}
                id,       Timestamp,          server Player,             BombInteraction
                KillData  2023.03.14-08.29.12 tsts   76561198124316469   BombDefused        
                """
                if 'BombData' in data:
                    c.execute("INSERT INTO BombData "
                              "(Timestamp, server, Player, BombInteraction) VALUES "
                              "(?, ?, ?, ?)",
                              (data['Timestamp'], server_name, data.get('Player', None),
                               data['BombData']['BombInteraction']))
                    conn.commit()
            else:
                print('Error sending data BombData')


def save_event_data_to_db(data):
    #print(f'save_match_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()

        if "RoundState" in data:
            #print(f'data[RoundState][Timestamp] {type(data["RoundState"]["Timestamp"])} = {data["RoundState"]["Timestamp"]}')
            c.execute(f"SELECT * FROM event WHERE Timestamp='{data['RoundState']['Timestamp']}'")
            result = c.fetchone()
        elif "RoundEnd" in data:
            c.execute(f"SELECT * FROM event WHERE Timestamp='{data['Timestamp']}'")
            result = c.fetchone()

        if result is None:
            if send_json(data, 'event'):
                print(f'save to bd event')
                """
                event,          Timestamp,            server      State,      Round,      WinningTeam
                RoundState      2023.03.14-08.29.12   tsts        Starting     
                RoundEnd        2023.03.14-08.29.28   tsts        Ended       10          1
                """
                if 'RoundState' in data and data.get('RoundState', {}).get('State') != 'Ended' and data.get('RoundState', {}).get('State') != 'StandBy':

                   c.execute("INSERT INTO event (event, Timestamp, server, State) VALUES (?, ?, ?, ?)",
                             ("RoundState", data['RoundState']['Timestamp'], server_name, data['RoundState']['State']))
                elif 'RoundEnd' in data:
                    c.execute("INSERT INTO event (event, Timestamp, server, State, Round, WinningTeam) VALUES (?, ?, "
                              "?, ?, ?, ?)",
                        ('RoundState', data['Timestamp'], server_name, 'Ended', data['RoundEnd']['Round'],
                         data['RoundEnd']['WinningTeam']))
                conn.commit()
            else:
                print('Error sending data event')



def save_Kill_data_to_db(data):
    # print(f'save_match_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM KillData WHERE Timestamp='{data['Timestamp']}'")
        result = c.fetchone()
        if result is None:
            if send_json(data, 'KillData'):
                print(f'save to bd KillData')
                """
                event,    Timestamp,          server Killer,  KillerTeamID, Killed, KilledTeamID, KilledBy, Headshot
                KillData  2023.03.14-08.29.12 tsts   765611   1             765611  1             de        true                     
               '''data = {"KillData": {"Killer": "76561198124316469", "KillerTeamID": 1, "Killed": "76561198124316469", "KilledTeamID": 1, "KilledBy": "1911", "Headshot": true}} 2023.03.14-08.30.51'''
                """
                if 'KillData' in data:
                    c.execute("INSERT INTO KillData "
                              "(event, Timestamp, server, Killer, KillerTeamID, Killed, KilledTeamID, KilledBy, "
                              "Headshot) VALUES ('KillData', ?, ?, ?, ?, ?, ?, ?, ?)",
                              (data['Timestamp'], server_name, data['KillData']['Killer'], data['KillData']['KillerTeamID'],
                               data['KillData']['Killed'], data['KillData']['KilledTeamID'], data['KillData']['KilledBy'],
                               data['KillData']['Headshot']))
                    conn.commit()
            else:
                print('Error sending data KillData')



def send_json2(json_data, type_event):
    if json_data:
        data = {'new_data': json.dumps(json_data), 'server_name': server_name, 'type_event':type_event}
        while True:
            try:
                response = requests.post(url, json=data)
                if response.status_code == 200:
                    print("Данные успешно отправлены!")
                    return True
                else:
                    print(f"Произошла ошибка: {response.status_code}")
                    return False
            except ConnectionRefusedError:
                print("Сервер недоступен, попытка повторной отправки через 5 секунд")
                time.sleep(30)

def send_json(json_data, type_event):
    if json_data:
        data = {'new_data': json.dumps(json_data), 'server_name': server_name, 'type_event':type_event}
        response = requests.post(url, json=data)
        if response.status_code == 200:
            print("Данные успешно отправлены!")
            return True
        else:
            print(f"Произошла ошибка: {response.status_code}")
            return False

def parse_folder(fldr_path):
    print('Parse all logs..')
    for filename in os.listdir(fldr_path):
        filepath = os.path.join(fldr_path, filename)
        if os.path.isfile(filepath) and filename.endswith(".log"):
            print(f'Parse {filepath}..')
            parse_log_files(filepath)

def restart_program():
    python = sys.executable
    os.execl(python, python, *sys.argv)

def run_parse_log_files():
    schedule.every(5).minutes.do(lambda: parse_log_files(def_pavlov_logfile))
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':

    create_db()
    #parse_folder(logs_folder)
    #flask_thread = threading.Thread(target=run_parse_log_files)
   # flask_thread.start()

    tst_dict = { "KillData" : {"Killer": "76561198391655588", "KillerTeamID": 1, "Killed": "76561198391655584", "KilledTeamID": 1, "KilledBy": "de", "Headshot": True } ,
                 "Timestamp": "2024.11.11-11.11.12" }

    send_json(tst_dict, 'KillData')

   # parse_log_files('Pavlov_fullstats.log')

