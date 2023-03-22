# -*- coding: utf-8 -*-
import json
import platform
import sqlite3
from flask import Flask, g, render_template, request, jsonify
from bs4 import BeautifulSoup
import requests

app = Flask(__name__)

database = 'stats.db'
conn = sqlite3.connect(database)
time_refresh_parsing = 300
server_name = 'local'

if platform.system() == 'Linux':
    logs_folder = '/home/ub/pavlovserver/Pavlov/Saved/Logs'
else:
    logs_folder = 'Logs'


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
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='map_name' ''')
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE map_name (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        UGCname TEXT NOT NULL,
                        name TEXT)''')
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='player_name' ''')
    if c.fetchone()[0] == 0:
        c.execute('''CREATE TABLE player_name (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        steam_id TEXT UNIQUE NOT NULL,
                        name TEXT)''')
    conn.close()

def get_map_name_from_workshop_id(workshop_id):
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute("SELECT name FROM map_name WHERE UGCname=?", (workshop_id,))
        row = c.fetchone()
        if row is not None:
            return row[0]
        else:
            workshop_url = f'https://steamcommunity.com/sharedfiles/filedetails/?id={workshop_id[3:]}'
            response = requests.get(workshop_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            map_name = soup.find('div', {'class': 'workshopItemTitle'}).text.strip()
            c.execute("INSERT INTO map_name (UGCname, name) VALUES (?, ?)", (workshop_id, map_name))
            conn.commit()
            return map_name

def get_player_name_from_id(steam_id):
    #print(f'steam_id  = {steam_id}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute("SELECT name FROM player_name WHERE steam_id=? LIMIT 1", (steam_id,))
        row_player_name = c.fetchone()
        if row_player_name is None:
            c.execute("SELECT playerName FROM match_users WHERE uniqueId_player=? LIMIT 1", (steam_id,))
            row_match_users = c.fetchone()

        if row_player_name is not None:
            return row_player_name[0]
        elif row_match_users is not None:
            print(f'if row_player_name is None: new steam_id = {steam_id} row_match_users = {row_match_users}')
            c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)",
                      (steam_id, row_match_users))
            conn.commit()
            return row_match_users[0]
        else:
            url = f"https://steamcommunity.com/profiles/{steam_id}/"
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            username_element = soup.find('span', {'class': 'actual_persona_name'})
            print(username_element)
            if username_element is None:
                print("Не удалось найти имя пользователя на странице профиля Steam")
                return None
            else:
                print(f'steam_id new = {steam_id}, new name = {username_element.text.strip()}')
                c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)", (steam_id, username_element.text.strip()))
                conn.commit()
                return username_element.text.strip()

def save_event_data_to_db(data, server_name):
    # print(f'save_event_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()

        if "RoundState" in data:
            # print(                f'data[RoundState][Timestamp] {type(data["RoundState"]["Timestamp"])} = {data["RoundState"]["Timestamp"]}')
            c.execute(f"SELECT * FROM event WHERE Timestamp='{data['RoundState']['Timestamp']}'")
        elif "RoundEnd" in data:
            #  print(f'data[Timestamp] {type(data["Timestamp"])} = {data["Timestamp"]}')
            c.execute(f"SELECT * FROM event WHERE Timestamp='{data['Timestamp']}'")

        result = c.fetchone()
        if result is None:
            #  print(f'saving to bd event ')
            """
             event,          Timestamp,            server      State,      Round,      WinningTeam
             RoundState      2023.03.14-08.29.12   tsts        Starting     
             RoundEnd        2023.03.14-08.29.28   tsts        Ended       10          1
             """
            if 'RoundState' in data and data.get('RoundState', {}).get('State') != 'Ended':
                # print(f'save to bd event RoundState')
                c.execute(f"INSERT INTO event "
                          f"(event, Timestamp, server, State) VALUES "
                          f"('RoundState', '{data['RoundState']['Timestamp']}', '{server_name}', '{data['RoundState']['State']}')")
            elif 'RoundEnd' in data:
                # print(f'save to bd event RoundEnd')
                c.execute(f"INSERT INTO event "
                          f"(event, Timestamp, server, State, Round, WinningTeam) VALUES "
                          f"('RoundState', '{data['Timestamp']}', '{server_name}', 'Ended', "
                          f"'{data['RoundEnd']['Round']}','{data['RoundEnd']['WinningTeam']}')")
            conn.commit()
        else:
            pass
        # print('no change event')


def save_Kill_data_to_db(data, server_name):
    #print(f'save_match_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM KillData WHERE Timestamp='{data['Timestamp']}'")
        result = c.fetchone()
        if result is None:
            # print(f'save to bd KillData')
            """
            event,    Timestamp,          server Killer,  KillerTeamID, Killed, KilledTeamID, KilledBy, Headshot
            KillData  2023.03.14-08.29.12 tsts   765611   1             765611  1             de        true                     

            """
            if 'KillData' in data:
                killer_id = data['KillData']['Killer']
                killed_by_id = data['KillData']['KilledBy']
                c.execute("SELECT name FROM player_name WHERE steam_id=? LIMIT 1", (killer_id,))
                killer_row = c.fetchone()
                if killer_row is None:
                    killer_name = get_player_name_from_id(killer_id)
                   # c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)", (killer_id, killer_name))
                    print(f'if killer_row is None: killer_name = {killer_name}')
                c.execute("SELECT name FROM player_name WHERE steam_id=? LIMIT 1", (killed_by_id,))
                killed_by_row = c.fetchone()
                if killed_by_row is None:
                    killed_by_name = get_player_name_from_id(killed_by_id)
                    #c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)", (killed_by_id, killed_by_name))
                    print(f'if killed_by_row is None: killed_by_row = {killer_name}')
                c.execute(
                    f"INSERT INTO KillData (event, Timestamp, server, Killer, KillerTeamID, Killed, KilledTeamID, KilledBy, Headshot) "
                    f"VALUES ('KillData', '{data['Timestamp']}', '{server_name}', '{killer_id}', "
                    f"'{data['KillData']['KillerTeamID']}', '{data['KillData']['Killed']}', "
                    f"'{data['KillData']['KilledTeamID']}', '{killed_by_id}', '{data['KillData']['Headshot']}')")
                conn.commit()
        else:
            pass
        #  print('no change KillData')

def save_Bomb_data_to_db(data):
    print(f'save_Bomb_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM BombData WHERE Timestamp='{data['Timestamp']}'")
        result = c.fetchone()

        if result is None:
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


def save_match_data_to_db(data, server_name):
    # print(f'save_match_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM match WHERE Timestamp='{data['Timestamp']}'")
        result = c.fetchone()
        if result is None:
            # print('save to bd match')
            c.execute(
                "INSERT INTO match (Timestamp, server, MapLabel, GameMode, PlayerCount, bTeams, Team0Score, "
                "Team1Score) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (data['Timestamp'], server_name, data['MapLabel'], data['GameMode'], data['PlayerCount'],
                 data['bTeams'], data['Team0Score'], data['Team1Score']))
            conn.commit()


def save_users_data_to_db(data):
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        # print(data)
        for player in data["allStats"]:
            c.execute(
                f"SELECT * FROM match_users WHERE Timestamp='{data['Timestamp']}' AND uniqueId_player='{player['uniqueId']}'")
            result = c.fetchone()
            if result is None:
                #  print('save to bd match_users')
                stats = {stat["statType"]: stat["amount"] for stat in player["stats"]}
                c.execute(
                    "INSERT INTO match_users (Timestamp, uniqueId_player, playerName, teamId, Death, "
                    "Assist, Kill, Headshot, BombPlanted, Experience) VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (data['Timestamp'], player['uniqueId'], player['playerName'], player['teamId'],
                     stats.get('Death', None), stats.get('Assist', None), stats.get('Kill', None),
                     stats.get('Headshot', None),
                     stats.get('BombPlanted', None), stats.get('Experience', None)))
                conn.commit()


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(database)
        db.row_factory = sqlite3.Row
    return db


# @app.teardown_appcontext
# def close_connection(exception):
#    db = getattr(g, '_database', None)
#    if db is not None:
#        db.close()

@app.route('/')
def index_start_pge():
    return render_template('index.html')


@app.route('/users')
def show_stats_users():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT Timestamp, uniqueId_player, playerName, COUNT(*) AS count, "
                "SUM(COALESCE(Kill, 0)) AS total_kill, "
                "SUM(COALESCE(Death, 0)) AS total_death,   "
                "ROUND(SUM(COALESCE(Kill, 0))/CAST(SUM(COALESCE(Death, 0)) AS FLOAT), 2) AS kill_death_ratio, "
                "SUM(COALESCE(Assist, 0)) AS total_assist, "
                "SUM(COALESCE(Headshot, 0)) AS total_headshot, "
                "ROUND(SUM(COALESCE(Headshot, 0)) / CAST(SUM(COALESCE(Kill, 0)) AS FLOAT) * 100 , 2) AS headshot_percent, "
                "SUM(COALESCE(BombPlanted, 0)) AS total_bombplanted, "
                "SUM(COALESCE(Experience, 0)) AS total_experience "
                "FROM match_users GROUP BY uniqueId_player ORDER BY count DESC;")
    match_users = cur.fetchall()

    cur.execute(
        "SELECT uniqueId_player, ROUND(COALESCE(SUM(CASE WHEN teamId = (CASE WHEN Team0Score > Team1Score THEN 0 ELSE 1 END) THEN 1 ELSE 0 END), 0) * 100 / NULLIF(CAST(COUNT(*) AS FLOAT), 0), 2) AS win_rate FROM match_users INNER JOIN match ON match_users.Timestamp = match.Timestamp "
        "WHERE match.PlayerCount > 7 GROUP BY uniqueId_player;")

    winrate = cur.fetchall()
    #    winrate = [(row[0], row[1], row[2], 0 if row[3] is None else row[3]) for row in winrate]
    return render_template('users.html', match_users=match_users, winrate=winrate)


@app.route('/player/<name>')
def player(name):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT   mu.uniqueId_player, "
                "playerName, "
                "COUNT(mu.Timestamp) AS num_matches, "
                "playerName, "
                "SUM(COALESCE(mu.Kill, 0)) AS total_kill, "
                "SUM(COALESCE(mu.Death, 0)) AS total_death, "
                "ROUND(SUM(COALESCE(mu.Kill, 0)) / NULLIF(SUM(COALESCE(mu.Death, 0)), 0), 2) AS kill_death_ratio, "
                "SUM(COALESCE(mu.Assist, 0)) AS total_assist, "
                "SUM(COALESCE(mu.Headshot, 0)) AS total_headshot, "
                "ROUND(SUM(COALESCE(mu.Headshot, 0)) / CAST(SUM(COALESCE(mu.Kill, 0)) AS FLOAT) * 100 , 2) AS headshot_percent, "
                "SUM(COALESCE(mu.BombPlanted, 0)) AS total_bombplanted, "
                "SUM(COALESCE(mu.Experience, 0)) AS total_experience FROM match_users AS mu "
                "INNER JOIN match AS m ON mu.Timestamp = m.Timestamp "
                "WHERE mu.uniqueId_player = ? GROUP BY mu.uniqueId_player;", (name,))
    player = cur.fetchone()

    cur.execute("SELECT COUNT(*) AS wins FROM match m JOIN match_users u ON m.Timestamp = u.Timestamp WHERE "
                "u.uniqueId_player = ? AND ((m.Team0Score > m.Team1Score AND u.teamId = 0) "
                "OR (m.Team1Score > m.Team0Score AND u.teamId = 1))", (name,))

    match_wins = cur.fetchone()

    cur.execute(
        "SELECT match.Timestamp, match.Timestamp, match.MapLabel, match.server, match.GameMode, match.Team0Score, match.Team1Score,        "
        "match_users.Kill, match_users.Death,        ROUND(CAST(match_users.Kill AS FLOAT) / NULLIF("
        "match_users.Death, 0), 2) AS KD,        match_users.BombPlanted,       match_users.teamId AS "
        "player_team,       CASE          WHEN match_users.teamId = 0 AND match.Team0Score > match.Team1Score "
        "THEN 'Win'          WHEN match_users.teamId = 1 AND match.Team1Score > match.Team0Score THEN "
        "'Win'          ELSE 'Lose'        END AS result FROM match INNER JOIN match_users ON "
        "match.Timestamp = match_users.Timestamp WHERE match_users.uniqueId_player = ? GROUP BY match.Timestamp;",
        (name,))
    matches = cur.fetchall()

    cur.execute(
        "SELECT ROUND(COALESCE(SUM(CASE WHEN teamId = (CASE WHEN Team0Score > Team1Score THEN 0 ELSE 1 END) THEN 1 ELSE 0 END), 0) * 100 / NULLIF(CAST(COUNT(*) AS FLOAT), 0), 2) AS win_rate "
        "FROM match_users INNER JOIN match ON match_users.Timestamp = match.Timestamp "
        "WHERE match.PlayerCount > 7 AND match_users.uniqueId_player = ?;", (name,))
    winrate = cur.fetchone()

    cur.execute('SELECT Timestamp , server , Killer , KillerTeamID , Killed , KilledTeamID , KilledBy , Headshot FROM KillData WHERE Killer = ? or Killed = ?', (name,name,))
    kills = cur.fetchall()


    conn.close()

    return render_template('player.html', player=player, matches=matches, winrate=winrate, match_wins=match_wins, kills=kills)


@app.route('/match/<Timestamp>')
def match(Timestamp):
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT Timestamp, server, MapLabel, GameMode, PlayerCount, bTeams, Team0Score, Team1Score  FROM match WHERE  Timestamp = ?;",
        (Timestamp,))
    match = cur.fetchall()

    for i in range(len(match)):
        map_label = match[i][2]
        if map_label.startswith("UGC"):
            map_name = get_map_name_from_workshop_id(map_label)
            match[i] = match[i][:2] + (map_name,) + match[i][3:]


    cur.execute("SELECT playerName, teamId FROM match_users WHERE Timestamp = ?  ORDER BY teamId ASC", (Timestamp,))
    players = cur.fetchall()

    cur.execute(
        "SELECT uniqueId_player, playerName, Kill, Death, Assist, Headshot, BombPlanted, Experience FROM match_users WHERE Timestamp = ? AND teamId = 0",
        (Timestamp,))
    players_team0 = cur.fetchall()
    cur.execute(
        "SELECT uniqueId_player, playerName, Kill, Death, Assist, Headshot, BombPlanted, Experience FROM match_users WHERE Timestamp = ? AND teamId = 1",
        (Timestamp,))
    players_team1 = cur.fetchall()
    max_players_count = max(len(players_team0), len(players_team1))
    range_m = range(max(len(players_team0), len(players_team1)))

    cur.execute('SELECT * FROM KillData')
    kills = cur.fetchall()

    conn.close()
    return render_template('match.html', match=match, players=players, range_m=range_m, len=len, max=max,
                           max_players_count=max_players_count, players_team0=players_team0,
                           players_team1=players_team1, kills=kills)


@app.route('/matchs')
def show_stats_matchs():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT Timestamp, server, MapLabel, GameMode, PlayerCount, bTeams, Team0Score, Team1Score "
                " FROM match ORDER BY Timestamp DESC")
    match = cur.fetchall()

    for i in range(len(match)):
        map_label = match[i][2]
        if map_label.startswith("UGC"):
            map_name = get_map_name_from_workshop_id(map_label)
            match[i] = match[i][:2] + (map_name,) + match[i][3:]

    return render_template('matchs.html', match=match)


@app.route('/data_in', methods=['POST'])
def receive_data():
    data = request.json.get('new_data')
    data_server_name = request.json.get('server_name')
    data_type_event = request.json.get('type_event')
    # print(f'=== {data_server_name} : {data}')
    if data and data_server_name:
        # print(f'Принято JSON с сервера "{data_server_name}" событие "{data_type_event}" : "{data}"')
        if data_type_event == 'allStats':
            save_match_data_to_db(json.loads(data), data_server_name)
            save_users_data_to_db(json.loads(data))
        elif data_type_event == 'event':
            save_event_data_to_db(json.loads(data), data_server_name)
        elif data_type_event == 'KillData':
            save_Kill_data_to_db(json.loads(data), data_server_name)
        elif data_type_event == 'BombData':
            save_Bomb_data_to_db(json.loads(data))
        else:
            print(f'[ERR] Приняты неизвестные данные  {data_type_event} =  {data}')

        return 'Data received!'
    else:
        # print('No data received')
        return 'Error: Bad Request', 400


@app.route('/rounds/<Timestamp>')
def rounds(Timestamp):
    conn = get_db()
    cur = conn.cursor()
    Server_name = request.args.get('Server_name')
    cur.execute("SELECT * FROM event WHERE Timestamp BETWEEN (SELECT Timestamp FROM event WHERE State = 'Start' AND "
                "Server = ? AND Timestamp <= ? ORDER BY Timestamp DESC LIMIT 1) AND "
                "? AND Server = ? AND State = 'Ended';", (Server_name, Timestamp, Timestamp, Server_name,))
    #       0=id	1=event	2=Timestamp	3=server	4=State	5=Round	6=WinningTeam
    #rounds = [{'Round': row[5], 'WinningTeam': row[6]} for row in cur.fetchall()]
    rounds = list(map(lambda row: {'Round': row[5], 'Timestamp': row[2], 'WinningTeam': row[6]}, cur.fetchall()))
    conn.close()
    return jsonify(rounds)


def run_flask():
    app.run(host='0.0.0.0', port=5000)


if __name__ == '__main__':
    create_db()
    run_flask()

#   Запуск отдельным потоком
#   flask_thread = threading.Thread(target=run_flask)
#   flask_thread.start()
