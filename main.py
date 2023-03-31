# -*- coding: utf-8 -*-
import json
import platform
import sqlite3
from flask import Flask, g, render_template, request, jsonify
from bs4 import BeautifulSoup
import requests
from datetime import datetime

app = Flask(__name__, static_url_path='/static')

database = 'stats.db'
conn = sqlite3.connect(database)
time_refresh_parsing = 300
# server_name = 'local'

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
                        steam_id INTEGER NOT NULL,
                        name TEXT)''')
    c.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='player_name_steam_id_idx'")
    if c.fetchone() is None:
        c.execute("CREATE INDEX player_name_steam_id_idx ON player_name(steam_id)")
    conn.close()


def get_dict_rnd(end_match, start_match, server_name):
   # print(f'end_match = {end_match} start_match = {start_match}, server_name = {server_name}')
    with sqlite3.connect(database) as conn:
        # Выполнение запроса на получение данных
        c = conn.cursor()
        c.execute("SELECT Timestamp, round, WinningTeam FROM event WHERE server = ? AND State = 'Ended' AND Timestamp "
                  ">= ? AND Timestamp <= ? ORDER BY Timestamp ASC;", (server_name, start_match, end_match))
        events_data = c.fetchall()


        round_data = {}
        team0_score = 0
        team1_score = 0
        for row in events_data:
            end_rnd = row[0]  # Время события
            round_num = row[1]  # Номер раунда
            win_team = row[2]  # WinningTeam

            c = conn.cursor()
            c.execute("SELECT Timestamp FROM event WHERE event = 'RoundState' AND State = 'Started' AND server = ? AND "
                      "Timestamp <= ? ORDER BY Timestamp DESC LIMIT 1", (server_name, end_rnd,))
            start_rnd = c.fetchall()

            start_rnd_str = start_rnd[0][0]
            start_rnd_time = datetime.strptime(start_rnd_str, '%Y.%m.%d-%H.%M.%S')
            start_rnd_data = start_rnd_time.strftime('%Y.%m.%d-%H.%M.%S')

            c.execute(
                "SELECT Timestamp, (SELECT name FROM player_name WHERE steam_id=Killer), KillerTeamID, (SELECT name "
                "FROM player_name WHERE steam_id=Killed), KilledTeamID, KilledBy, Headshot, Killer, Killed "
                "FROM KillData "
                "WHERE event = 'KillData' AND server = ? "
                "AND Timestamp >= ? "
                "AND Timestamp <= ? "
                "ORDER BY Timestamp ASC;",
                (server_name, start_rnd_data, end_rnd)
            )
            kills = c.fetchall()

            if win_team == 0:
                team0_score += 1
            elif win_team == 1:
                team1_score += 1

            # Добавление данных в словарь
            if round_num not in round_data:
                round_data[round_num] = {'team0_score': 0, 'team1_score': 0, 'rnd_start': '', 'event': []}

            round_data[round_num]['rnd_start'] = start_rnd_data
            round_data[round_num]['team0_score'] = team0_score
            round_data[round_num]['team1_score'] = team1_score


            for row in kills:
                kill_Timestamp = row[0]
                kill_Killer = row[1]
                kill_KillerTeamID = row[2]
                kill_Killed = row[3]
                kill_KilledTeamID = row[4]
                kill_KilledBy = row[5]
                kill_Headshot = row[6]
                kill_Killer_id = row[7]
                kill_Killed_id = row[8]

                kill_Timestamp_str = kill_Timestamp
                kill_Timestamp_time = datetime.strptime(kill_Timestamp_str, '%Y.%m.%d-%H.%M.%S')


                new_time = str(kill_Timestamp_time - start_rnd_time)
                event = {'time': new_time,
                         'killer': kill_Killer, 'killer_id': kill_Killer_id,  'killer_Teamid': kill_KillerTeamID,
                         'killed': kill_Killed, 'killed_id': kill_Killed_id, 'killed_Teamid': kill_KilledTeamID,
                         'weapon': kill_KilledBy, 'Headshot': kill_Headshot}

                if round_num not in round_data:
                    round_data[round_num] = {'event': []}
                round_data[round_num]['event'].append(event)

    return round_data



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
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        # c.execute("SELECT name FROM player_name WHERE steam_id=? LIMIT 1", (steam_id,))
        c.execute("SELECT name FROM player_name INDEXED BY player_name_steam_id_idx WHERE steam_id=? LIMIT 1",
                  (steam_id,))
        row_player_name = c.fetchone()
        if row_player_name is None:
            c.execute("SELECT playerName FROM match_users WHERE uniqueId_player=? LIMIT 1", (steam_id,))
            row_match_users = c.fetchone()
            if row_match_users is not None and row_match_users[0] != '':
                c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)",
                          (steam_id, row_match_users[0]))
                conn.commit()
                return row_match_users[0]
            else:
                url = f"https://steamcommunity.com/profiles/{steam_id}/"
                response = requests.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')
                username_element = soup.find('span', {'class': 'actual_persona_name'})
                if username_element is None:
                    print("Not found Steam name")
                    return None
                else:
                    c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)",
                              (steam_id, username_element.text.strip()))
                    conn.commit()
                    return username_element.text.strip()
        else:
            return row_player_name[0]


def save_event_data_to_db(data, server_name):
    # print(f'save_event_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
    with sqlite3.connect(database) as conn:
        c = conn.cursor()
        if "RoundState" in data:
            c.execute(f"SELECT * FROM event WHERE Timestamp='{data['RoundState']['Timestamp']}'")
        elif "RoundEnd" in data:
            c.execute(f"SELECT * FROM event WHERE Timestamp='{data['Timestamp']}'")
        result = c.fetchone()
        if result is None:
            if 'RoundState' in data and data.get('RoundState', {}).get('State') != 'Ended':
                c.execute(f"INSERT INTO event "
                          f"(event, Timestamp, server, State) VALUES "
                          f"('RoundState', '{data['RoundState']['Timestamp']}', '{server_name}', '{data['RoundState']['State']}')")
            elif 'RoundEnd' in data:
                c.execute(f"INSERT INTO event "
                          f"(event, Timestamp, server, State, Round, WinningTeam) VALUES "
                          f"('RoundState', '{data['Timestamp']}', '{server_name}', 'Ended', "
                          f"'{data['RoundEnd']['Round']}','{data['RoundEnd']['WinningTeam']}')")
            conn.commit()


def save_Kill_data_to_db(data, server_name):
    print(f'save_match_data_to_db data {type(data)} = {data} server_name {type(server_name)} = {server_name}')
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
                killed_id = data['KillData']['Killed']
                c.execute("SELECT name FROM player_name WHERE steam_id=? LIMIT 1", (killer_id,))
                killer_row = c.fetchone()
                if killer_row is None:
                    killer_name = get_player_name_from_id(killer_id)
                    c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)", (killer_id, killer_name))
                    print(f'if killer_row is None: killer_name = {killer_name}')
                c.execute("SELECT name FROM player_name WHERE steam_id=? LIMIT 1", (killed_id,))
                killed_row = c.fetchone()
                if killed_row is None:
                    killed_by_name = get_player_name_from_id(killed_id)
                    c.execute("INSERT INTO player_name (steam_id, name) VALUES (?, ?)", (killed_id, killed_by_name))
                    print(f'if killed_by_row is None: killed_by_row = {killed_by_name}')
                c.execute(
                    f"INSERT INTO KillData (event, Timestamp, server, Killer, KillerTeamID, Killed, KilledTeamID, KilledBy, Headshot) "
                    f"VALUES ('KillData', '{data['Timestamp']}', '{server_name}', '{killer_id}', "
                    f"'{data['KillData']['KillerTeamID']}', '{data['KillData']['Killed']}', "
                    f"'{data['KillData']['KilledTeamID']}', '{killed_id}', '{data['KillData']['Headshot']}')")
                conn.commit()
        else:
            pass
        #  print('no change KillData')


def save_Bomb_data_to_db(data, server_name):
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

    for i in range(len(match_users)):
        user_id = match_users[i][1]
        player_name = get_player_name_from_id(user_id)
        match_users[i] = match_users[i][:2] + (player_name,) + match_users[i][3:]

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
                "(SELECT name from player_name WHERE steam_id = uniqueId_player) AS player_name, "
                "COUNT(mu.Timestamp) AS num_matches, "
                "SUM(COALESCE(mu.Kill, 0)) AS total_kill, "
                "SUM(COALESCE(mu.Death, 0)) AS total_death, "
                #"ROUND(SUM(COALESCE(mu.Kill, 0)) / NULLIF(SUM(COALESCE(mu.Death, 0)), 0), 2) AS kill_death_ratio, "
                "ROUND(SUM(COALESCE(mu.Kill, 0))/CAST(SUM(COALESCE(mu.Death, 0)) AS FLOAT), 2) AS kill_death_ratio, "
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

    cur.execute(
        'SELECT Timestamp , server , Killer , KillerTeamID , Killed , KilledTeamID , KilledBy , Headshot, (SELECT name FROM player_name WHERE steam_id=Killer),  (SELECT name FROM player_name WHERE steam_id=Killed)  FROM KillData WHERE Killer = ? or Killed = ?',
        (name, name,))
    kills = cur.fetchall()

    conn.close()

    return render_template('player.html', player=player, matches=matches, winrate=winrate, match_wins=match_wins,
                           kills=kills)


@app.route('/match/<Timestamp>')
def match(Timestamp):
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT Timestamp, server, MapLabel, GameMode, PlayerCount, bTeams, Team0Score, Team1Score  FROM match WHERE  Timestamp = ?;",
        (Timestamp,))
    match = cur.fetchall()
    server = match[0][1]
    cur.execute(
        "SELECT Timestamp FROM event WHERE event = 'RoundState' AND State = 'Start' AND server = ? AND Timestamp <= ? ORDER BY Timestamp DESC LIMIT 1;",
        (server, Timestamp,))
    start_match_time = cur.fetchall()

    for i in range(len(match)):
        map_label = match[i][2]
        if map_label.startswith("UGC"):
            map_name = get_map_name_from_workshop_id(map_label)
            match[i] = match[i][:2] + (map_name,) + match[i][3:]

    cur.execute("SELECT playerName, teamId FROM match_users WHERE Timestamp = ?  ORDER BY teamId ASC", (Timestamp,))
    players = cur.fetchall()

    cur.execute(
        "SELECT uniqueId_player, (SELECT name from player_name WHERE steam_id = uniqueId_player), Kill, Death, Assist, Headshot, BombPlanted, Experience FROM match_users WHERE Timestamp = ? AND teamId = 0",
        (Timestamp,))
    players_team0 = cur.fetchall()
    cur.execute(
        "SELECT uniqueId_player, (SELECT name from player_name WHERE steam_id = uniqueId_player), Kill, Death, Assist, Headshot, BombPlanted, Experience FROM match_users WHERE Timestamp = ? AND teamId = 1",
        (Timestamp,))
    players_team1 = cur.fetchall()
    max_players_count = max(len(players_team0), len(players_team1))
    range_m = range(max(len(players_team0), len(players_team1)))

    cur.execute(
        "SELECT Timestamp, (SELECT name FROM player_name WHERE steam_id=Killer), KillerTeamID, (SELECT name FROM player_name WHERE steam_id=Killed), KilledTeamID, KilledBy, Headshot, Killer, Killed "
        "FROM KillData "
        "WHERE event = 'KillData' AND server = ? "
        "AND Timestamp >= (SELECT Timestamp FROM event WHERE event = 'RoundState' "
        "                  AND State = 'Start' AND server = ? AND Timestamp <= ? "
        "                  ORDER BY Timestamp DESC LIMIT 1) "
        "AND Timestamp <= ? "
        "ORDER BY Timestamp ASC;",
        (server, server, Timestamp, Timestamp)
    )
    kills = cur.fetchall()

    #    cur.execute("SELECT Timestamp, round, WinningTeam FROM event WHERE server = ? AND State = 'Ended' AND Timestamp >= ? AND Timestamp <= ? ORDER BY Timestamp ASC;",
    #        (server, Timestamp, start_match_time),
    #    )
    #    round_state = cur.fetchone()

    #    cur.execute(
    #        "SELECT Timestamp, SteamID, PlayerName FROM BombData "
    #        "WHERE event = 'BombPlanted' AND server = ? "
    #        "AND Timestamp >= (SELECT Timestamp FROM event WHERE event = 'RoundState' "
    #        "                  AND State = 'FreezeTime' AND server = ? AND Timestamp <= ? "
    #        "                  ORDER BY Timestamp DESC LIMIT 1) "
    #        "AND Timestamp <= ? ORDER BY Timestamp ASC;",
    #        (server, server, round_state[1], round_state[1]),
    #    )
    #    bomb_plant = cur.fetchone()

   # round_data = {1: {'team0_score': 0, 'team1_score': 1, 'rnd_start': '2023.03.27-18.05.45', 'event': [{'time': '0:00:22', 'killer': 'STILBITE|youtube', 'killed': 'Plex', 'weapon': 'sock'}, {'time': '0:00:27', 'killer': 'STILBITE|youtube', 'killed': 'alex', 'weapon': 'sock'}, {'time': '0:00:51', 'killer': 'STILBITE|youtube', 'killed': 'X7XVendettaX7X', 'weapon': 'sock'}]}, 2: {'team0_score': 1, 'team1_score': 1, 'rnd_start': '2023.03.27-18.06.56', 'event': [{'time': '0:00:34', 'killer': 'X7XVendettaX7X', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:41', 'killer': '[TL] Kungfusnail', 'killed': 'X7XVendettaX7X', 'weapon': 'AR'}, {'time': '0:00:47', 'killer': 'STILBITE|youtube', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}, {'time': '0:00:50', 'killer': 'SirRobertsOBE', 'killed': '[TL] Kungfusnail', 'weapon': '57'}, {'time': '0:00:56', 'killer': 'Plex', 'killed': 'STILBITE|youtube', 'weapon': 'SMG'}]}, 3: {'team0_score': 2, 'team1_score': 1, 'rnd_start': '2023.03.27-18.08.12', 'event': [{'time': '0:00:14', 'killer': 'X7XVendettaX7X', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}, {'time': '0:00:32', 'killer': 'SirRobertsOBE', 'killed': '[TL] Kungfusnail', 'weapon': 'AR'}, {'time': '0:00:40', 'killer': 'SirRobertsOBE', 'killed': 'STILBITE|youtube', 'weapon': 'AR'}, {'time': '0:00:42', 'killer': 'SirRobertsOBE', 'killed': 'Sin_Of_Bob', 'weapon': 'AR'}]}, 4: {'team0_score': 3, 'team1_score': 1, 'rnd_start': '2023.03.27-18.09.15', 'event': [{'time': '0:00:13', 'killer': 'SirRobertsOBE', 'killed': '[TL] Kungfusnail', 'weapon': 'AR'}, {'time': '0:00:17', 'killer': 'Plex', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:18', 'killer': 'Gatto_nero', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}, {'time': '0:00:29', 'killer': 'STILBITE|youtube', 'killed': 'Gatto_nero', 'weapon': 'AK47'}]}, 5: {'team0_score': 4, 'team1_score': 1, 'rnd_start': '2023.03.27-18.11.24', 'event': [{'time': '0:00:17', 'killer': 'SirRobertsOBE', 'killed': '[VA] Hecktor 24', 'weapon': 'AR'}, {'time': '0:00:25', 'killer': 'X7XVendettaX7X', 'killed': 'STILBITE|youtube', 'weapon': 'AK47'}, {'time': '0:01:00', 'killer': 'Plex', 'killed': '[TL] Kungfusnail', 'weapon': 'AK47'}, {'time': '0:01:06', 'killer': 'Plex', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}]}, 6: {'team0_score': 4, 'team1_score': 2, 'rnd_start': '2023.03.27-18.12.50', 'event': [{'time': '0:00:24', 'killer': 'Sin_Of_Bob', 'killed': 'SirRobertsOBE', 'weapon': 'AK47'}, {'time': '0:00:34', 'killer': 'STILBITE|youtube', 'killed': 'X7XVendettaX7X', 'weapon': 'AK47'}, {'time': '0:00:41', 'killer': '[VA] Hecktor 24', 'killed': 'Plex', 'weapon': 'AK47'}, {'time': '0:00:59', 'killer': 'Gatto_nero', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}, {'time': '0:01:00', 'killer': '[VA] Hecktor 24', 'killed': 'Gatto_nero', 'weapon': 'AK47'}]}, 7: {'team0_score': 5, 'team1_score': 2, 'rnd_start': '2023.03.27-18.14.10', 'event': [{'time': '0:00:18', 'killer': 'SirRobertsOBE', 'killed': 'Sin_Of_Bob', 'weapon': 'AR'}, {'time': '0:00:37', 'killer': 'SirRobertsOBE', 'killed': '[TL] Kungfusnail', 'weapon': 'AR'}, {'time': '0:00:38', 'killer': 'STILBITE|youtube', 'killed': 'SirRobertsOBE', 'weapon': 'AR'}, {'time': '0:00:43', 'killer': 'X7XVendettaX7X', 'killed': 'STILBITE|youtube', 'weapon': 'AR'}, {'time': '0:01:25', 'killer': '[VA] Hecktor 24', 'killed': 'Plex', 'weapon': 'AK47'}, {'time': '0:02:06', 'killer': 'Gatto_nero', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}]}, 8: {'team0_score': 6, 'team1_score': 2, 'rnd_start': '2023.03.27-18.16.55', 'event': [{'time': '0:00:17', 'killer': 'Plex', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}, {'time': '0:00:27', 'killer': 'SirRobertsOBE', 'killed': 'Richard The Lionfart', 'weapon': 'AR'}, {'time': '0:00:36', 'killer': 'SirRobertsOBE', 'killed': 'Sin_Of_Bob', 'weapon': 'AR'}, {'time': '0:00:42', 'killer': 'STILBITE|youtube', 'killed': 'X7XVendettaX7X', 'weapon': 'AR'}, {'time': '0:01:01', 'killer': 'Gatto_nero', 'killed': 'STILBITE|youtube', 'weapon': 'AK47'}]}, 9: {'team0_score': 6, 'team1_score': 3, 'rnd_start': '2023.03.27-18.18.17', 'event': [{'time': '0:00:17', 'killer': 'SirRobertsOBE', 'killed': 'Richard The Lionfart', 'weapon': 'AR'}, {'time': '0:00:23', 'killer': '[VA] Hecktor 24', 'killed': 'SirRobertsOBE', 'weapon': 'akshorty'}, {'time': '0:00:25', 'killer': '[VA] Hecktor 24', 'killed': 'Wendel Horatio', 'weapon': 'akshorty'}, {'time': '0:00:51', 'killer': 'STILBITE|youtube', 'killed': 'X7XVendettaX7X', 'weapon': 'AK47'}, {'time': '0:01:00', 'killer': '[VA] Hecktor 24', 'killed': 'Gatto_nero', 'weapon': 'AR'}, {'time': '0:01:03', 'killer': 'Plex', 'killed': '[TL] Kungfusnail', 'weapon': 'AK47'}, {'time': '0:01:08', 'killer': 'Plex', 'killed': 'STILBITE|youtube', 'weapon': 'AK47'}, {'time': '0:01:11', 'killer': 'Plex', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}, {'time': '0:01:13', 'killer': 'Sin_Of_Bob', 'killed': 'Plex', 'weapon': 'akshorty'}]}, 10: {'team0_score': 7, 'team1_score': 3, 'rnd_start': '2023.03.27-18.19.51', 'event': [{'time': '0:00:19', 'killer': 'SirRobertsOBE', 'killed': '[VA] Hecktor 24', 'weapon': 'cet9'}, {'time': '0:00:23', 'killer': 'X7XVendettaX7X', 'killed': 'Sin_Of_Bob', 'weapon': 'm9'}, {'time': '0:00:49', 'killer': 'STILBITE|youtube', 'killed': "xj39 El'Diablo", 'weapon': 'sock'}, {'time': '0:00:55', 'killer': '[TL] Kungfusnail', 'killed': '[TL] Kungfusnail', 'weapon': 'grenade_ru'}, {'time': '0:01:05', 'killer': 'SirRobertsOBE', 'killed': 'STILBITE|youtube', 'weapon': 'cet9'}]}, 11: {'team0_score': 7, 'team1_score': 4, 'rnd_start': '2023.03.27-18.21.16', 'event': [{'time': '0:00:12', 'killer': 'Rfl', 'killed': 'Gatto_nero', 'weapon': 'AR'}, {'time': '0:00:19', 'killer': 'Rfl', 'killed': 'SirRobertsOBE', 'weapon': 'AR'}, {'time': '0:00:22', 'killer': 'Plex', 'killed': '[VA] Hecktor 24', 'weapon': 'SMG'}, {'time': '0:00:26', 'killer': 'STILBITE|youtube', 'killed': 'Plex', 'weapon': 'SMG'}, {'time': '0:00:28', 'killer': "xj39 El'Diablo", 'killed': 'STILBITE|youtube', 'weapon': 'AR'}, {'time': '0:00:52', 'killer': '[TL] Kungfusnail', 'killed': 'much.zen', 'weapon': 'SMG'}]}, 12: {'team0_score': 7, 'team1_score': 5, 'rnd_start': '2023.03.27-18.22.28', 'event': [{'time': '0:00:17', 'killer': 'Plex', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:23', 'killer': 'STILBITE|youtube', 'killed': 'Gatto_nero', 'weapon': 'AR'}, {'time': '0:00:30', 'killer': 'X7XVendettaX7X', 'killed': 'little witch', 'weapon': 'AK47'}, {'time': '0:00:31', 'killer': '[TL] Kungfusnail', 'killed': 'Plex', 'weapon': 'AR'}, {'time': '0:00:36', 'killer': '[VA] Hecktor 24', 'killed': 'SirRobertsOBE', 'weapon': 'AK47'}, {'time': '0:00:38', 'killer': 'Rfl', 'killed': 'X7XVendettaX7X', 'weapon': 'AR'}, {'time': '0:00:50', 'killer': '[VA] Hecktor 24', 'killed': "xj39 El'Diablo", 'weapon': 'AK47'}, {'time': '0:00:59', 'killer': 'Rfl', 'killed': 'much.zen', 'weapon': 'AR'}]}, 13: {'team0_score': 7, 'team1_score': 6, 'rnd_start': '2023.03.27-18.23.47', 'event': [{'time': '0:00:35', 'killer': '[TL] Kungfusnail', 'killed': 'Plex', 'weapon': 'AR'}, {'time': '0:00:37', 'killer': 'X7XVendettaX7X', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:41', 'killer': '[TL] Kungfusnail', 'killed': 'much.zen', 'weapon': 'AR'}, {'time': '0:00:44', 'killer': '[VA] Hecktor 24', 'killed': "xj39 El'Diablo", 'weapon': 'AK47'}, {'time': '0:00:50', 'killer': 'little witch', 'killed': 'SirRobertsOBE', 'weapon': 'AK47'}, {'time': '0:01:12', 'killer': 'Gatto_nero', 'killed': 'STILBITE|youtube', 'weapon': 'AK47'}, {'time': '0:01:20', 'killer': 'Gatto_nero', 'killed': '[TL] Kungfusnail', 'weapon': 'AK47'}]}, 14: {'team0_score': 7, 'team1_score': 7, 'rnd_start': '2023.03.27-18.25.56', 'event': [{'time': '0:00:23', 'killer': 'Gatto_nero', 'killed': 'little witch', 'weapon': 'AK47'}, {'time': '0:00:28', 'killer': 'Gatto_nero', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:29', 'killer': '[VA] Hecktor 24', 'killed': 'X7XVendettaX7X', 'weapon': 'AK47'}, {'time': '0:00:31', 'killer': 'SirRobertsOBE', 'killed': '[VA] Hecktor 24', 'weapon': 'AR'}, {'time': '0:00:34', 'killer': 'STILBITE|youtube', 'killed': 'much.zen', 'weapon': 'AR'}, {'time': '0:00:35', 'killer': "xj39 El'Diablo", 'killed': 'STILBITE|youtube', 'weapon': 'AK47'}, {'time': '0:01:08', 'killer': 'Plex', 'killed': '[TL] Kungfusnail', 'weapon': 'AK47'}, {'time': '0:01:51', 'killer': 'Rfl', 'killed': "xj39 El'Diablo", 'weapon': 'AR'}, {'time': '0:01:54', 'killer': 'Rfl', 'killed': 'Plex', 'weapon': 'AR'}]}, 15: {'team0_score': 7, 'team1_score': 8, 'rnd_start': '2023.03.27-18.28.16', 'event': [{'time': '0:00:15', 'killer': 'Gatto_nero', 'killed': 'Rfl', 'weapon': 'AK47'}, {'time': '0:00:23', 'killer': 'Gatto_nero', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:29', 'killer': 'SirRobertsOBE', 'killed': '[VA] Hecktor 24', 'weapon': 'AR'}, {'time': '0:00:31', 'killer': 'Gatto_nero', 'killed': 'little witch', 'weapon': 'AK47'}, {'time': '0:00:34', 'killer': '[TL] Kungfusnail', 'killed': 'Gatto_nero', 'weapon': 'AR'}, {'time': '0:00:49', 'killer': 'STILBITE|youtube', 'killed': "xj39 El'Diablo", 'weapon': 'AK47'}, {'time': '0:01:01', 'killer': 'STILBITE|youtube', 'killed': 'Plex', 'weapon': 'AK47'}, {'time': '0:01:05', 'killer': '[TL] Kungfusnail', 'killed': 'much.zen', 'weapon': 'AR'}, {'time': '0:01:30', 'killer': '[TL] Kungfusnail', 'killed': 'X7XVendettaX7X', 'weapon': 'AR'}]}, 16: {'team0_score': 8, 'team1_score': 8, 'rnd_start': '2023.03.27-18.30.07', 'event': [{'time': '0:00:24', 'killer': '[VA] Hecktor 24', 'killed': 'X7XVendettaX7X', 'weapon': 'AR'}, {'time': '0:00:26', 'killer': 'Gatto_nero', 'killed': '[VA] Hecktor 24', 'weapon': 'AK47'}, {'time': '0:00:32', 'killer': 'Plex', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:33', 'killer': 'Rfl', 'killed': 'Plex', 'weapon': 'AR'}, {'time': '0:00:34', 'killer': 'STILBITE|youtube', 'killed': 'Gatto_nero', 'weapon': 'AK47'}, {'time': '0:00:48', 'killer': 'SirRobertsOBE', 'killed': '[TL] Kungfusnail', 'weapon': 'AR'}, {'time': '0:00:57', 'killer': 'STILBITE|youtube', 'killed': 'much.zen', 'weapon': 'AK47'}, {'time': '0:01:14', 'killer': "xj39 El'Diablo", 'killed': 'little witch', 'weapon': 'm16'}, {'time': '0:01:24', 'killer': 'STILBITE|youtube', 'killed': "xj39 El'Diablo", 'weapon': 'AK47'}, {'time': '0:02:04', 'killer': 'SirRobertsOBE', 'killed': 'STILBITE|youtube', 'weapon': 'AR'}, {'time': '0:02:05', 'killer': 'SirRobertsOBE', 'killed': 'Rfl', 'weapon': 'AR'}]}, 17: {'team0_score': 8, 'team1_score': 9, 'rnd_start': '2023.03.27-18.32.33', 'event': [{'time': '0:00:17', 'killer': 'Plex', 'killed': '[VA] Hecktor 24', 'weapon': 'AR'}, {'time': '0:00:21', 'killer': 'Gatto_nero', 'killed': 'little witch', 'weapon': 'AK47'}, {'time': '0:00:22', 'killer': 'Rfl', 'killed': 'Gatto_nero', 'weapon': 'AR'}, {'time': '0:00:26', 'killer': 'STILBITE|youtube', 'killed': 'Plex', 'weapon': 'AR'}, {'time': '0:00:29', 'killer': 'STILBITE|youtube', 'killed': "xj39 El'Diablo", 'weapon': 'AR'}, {'time': '0:00:33', 'killer': '[TL] Kungfusnail', 'killed': 'much.zen', 'weapon': 'ak12'}, {'time': '0:00:51', 'killer': 'Rfl', 'killed': 'SirRobertsOBE', 'weapon': 'AR'}]}, 18: {'team0_score': 8, 'team1_score': 10, 'rnd_start': '2023.03.27-18.33.44', 'event': [{'time': '0:00:22', 'killer': 'Gatto_nero', 'killed': 'Sin_Of_Bob', 'weapon': 'AK47'}, {'time': '0:00:29', 'killer': '[TL] Kungfusnail', 'killed': 'X7XVendettaX7X', 'weapon': 'ak12'}, {'time': '0:00:30', 'killer': '[TL] Kungfusnail', 'killed': 'SirRobertsOBE', 'weapon': 'ak12'}, {'time': '0:00:31', 'killer': 'STILBITE|youtube', 'killed': 'Gatto_nero', 'weapon': 'AR'}, {'time': '0:00:34', 'killer': 'Rfl', 'killed': 'Plex', 'weapon': 'AR'}, {'time': '0:01:33', 'killer': '[VA] Hecktor 24', 'killed': 'much.zen', 'weapon': 'AR'}]}}
    round_data = get_dict_rnd(Timestamp, start_match_time[0][0], server)

    conn.close()
    return render_template('match.html', match=match, round_data=round_data, start_match_time=start_match_time,
                           players=players,
                           range_m=range_m, len=len, max=max, max_players_count=max_players_count,
                           players_team0=players_team0, players_team1=players_team1, kills=kills)


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


@app.route('/data_in', methods=['PUT'])
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
            save_Bomb_data_to_db(json.loads(data), data_server_name)
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
    # rounds = [{'Round': row[5], 'WinningTeam': row[6]} for row in cur.fetchall()]
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
