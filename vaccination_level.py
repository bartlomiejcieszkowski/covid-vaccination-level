#!/usr/bin/env python

# 1. Get request from developer tools in chrome based browser
# 2. https://curl.trillworks.com/
import sys

import requests
import sqlite3
import time
import hashlib

import signal
import argparse

from datetime import datetime

headers = {
    'sec-ch-ua': '" Not;A Brand";v="99", "Microsoft Edge";v="91", "Chromium";v="91"',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Referer': 'https://www.gov.pl/web/szczepienia-gmin',
    'DNT': '1',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36 Edg/91.0.864.53',
}

params = (
    ('segment', 'A,B,C'),
)
debug_logs = True
db_name = 'vaccination_level.db'

repl = str.maketrans(
    "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ",
    "acelnoszzACELNOSZZ"
)

def get_json():
    timestamp = time.time()
    response = requests.get('https://www.gov.pl/api/data/covid-vaccination-contest/results-details', headers=headers, params=params)

    if response.ok is False:
        return None, None, None

    hash_md5 = hashlib.md5(response.text.encode())

    return response.json(), int(timestamp), hash_md5.hexdigest()


def nice_date(timestamp: int):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y/%m/%d')


class VoivodeshipVaccineData:
    def __init__(self, timestamp: int, voivodeship: str, population: int, full_vaccinated_amount: int):
        self.timestamp = timestamp
        self.voivodeship = voivodeship
        self.population = population
        self.full_vaccinated_amount = full_vaccinated_amount
        self.full_vaccinated_percent = self.full_vaccinated_amount / self.population

    def update(self, json_entry):
        self.population += json_entry['population']
        self.full_vaccinated_amount += json_entry['full_vaccinated_amount']
        self.full_vaccinated_percent = self.full_vaccinated_amount / self.population

    def percent_string(self):
        return '{:.4f}%'.format(self.full_vaccinated_percent * 100)


class CommunityVaccineData:
    def __init__(self, json_entry):
        self.voivodeship = json_entry['voivodeship'].translate(repl)
        self.county = json_entry['county'].translate(repl)
        self.community = json_entry['community'].translate(repl)
        self.community_type = json_entry['community_type']
        self.teryt = json_entry['teryt_code']
        self.population = 0
        self.full_vaccinated_amount = 0
        self.full_vaccinated_percent = 0
        self.update(json_entry)

    def update(self, json_entry):
        self.population += json_entry['population']
        self.full_vaccinated_amount += json_entry['full_vaccinated_amount']
        self.full_vaccinated_percent = self.full_vaccinated_amount / self.population


run = True


def signal_handler(sig, frame):
    print('Ctrl-C caught - closing')
    global run
    run = False


def update_db():
    json_resp, timestamp, hash_md5 = get_json()
    if json_resp:
        voivodeships = {}
        communities = []
        for entry in json_resp:
            v = entry['voivodeship'].translate(repl)
            if v in voivodeships:
                voivodeships[v].update(entry)
            else:
                voivodeships[v] = VoivodeshipVaccineData(timestamp, v, entry['population'], entry['full_vaccinated_amount'])
            communities.append(CommunityVaccineData(entry))

        create_db()
        if hash_exists(hash_md5):
            print(f'{timestamp} - nothing to be done - data already in db')
        else:
            update_voivodeships(timestamp, voivodeships, hash_md5)
            update_communities(timestamp, communities)


def update(args):
    if args.continuous is False:
        update_db()
    else:
        signal.signal(signal.SIGINT, signal_handler)
        while run:
            update_db()
            print('sleep')
            for i in range(0, 120):
                if run is False:
                    break
                # print(f'sleep {i+1}/120')
                time.sleep(30)

    print('bye')
    return 0

headers = [
    'WOJEWODZTWO'
]


def stats(args):
    output=sys.stdout
    print('```', file=output)
    timestamps = get_timestamps()
    voivodeships = get_voivodeships()
    v_len = len(max(voivodeships, key=len))
    d_len = len(nice_date(0))
    v_string = '{:' + str(v_len) + 's} '
    t_string = '{:>' + str(d_len) + 's} '

    # create table header
    header = v_string.format(headers[0])
    for timestamp in timestamps:
        header += t_string.format(nice_date(timestamp))
    print(header, file=output)

    # here it is assumed that no new voivodeships will be created ;), and always all will have data

    for voivodeship in voivodeships:
        data = get_voivodeship_data(voivodeship)
        out = v_string.format(voivodeship)
        for v in data:
            out += t_string.format(v.percent_string())
        print(out, file=output)
    print('```', file=output)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-v', '--verbose', action='count', help='verbose output')
    sub = ap.add_subparsers()
    update_ap = sub.add_parser('update', help='updates db')
    update_ap.add_argument('-c', '--continuous', action='store_true', help='runs update periodically - ctrl-c to stop')
    update_ap.set_defaults(func=update)
    stats_ap = sub.add_parser('stats', help='prints stats')
    stats_ap.add_argument('-o', '--output', type=str, default=None, help='output stats to a file')
    stats_ap.set_defaults(func=stats)

    args = ap.parse_args()
    return args.func(args)


def create_db():
    conn = sqlite3.connect(db_name)
    conn.execute('''CREATE TABLE IF NOT EXISTS Voivodeships
                (time INTEGER,
                voivodeship TEXT,
                population INTEGER DEFAULT 0,
                full_vaccinated_amount INTEGER DEFAULT 0
                );''')
    conn.execute('''CREATE TABLE IF NOT EXISTS Timestamps
                    (time INTEGER PRIMARY KEY ASC,
                    hash_md5 TEXT);''')

    conn.execute('''CREATE TABLE IF NOT EXISTS Communities_info
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                county TEXT,
                community TEXT,
                voivodeship TEXT,
                community_type INTEGER,
                teryt TEXT
                );''')
    conn.execute('''CREATE TABLE IF NOT EXISTS Communities
                (time INTEGER,
                id INTEGER,
                population INTEGER,
                full_vaccinated_amount INTEGER,
                PRIMARY KEY (time, id));''')
    conn.commit()
    conn.close()


def update_communities(timestamp: int, communities):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print(f'{timestamp}')

    # Update Communities table - should be done once only

    for v in communities:
        p = (v.county,v.community,v.voivodeship,v.community_type,v.teryt)
        cursor.execute('REPLACE INTO Communities_info (county,community, voivodeship,community_type,teryt) VALUES (?,?,?,?,?)', p)
    conn.commit()

    for v in communities:
        cursor.execute("SELECT id FROM Communities_info WHERE teryt=:TERYT", {'TERYT': v.teryt})
        result = cursor.fetchone()
        #print(f'{result} - {v.voivodeship} {v.county} {v.community}')
        p = (timestamp, result[0], v.population, v.full_vaccinated_amount)
        cursor.execute("INSERT INTO Communities (time,id,population,full_vaccinated_amount) VALUES (?,?,?,?)", p)

    conn.commit()
    print(f'{timestamp} - counties - insert done')
    conn.close()


def hash_exists(hash_md5):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT time FROM Timestamps WHERE hash_md5=:NAME", {'NAME': hash_md5})
    result = cursor.fetchone()
    if result is None:
        conn.close()
        return False

    print(f'{hash_md5} - exists with timestamp {result[0]}')
    conn.close()
    return True


def update_voivodeships(timestamp, voivodeships, hash_md5):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    p = (timestamp,hash_md5)

    cursor.execute('REPLACE INTO Timestamps (time,hash_md5) VALUES (?,?)', p)
    for key, v in voivodeships.items():
        print(f'{v.voivodeship}: {v.full_vaccinated_amount}/{v.population} = {v.full_vaccinated_percent * 100}%')
        p = (timestamp, v.voivodeship, v.population, v.full_vaccinated_amount)
        cursor.execute('REPLACE INTO Voivodeships (time,voivodeship,population,full_vaccinated_amount) VALUES (?,?,?,?)', p)

    conn.commit()
    print(f'{timestamp} - counties - insert done')
    conn.close()


def get_voivodeships():
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT voivodeship FROM Voivodeships ORDER BY voivodeship')
    results = cursor.fetchall()
    out = list(map(lambda x: x[0], results))
    conn.close()
    return out


def get_timestamps():
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT time FROM Timestamps ORDER BY time ASC')
    results = cursor.fetchall()
    out = list(map(lambda x: x[0], results))
    conn.close()
    return out


def get_voivodeship_data(voivodeship: str):
    out = []
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('SELECT time,voivodeship,population,full_vaccinated_amount FROM Voivodeships WHERE voivodeship=:NAME ORDER BY time ASC', {'NAME': voivodeship})
    entry = cursor.fetchone()
    while entry:
        out.append(VoivodeshipVaccineData(timestamp=entry[0], voivodeship=entry[1], population=entry[2], full_vaccinated_amount=entry[3]))
        entry = cursor.fetchone()
    conn.close()
    return out


if __name__ == "__main__":
    sys.exit(main())