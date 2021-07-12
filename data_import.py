#!/usr/bin/env python

# 1. Get request from developer tools in chrome based browser
# 2. https://curl.trillworks.com/
import sys

import requests
import sqlite3
import time
import hashlib

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

    return response.json(), timestamp, hash_md5.hexdigest()


class VoivodeshipVaccineData:
    def __init__(self, json_entry):
        self.voivodeship = json_entry['voivodeship'].translate(repl)
        self.population = 0
        self.full_vaccinated_amount = 0
        self.full_vaccinated_percent = 0
        self.update(json_entry)

    def update(self, json_entry):
        self.population += json_entry['population']
        self.full_vaccinated_amount += json_entry['full_vaccinated_amount']
        self.full_vaccinated_percent = self.full_vaccinated_amount / self.population


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


def main():
    json_resp, timestamp, hash_md5 = get_json()
    if json_resp is None:
        return -1

    voivodeships = {}
    communities = []
    for entry in json_resp:
        if entry['voivodeship'] in voivodeships:
            voivodeships[entry['voivodeship']].update(entry)
        else:
            voivodeships[entry['voivodeship']] = VoivodeshipVaccineData(entry)
        communities.append(CommunityVaccineData(entry))

    create_db()
    if hash_exists(hash_md5):
        print(f'{timestamp} - nothing to be done - data already in db')
        return 0
    update_voivodeships(timestamp, voivodeships, hash_md5)
    update_communities(timestamp, communities)

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

def update_communities(timestamp, communities):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print(f'{timestamp} -> {int(timestamp)}')
    timestamp = int(timestamp)

    # Update Communities table - should be done once only

    for v in communities:
        p = (v.county,v.community,v.voivodeship,v.community_type,v.teryt)
        cursor.execute('REPLACE INTO Communities_info (county,community, voivodeship,community_type,teryt) VALUES (?,?,?,?,?)', p)
    conn.commit()

    for v in communities:
        cursor.execute("SELECT id FROM Communities_info WHERE teryt=:TERYT", {'TERYT': v.teryt})
        result = cursor.fetchone()
        print(f'{result} - {v.voivodeship} {v.county} {v.community}')
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

    print(f'{timestamp} -> {int(timestamp)}')
    timestamp = int(timestamp)
    p = (timestamp,hash_md5)

    cursor.execute('REPLACE INTO Timestamps (time,hash_md5) VALUES (?,?)', p)
    for key, v in voivodeships.items():
        print(f'{v.voivodeship}: {v.full_vaccinated_amount}/{v.population} = {v.full_vaccinated_percent * 100}%')
        p = (timestamp, v.voivodeship, v.population, v.full_vaccinated_amount)
        cursor.execute('REPLACE INTO Voivodeships (time,voivodeship,population,full_vaccinated_amount) VALUES (?,?,?,?)', p)

    conn.commit()
    print(f'{timestamp} - counties - insert done')
    conn.close()


if __name__ == "__main__":
    sys.exit(main())
