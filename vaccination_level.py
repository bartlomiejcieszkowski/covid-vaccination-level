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

from datetime import datetime, date, timedelta

import math

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


def timestamp_to_utcdatetime(timestamp):
    return datetime.utcfromtimestamp(timestamp)

def nice_date(timestamp: int):
    return timestamp_to_utcdatetime(timestamp).strftime('%Y/%m/%d')


class VoivodeshipVaccineData:
    def __init__(self, timestamp: int, voivodeship: str, population: int, full_vaccinated_amount: int):
        self.timestamp = timestamp
        self.voivodeship = voivodeship
        self.population = population
        self.full_vaccinated_amount = full_vaccinated_amount
        self.full_vaccinated_percent = self.full_vaccinated_amount / self.population

    def update(self, population: int, full_vaccinated_amount: int):
        self.population += population
        self.full_vaccinated_amount += full_vaccinated_amount
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


class PlotData:
    def __init__(self):
        self.name = ""
        self.x = []
        self.y = []


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
                voivodeships[v].update(entry['population'], entry['full_vaccinated_amount'])
            else:
                voivodeships[v] = VoivodeshipVaccineData(timestamp, v, entry['population'], entry['full_vaccinated_amount'])
            communities.append(CommunityVaccineData(entry))

        create_db()
        if hash_exists(hash_md5):
            print(f'{timestamp} - nothing to be done - data already in db')
            return -1
        else:
            update_voivodeships(timestamp, voivodeships, hash_md5)
            update_communities(timestamp, communities)
    return 0

def update(args):
    if args.continuous is False:
        return update_db()
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

headers_table = [
    'WOJEWODZTWO'
]

from pathlib import Path

herd_immunity = 70.0

def when_herd_immunity(start, end):
    delta_percent = (end.full_vaccinated_percent * 100) - (start.full_vaccinated_percent * 100)
    end_date = timestamp_to_utcdatetime(end.timestamp).date()
    start_date = timestamp_to_utcdatetime(start.timestamp).date()
    delta_days = (end_date - start_date).days

    daily_increase_average = delta_percent / delta_days
    #print(f'{end.voivodeship} start: {start_date}, end: {end_date}, days: {delta_days} delta_percent: {delta_percent}% daily_incr: {daily_increase_average}%')

    percent_required = herd_immunity - (end.full_vaccinated_percent * 100)
    days_to_herd_immunity = int(math.ceil(percent_required / daily_increase_average))
    herd_immunity_date = timestamp_to_utcdatetime(end.timestamp).date() + timedelta(days=days_to_herd_immunity)
    return daily_increase_average, herd_immunity_date, days_to_herd_immunity


def stats(args):
    plot_dates = []
    plot_data = []

    output=sys.stdout
    if args.output:
        path = Path(args.output)
        if path.exists() == False or path.is_file():
            output = path.open('w')

    timestamps = get_timestamps()
    voivodeships = get_voivodeships()
    v_len = len(max(voivodeships, key=len))
    d_len = len(nice_date(0))
    v_string = '{:' + str(v_len) + 's} | '
    t_string = '{:>' + str(d_len) + 's} | '
    herd_string = '{:.4f}%/dzien | {:>3s} dni | {:>' + str(d_len) + 's}'

    herd_immunity_lines = []
    stats_lines = []
    desc_lines = []

    # create table header
    header = v_string.format(headers_table[0])
    desc_lines.append(header)
    desc_len = len(header)
    header = ''
    skip_idx = []

    for idx in range(0, len(timestamps)):
        timestamp = timestamps[idx]
        new_date = timestamp_to_utcdatetime(timestamp).date()
        if len(plot_dates) > 0 and plot_dates[-1] == new_date:
            skip_idx.append(idx)
            continue
        nice_timestamp = nice_date(timestamp)
        header += t_string.format(nice_timestamp)
        plot_dates.append(new_date)

    stats_len = len(header)
    stats_lines.append(header)
    header = '     KIEDY ODPORNOSC STADNA {:.0f}%     '.format(herd_immunity)
    herd_immunity_len = len(header)
    herd_immunity_lines.append(header)
    header = ''
    #print(header, file=output)
    desc_line_separator = '-' * desc_len
    stats_line_separator = '-' * stats_len
    herd_immunity_line_separator = '-' * herd_immunity_len

    desc_lines.append(desc_line_separator)
    stats_lines.append(stats_line_separator)
    herd_immunity_lines.append('-' * herd_immunity_len)

    # here it is assumed that no new voivodeships will be created ;), and always all will have data

    off = 2 # header + separator

    if len(voivodeships) > 0:
        plot_entry = PlotData()
        plot_entry.name = voivodeships[0]
        plot_entry.x = plot_dates

        master_data = get_voivodeship_data(voivodeships[0])
        out = v_string.format(voivodeships[0])

        desc_lines.append(out)
        herd_immunity_lines.append('')
        stats_lines.append('')

        last_idx = 0
        for idx in range(0, len(master_data)):
            if idx in skip_idx:
                continue
            last_idx = idx
            stats_lines[0+off] += t_string.format(master_data[idx].percent_string())
            plot_entry.y.append(master_data[idx].full_vaccinated_percent)
        daily_increase_average, herd_immunity_date, days_to_herd_immunity = when_herd_immunity(master_data[0], master_data[last_idx])
        herd_immunity_lines[0+off] += herd_string.format(daily_increase_average, str(days_to_herd_immunity), herd_immunity_date.strftime('%Y/%m/%d'))
        #print(out, file=output)

        plot_data.append(plot_entry)

        for i in range(1, len(voivodeships)):
            plot_entry = PlotData()
            plot_entry.name = voivodeships[i]
            plot_entry.x = plot_dates

            data = get_voivodeship_data(voivodeships[i])
            out = v_string.format(voivodeships[i])

            desc_lines.append(out)
            herd_immunity_lines.append('')
            stats_lines.append('')

            last_idx = 0
            for j in range(0, len(data)):
                if j in skip_idx:
                    continue
                last_idx = j
                stats_lines[i+off] += t_string.format(data[j].percent_string())
                plot_entry.y.append(data[j].full_vaccinated_percent)
                master_data[j].update(data[j].population, data[j].full_vaccinated_amount)
            daily_increase_average, herd_immunity_date, days_to_herd_immunity = when_herd_immunity(data[0], data[last_idx])
            herd_immunity_lines[i+off] += herd_string.format(daily_increase_average, str(days_to_herd_immunity), herd_immunity_date.strftime('%Y/%m/%d'))
            #print(out, file=output)
            plot_data.append(plot_entry)

    desc_lines.append(desc_line_separator)
    stats_lines.append(stats_line_separator)
    herd_immunity_lines.append(herd_immunity_line_separator)
    #print(line_separator, file=output)

    desc_lines.append(v_string.format('POLSKA'))
    stats_lines.append('')
    herd_immunity_lines.append('')

    plot_entry = PlotData()
    plot_entry.name = 'POLSKA'
    plot_entry.x = plot_dates

    last_idx = 0
    for idx in range(0, len(master_data)):
        if idx in skip_idx:
            continue
        last_idx = idx
        stats_lines[-1] += t_string.format(master_data[idx].percent_string())
        plot_entry.y.append(master_data[idx].full_vaccinated_percent)
    daily_increase_average, herd_immunity_date, days_to_herd_immunity = when_herd_immunity(master_data[0], master_data[last_idx])
    herd_immunity_lines[-1] += herd_string.format(daily_increase_average, str(days_to_herd_immunity), herd_immunity_date.strftime('%Y/%m/%d'))

    plot_data.insert(0, plot_entry)

    #print(out, file=output)

    print('# Opracowanie na podstawie danych z https://www.gov.pl/web/szczepienia-gmin', file=output)
    print('', file=output)
    chart_list = generate_chart('level', f'({plot_dates[-1]}) Procent zaszczepionych w Polsce', plot_data)
    for chart in chart_list:
        print(chart, file=output)
    if args.md:
        print('```', file=output)

    for i in range(0, len(herd_immunity_lines)):
        print(f"{desc_lines[i]}{herd_immunity_lines[i]}", file=output)

    if args.md:
        print('```', file=output)
    print('', file=output)

    if args.md:
        print('```', file=output)

    for i in range(0, len(stats_lines)):
        print(f"{desc_lines[i]}{stats_lines[i]}", file=output)

    if args.md:
        print('```', file=output)





    if output != sys.stdout:
        output.close()
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-v', '--verbose', action='count', help='verbose output')
    sub = ap.add_subparsers()
    update_ap = sub.add_parser('update', help='updates db')
    update_ap.add_argument('-c', '--continuous', action='store_true', help='runs update periodically - ctrl-c to stop')
    update_ap.set_defaults(func=update)
    stats_ap = sub.add_parser('stats', help='prints stats')
    stats_ap.add_argument('-m', '--md', action='store_true', help='adds md headers')
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


import plotly.graph_objects as go
from lxml import etree

def generate_chart(filename: str, decription: str, charts_data: PlotData):
    output = []

    chart_dir_name = "charts"
    chart_dir_path = Path(chart_dir_name)
    try:
        chart_dir_path.mkdir(parents=True, exist_ok=True)
    except FileExistsError as ex:
        print(f'{ex}', file=sys.stderr)
        return output

    line_styles = [
        None,
        'dot',
        'dash',
        'dashdot'
    ]
    color_count = 10


    fig = go.Figure()
    fig.update_yaxes(tickformat="%")
    # https://plotly.com/python/reference/#layout-xaxis-nticks
    # If the axis `type` is "date", then you must convert the time to milliseconds. For example, to set the interval between ticks to one day, set `dtick` to 86400000.0
    fig.update_xaxes(
        tickformat='%Y-%m-%d',
        tickmode='auto',
        dtick=86400000)
    fig.update_layout(width=1500, height=1000, title=decription)

    idx = 0
    for chart in charts_data:
        fig.add_trace(go.Scatter(x=chart.x, y=chart.y, mode='lines+markers', name=chart.name, line=dict(dash=line_styles[idx // color_count])))
        idx += 1

    fig_path = chart_dir_path / f'{filename}.svg'


    fig.write_image(fig_path)

    # plotly and kaleido when regenerating svg insert different id
    # pretty print xml, so the diff looks reasonable
    et = etree.parse(str(fig_path))
    et.write(str(fig_path), pretty_print=True)

    output.append('')
    output.append(f'![{decription}]({chart_dir_name}/{filename}.svg)')
    output.append('')

    return output


if __name__ == "__main__":
    sys.exit(main())
