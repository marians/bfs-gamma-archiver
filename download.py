# encoding: utf8

"""
Download current gamma radiation data
from Bundesamt für Strahlenschutz (bfs)
and write to MySQLdb
"""

import requests
import MySQLdb
import sys
import argparse
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from contextlib import closing
import csv
import config

files_1h = [
    '1h_00.dat',
    '1h_01.dat',
    '1h_02.dat',
    '1h_03.dat',
    '1h_04.dat',
    '1h_05.dat',
    '1h_06.dat',
    '1h_07.dat',
    '1h_08.dat',
    '1h_09.dat',
    '1h_10.dat',
    '1h_11.dat',
    '1h_12.dat',
    '1h_13.dat',
    '1h_14.dat',
    '1h_15.dat',
    '1h_16.dat',
    '1h_17.dat',
    '1h_18.dat',
    '1h_19.dat',
    '1h_20.dat',
    '1h_21.dat',
    '1h_22.dat',
    '1h_23.dat'
]


def write_values_to_db(data):
    sql = '''INSERT IGNORE INTO values_1h
        (station_id, datetime_utc, dose, status)
        VALUES
        ("%s", "%s", "%s", "%s")''' % (data[0], data[1], data[2], data[3])
    try:
        cursor.execute(sql)
        return cursor.rowcount
    except MySQLdb.Error, e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))


def write_stations_to_db(data):
    sql = '''INSERT IGNORE INTO stations (id, postalcode, name, longitude, latitude)
        VALUES ("%s", "%s", "%s", "%s", "%s")''' % (data[0], data[1], data[2], data[3], data[4])
    try:
        cursor.execute(sql)
        return cursor.rowcount
    except MySQLdb.Error, e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))


def get_mysql_rows(sql):
    """Execute MySQL query and return result rows"""
    try:
        cursor.execute(sql)
        while (1):
            row = cursor.fetchone()
            if row == None:
                break
            yield row
    except MySQLdb.Error, e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))


def execute(sql):
    try:
        cursor.execute(sql)
    except MySQLdb.Error, e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))


def get_stations():
    rows = get_mysql_rows('SELECT id FROM stations')
    stations = {}
    for row in rows:
        stations[row['id']] = True
    return stations


def init_db():
    """Initialize database (create tables)"""
    # fetch existing tables
    tables = []
    for item in get_mysql_rows('SHOW TABLES'):
        for v in item.values():
            tables.append(v)
    # stations table
    if "stations" not in tables:
        if args.verbose:
            print("Creating station table 'stations'")
        sql = ("""CREATE TABLE `stations` (
          `id` varchar(9) NOT NULL DEFAULT '',
          `postalcode` varchar(5) NOT NULL DEFAULT '',
          `name` varchar(255) NOT NULL DEFAULT '',
          `longitude` decimal(5,2) NOT NULL,
          `latitude` decimal(5,2) NOT NULL,
          `pachube_feed_id` int(10) unsigned DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=%s DEFAULT CHARSET=utf8 COMMENT='BFS stations'""" %
        config.DB_ENGINE)
        execute(sql)
    # values table
    if "values_1h" not in tables:
        if args.verbose:
            print("Creating value table 'values_1h'")
        sql = ("""CREATE TABLE `values_1h` (
          `station_id` varchar(9) NOT NULL,
          `datetime_utc` datetime NOT NULL,
          `dose` decimal(6,3) NOT NULL,
          `status` tinyint(4) NOT NULL,
          UNIQUE KEY `uniq` (`station_id`,`datetime_utc`),
          KEY `station_id` (`station_id`),
          KEY `datetime_utc` (`datetime_utc`),
          KEY `status` (`status`)
        ) ENGINE=%s DEFAULT CHARSET=utf8 COMMENT='1 hour values'""" %
        config.DB_ENGINE)
        execute(sql)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Loading bfs gamma radiatio data into database')
    parser.add_argument('-v', action='store_true', dest='verbose',
                       help='Enable verbose output')
    args = parser.parse_args()
    try:
        conn = MySQLdb.connect(host=config.DB_HOST,
                               user=config.DB_USER,
                               passwd=config.DB_PASS,
                               db=config.DB_NAME)
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    except MySQLdb.Error, e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        if int(e.args[0]) == 1049:
            sys.stderr.write("Please create a database '%s' first.\n" %
                config.DB_NAME)
            if config.DB_USER != "root":
                sys.stderr.write("Don't forget to make it writeable for user '%s'.\n" %
                    config.DB_USER)
        sys.exit(1)

    init_db()

    #sys.exit()

    stations_cache = get_stations()

    written_records = 0

    # download CSV and write to database
    for filename in files_1h:
        num_written_values = 0
        num_written_stations = 0
        url = config.BFS_URL + filename
        if args.verbose:
            print("Fetching file %s via URL %s" % (filename, url))
        req = requests.get(url, auth=(config.BFS_USER, config.BFS_PASS))
        req.encoding = "ISO-8859-1"
        text = req.text.encode("utf8")
        reader = csv.reader(StringIO(text), delimiter="|")
        for row in reader:
            (idnum, plz, city, longitude, latitude, datestring, dose, status) = row
            num_written_values += write_values_to_db([idnum, datestring, dose, status])
            # add station if not there
            if idnum not in stations_cache:
                num_written_stations += write_stations_to_db([idnum, plz, city, longitude, latitude])
                stations_cache[idnum] = True
        written_records += num_written_values + num_written_stations

    if args.verbose:
        if written_records == 0:
            print("No (new) records have been stored.")
        else:
            print("%d records have been stored." % written_records)
