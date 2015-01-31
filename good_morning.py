# Copyright (c) 2015 Peter Cerno
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
"""Module for downloading financial data from financials.morningstar.com.
"""

import csv
import json
import numpy as np
import pandas as pd
import re
import urllib2
from bs4 import BeautifulSoup
from datetime import date

class KeyRatiosDownloader(object):
    """Downloads key ratios from http://financials.morningstar.com/
    """
    def __init__(self, table_prefix = 'morningstar_'):
        """Constructs the KeyRatiosDownloader instance.
        table_prefix: Prefix of the MySQL tables.
        """
        self._table_prefix = table_prefix

    def download(self, ticker, conn = None):
        """Downloads and returns an array of pandas.DataFrames containing
        the key ratios for the given Morningstar ticker. If the MySQL
        connection is specified then the downloaded key ratios are uploaded
        to the MySQL database.
        ticker: Morningstar ticker.
        conn: MySQLdb connection.
        """
        url = r"http://financials.morningstar.com/ajax/exportKR2CSV.html?" +\
            r"&callback=?&t=" + ticker + r"&region=usa&culture=en-US&cur=USD"
        response = urllib2.urlopen(url)
        tables = self._parse_tables(response)
        response_structure = [
            # Original Name, New pandas.DataFrame Name
            ('Financials', 'Key Financials'),
            ('Key Ratios -> Profitability', 'Key Margins % of Sales'),
            ('Key Ratios -> Profitability', 'Key Profitability'),
            ('Key Ratios -> Growth', None),
            ('Revenue %', 'Key Revenue %'),
            ('Operating Income %', 'Key Operating Income %'),
            ('Net Income %', 'Key Net Income %'),
            ('EPS %', 'Key EPS %'),
            ('Key Ratios -> Cash Flow', 'Key Cash Flow Ratios'),
            ('Key Ratios -> Financial Health',
                'Key Balance Sheet Items (in %)'),
            ('Key Ratios -> Financial Health',
                'Key Liquidity/Financial Health'),
            ('Key Ratios -> Efficiency Ratios', 'Key Efficiency Ratios')]
        frames = self._parse_frames(tables, response_structure)
        currency = re.match('^.* ([A-Z]+) Mil$', frames[0].index[0]).group(1)
        frames[0].index.name += ' ' + currency
        if conn: self._upload_frames_to_db(ticker, frames, conn)
        return frames

    def _parse_tables(self, response):
        """Parses the given csv response from financials.morningstar.com.
        Returns an array of pairs, where the first item is the name of the
        table (extracted from the response) and the second item is the
        corresponding pandas.DataFrame table containing the data.
        response: Csv response from financials.morningstar.com.
        """
        # Regex pattern used to recognize csv lines containing financial data.
        num_commas = 5
        pat_commas = r'(.*,){%d,}' % num_commas
        # Resulting array of pairs (table_name, table_frame).
        tables = []
        table_name = None
        table_rows = None
        for line in response:
            line = line.strip()
            match = re.match(pat_commas, line)
            if match:
                for row in csv.reader([line]):
                    table_rows.append(row)
            else:
                if table_name and table_rows:
                    tables.append([table_name, pd.DataFrame(table_rows)])
                if line != '':
                    table_name = line
                table_rows = []
        if table_name and table_rows:
            tables.append([table_name, pd.DataFrame(table_rows)])
        return tables

    def _parse_frames(self, tables, response_structure):
        """Returns an array of processed pandas.DataFrames based on the
        original array of tables and the special response_structure array.
        tables: Original array of tables (obtained from the _parse_tables).
        response_structure: Array of pairs (expected table name, new name
            assigned to the corresponding (processed) pandas.DataFrame).
        """
        period_start = tables[0][1].ix[0][1]
        period_month = pd.datetime.strptime(period_start, '%Y-%m').month
        period_freq = pd.datetools.YearEnd(month=period_month)
        frames = []
        for index, (check_name, frame_name) in enumerate(response_structure):
            if frame_name and tables[index][0] == check_name:
                frame = self._process_frame(
                    tables[index][1], frame_name, period_start, period_freq)
                if (frame is not None and frame.index.size > 0):
                    frames.append(frame)
        return frames

    def _process_frame(self, frame, frame_name, period_start, period_freq):
        """Returns a processed pandas.DataFrame based on the original frame.
        frame: Original pandas.DataFrame to be processed.
        frame_name: New name assigned to the new (processed) pandas.DataFrame.
        period_start: Start of the period.
        period_freq: Frequency of the period.
        """
        output_frame = frame.set_index(frame[0])
        del output_frame[0]
        output_frame.index.name = frame_name
        output_frame.columns = pd.period_range(period_start,
            periods=len(output_frame.ix[0]), freq=period_freq)
        output_frame.columns.name = 'Period'
        if re.match(r'^\d{4}-\d{2}$', output_frame.ix[0][0]):
            output_frame.drop(output_frame.index[0], inplace=True)
        output_frame.replace(',', '', regex=True, inplace=True)
        output_frame.replace('^\s*$', 'NaN', regex=True, inplace=True)
        return output_frame.astype(float)

    def _upload_frames_to_db(self, ticker, frames, conn):
        """Uploads the given array of pandas.DataFrames to the MySQL database.
        ticker: Morningstar ticker.
        frames: Array of pandas.DataFrames to be uploaded.
        conn: MySQLdb connection.
        """
        for frame in frames:
            table_name = self._get_db_table_name(frame)
            if not _db_table_exists(table_name, conn):
                _db_execute(self._get_db_create_table(frame), conn)
            _db_execute(self._get_db_replace_values(ticker, frame), conn)

    def _get_db_name(self, name):
        """Returns a new (cleaned) name that can be used in a MySQL database.
        name: Original name.
        """
        name = name.lower()\
            .replace('/', ' per ')\
            .replace('&', ' and ')\
            .replace('%', ' percent ')
        name = re.sub(r'[^a-z0-9]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name.replace(' ', '_')

    def _get_db_table_name(self, frame):
        """Returns the MySQL TABLE name for the given pandas.DataFrame.
        frame: pandas.DataFrame.
        """
        return self._table_prefix + self._get_db_name(frame.index.name)

    def _get_db_create_table(self, frame):
        """Returns the MySQL CREATE TABLE statement for the given
        pandas.DataFrame.
        frame: pandas.DataFrame.
        """
        columns = ',\n'.join([
            '  `%s` DECIMAL(20,5) DEFAULT NULL COMMENT "%s"' %\
            (self._get_db_name(name), name) for name in frame.index.values])
        table_name = self._get_db_table_name(frame)
        return \
          'CREATE TABLE `%s` (\n' % table_name +\
          '  `ticker` VARCHAR(50) NOT NULL COMMENT "Exchange:Ticker",\n' +\
          '  `period` DATE NOT NULL COMMENT "Period",\n' +\
          '%s,\n' % columns +\
          '  PRIMARY KEY USING BTREE (`ticker`, `period`),\n' +\
          '  KEY `ix_ticker` USING BTREE (`ticker`))\n' +\
          'ENGINE=MyISAM DEFAULT CHARSET=utf8\n' +\
          'COMMENT = "%s"' % frame.index.name

    def _get_db_replace_values(self, ticker, frame):
        """Returns the MySQL REPLACE INTO statement for the given
        Morningstar ticker and the corresponding pandas.DataFrame.
        ticker: Morningstar ticker.
        frame: pandas.DataFrame.
        """
        columns = ['`ticker`', '`period`'] +\
          ['`%s`' % self._get_db_name(name) for name in frame.index.values]
        return \
          'REPLACE INTO `%s`\n' % self._get_db_table_name(frame) +\
          '  (%s)\nVALUES\n' % ',\n   '.join(columns) +\
          ',\n'.join([
              '("' + ticker + '", "' + column.strftime('%Y-%m-%d') + '", ' +
              ', '.join(['NULL' if np.isnan(x) else '%.5f' % x
                         for x in frame[column].values]) +
              ')' for column in frame.columns])

class FinancialsDownloader(object):
    """Downloads financials from http://financials.morningstar.com/
    """
    def __init__(self, table_prefix = 'morningstar_'):
        """Constructs the FinancialsDownloader instance.
        table_prefix: Prefix of the MySQL tables.
        """
        self._table_prefix = table_prefix

    def download(self, ticker, conn = None):
        """Downloads and returns a dictionary containing pandas.DataFrames
        representing the financials (i.e. income statement, balance sheet,
        cash flow) for the given Morningstar ticker. If the MySQL connection
        is specified then the downloaded financials are uploaded to the MySQL
        database.
        ticker: Morningstar ticker.
        conn: MySQLdb connection.
        """
        result = {}
        for report_type, table_name in [
            ('is', 'income_statement'),
            ('bs', 'balance_sheet'),
            ('cf', 'cash_flow')]:
            frame = self._download(ticker, report_type)
            result[table_name] = frame
            if conn: self._upload_frame(
                frame, ticker, self._table_prefix + table_name, conn)
        if conn: self._upload_unit(ticker, self._table_prefix + 'unit', conn)
        result['period_range'] = self._period_range
        result['fiscal_year_end'] = self._fiscal_year_end
        result['currency'] = self._currency
        return result

    def _download(self, ticker, report_type):
        """Downloads and returns a pandas.DataFrame corresponding to the
        given Morningstar ticker and the given type of the report.
        ticker: Morningstar ticker.
        report_type: Type of the report ('is', 'bs', 'cf').
        """
        url = r"http://financials.morningstar.com/ajax/" +\
            r"ReportProcess4HtmlAjax.html?&t=" + ticker +\
            r"&region=usa&culture=en-US&cur=USD" +\
            r"&reportType=" + report_type + r"&period=12" +\
            r"&dataType=A&order=asc&columnYear=5&rounding=3&view=raw"
        response = json.loads(urllib2.urlopen(url).read())
        return self._parse(BeautifulSoup(response['result']))

    def _parse(self, soup):
        """Extracts and returns a pandas.DataFrame corresponding to the
        given parsed HTML repsonse from the financials.morningstar.com.
        soup: Parsed HTML response by BeautifulSoup.
        """
        # left node contains the labels
        left = soup.find('div', 'left').div
        # main node contains the (raw) data
        main = soup.find('div', 'main').find('div', 'rf_table')
        year = main.find('div', {'id': 'Year'})
        self._year_ids = [node.attrs['id'] for node in year]
        period_month = pd.datetime.strptime(
            year.div.text, '%Y-%m').month
        self._period_range = pd.period_range(year.div.text,
            periods=len(self._year_ids),
            freq=pd.datetools.YearEnd(month=period_month))
        unit = left.find('div', {'id': 'unitsAndFiscalYear'})
        self._fiscal_year_end = int(unit.attrs['fyenumber'])
        self._currency = unit.attrs['currency']
        self._data = []
        self._label_index = 0
        self._read_labels(left)
        self._data_index = 0
        self._read_data(main)
        return pd.DataFrame(self._data,
            columns=['parent_index', 'title'] +
            list(self._period_range))

    def _read_labels(self, root_node, parent_label_index = None):
        """Recursively reads labels from the parsed HTML response.
        """
        for node in root_node:
            if node.has_attr('class') and\
                'r_content' in node.attrs['class']:
                self._read_labels(node, self._label_index - 1)
            if node.has_attr('id') and\
                node.attrs['id'].startswith('label') and\
                not node.attrs['id'].endswith('padding') and\
                ((not node.has_attr('style')) or
                 ('display:none' not in node.attrs['style'])):
                label_id = node.attrs['id'][6:]
                label_title = node.div.attrs['title']\
                    if node.div.has_attr('title')\
                    else node.div.text
                self._data.append({
                    'id': label_id,
                    'index': self._label_index,
                    'parent_index': parent_label_index
                        if parent_label_index is not None
                        else self._label_index,
                    'title': label_title})
                self._label_index += 1

    def _read_data(self, root_node):
        """Recursively reads data from the parsed HTML response.
        """
        for node in root_node:
            if node.has_attr('class') and\
                'r_content' in node.attrs['class']:
                self._read_data(node)
            if node.has_attr('id') and\
                node.attrs['id'].startswith('data') and\
                not node.attrs['id'].endswith('padding') and\
                ((not node.has_attr('style')) or
                 ('display:none' not in node.attrs['style'])):
                data_id = node.attrs['id'][5:]
                assert(self._data[self._data_index]['id'] == data_id)
                for (i, child) in enumerate(node.children):
                    try:
                        value = float(child.attrs['rawvalue'])
                    except ValueError:
                        value = None
                    self._data[self._data_index][
                        self._period_range[i]] = value
                self._data_index += 1

    def _upload_frame(self, frame, ticker, table_name, conn):
        """Uploads the given pandas.DataFrame to the MySQL database.
        frame: pandas.DataFrames to be uploaded.
        ticker: Morningstar ticker.
        table_name: Name of the MySQL table.
        conn: MySQLdb connection.
        """
        if not _db_table_exists(table_name, conn):
            _db_execute(self._get_db_create_table(table_name), conn)
        _db_execute(self._get_db_replace_values(
            ticker, frame, table_name), conn)

    def _upload_unit(self, ticker, table_name, conn):
        """Uploads the fiscal_year_end and the currency to the MySQL database.
        ticker: Morningstar ticker.
        table_name: Name of the MySQL table.
        conn: MySQLdb connection.
        """
        if not _db_table_exists(table_name, conn):
            _db_execute(
                'CREATE TABLE `%s` (\n' % table_name +\
                '  `ticker` varchar(50) NOT NULL\n' +\
                '    COMMENT "Exchange:Ticker",\n' +\
                '  `fiscal_year_end` int(10) unsigned NOT NULL\n' +\
                '    COMMENT  "Fiscal Year End Month",\n' +\
                '  `currency` varchar(50) NOT NULL\n' +\
                '    COMMENT "Currency",\n' +\
                '  PRIMARY KEY USING BTREE (`ticker`))\n' +\
                'ENGINE=MyISAM DEFAULT CHARSET=utf8', conn)
        _db_execute(
            'REPLACE INTO `%s`\n' % table_name +\
            '  (`ticker`, `fiscal_year_end`, `currency`)\nVALUES\n' +\
            '("%s", %d, "%s")' % (
                ticker, self._fiscal_year_end, self._currency), conn)

    def _get_db_create_table(self, table_name):
        """Returns the MySQL CREATE TABLE statement for the given
        table table_name.
        table_name: Name of the MySQL table.
        """
        year = date.today().year
        year_range = range(year - 6, year + 1)
        columns = ',\n'.join([
            '  `year_%d` DECIMAL(20,5) DEFAULT NULL ' % year + 
            'COMMENT "Year %d"' % year
            for year in year_range])
        return \
          'CREATE TABLE `%s` (\n' % table_name +\
          '  `ticker` VARCHAR(50) NOT NULL COMMENT "Exchange:Ticker",\n' +\
          '  `id` int(10) unsigned NOT NULL COMMENT "Id",\n' +\
          '  `parent_id` int(10) unsigned NOT NULL COMMENT "Parent Id",\n' +\
          '  `item` varchar(500) NOT NULL COMMENT "Item",\n' +\
          '%s,\n' % columns +\
          '  PRIMARY KEY USING BTREE (`ticker`, `id`),\n' +\
          '  KEY `ix_ticker` USING BTREE (`ticker`))\n' +\
          'ENGINE=MyISAM DEFAULT CHARSET=utf8'

    def _get_db_replace_values(self, ticker, frame, table_name):
        """Returns the MySQL REPLACE INTO statement for the given
        Morningstar ticker and the corresponding pandas.DataFrame.
        ticker: Morningstar ticker.
        frame: pandas.DataFrame.
        table_name: Name of the MySQL table.
        """
        columns = ['`ticker`', '`id`, `parent_id`, `item`'] +\
            ['`year_%d`' % period.year for period in frame.columns[2:]]
        return \
          'REPLACE INTO `%s`\n' % table_name +\
          '  (%s)\nVALUES\n' % ', '.join(columns) +\
          ',\n'.join([
              '("' + ticker + '", %d, %d, "%s", ' % 
              (index, frame.ix[index, 'parent_index'],
                      frame.ix[index, 'title']) +\
              ', '.join(['NULL' if np.isnan(frame.ix[index, period])
                         else '%.5f' % frame.ix[index, period]
                         for period in frame.columns[2:]]) +\
              ')' for index in frame.index])

def _db_table_exists(table_name, conn):
    """MySQLdb helper method to check whether a MySQL table exists.
    table_name: Name of the MySQL table to be checked.
    conn: MySQLdb connection.
    """
    dbcur = conn.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(table_name))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True
    dbcur.close()
    return False

def _db_execute(query, conn):
    """MySQLdb helper method to execute a MySQL query.
    query: MySQL query to be executed.
    conn: MySQLdb connection.
    """
    dbcur = conn.cursor()
    dbcur.execute(query)
    dbcur.close()