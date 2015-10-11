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
import http.client
import json
import numpy as np
import pandas as pd
import pymysql
import re
import urllib.request
from bs4 import BeautifulSoup, Tag
from datetime import date


class KeyRatiosDownloader(object):
    """Downloads key ratios from http://financials.morningstar.com/
    """

    def __init__(self, table_prefix: str = 'morningstar_'):
        """Constructs the KeyRatiosDownloader instance.

        :param table_prefix: Prefix of the MySQL tables.
        """
        self._table_prefix = table_prefix

    def download(self, ticker: str, conn: pymysql.Connection = None) -> list:
        """Downloads and returns key ratios for the given Morningstar ticker.

        Downloads and returns an array of pandas.DataFrames containing the key
        ratios for the given Morningstar ticker. If the MySQL connection is
        specified then the downloaded key ratios are uploaded to the MySQL
        database.

        :param ticker: Morningstar ticker.
        :param conn: MySQL connection.
        :return: List of pandas.DataFrames containing the key ratios.
        """
        url = (r'http://financials.morningstar.com/ajax/exportKR2CSV.html?' +
               r'&callback=?&t={0}&region=usa&culture=en-US&cur=USD'.format(
                   ticker))
        with urllib.request.urlopen(url) as response:
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
            currency = re.match('^.* ([A-Z]+) Mil$',
                                frames[0].index[0]).group(1)
            frames[0].index.name += ' ' + currency
            if conn:
                self._upload_frames_to_db(ticker, frames, conn)
            return frames

    @staticmethod
    def _parse_tables(response: http.client.HTTPResponse) -> list:
        """Parses the given csv response from financials.morningstar.com.

        :param response: Response from financials.morningstar.com.
        :return: List of pairs, where the first item is the name of the table
        (extracted from the response) and the second item is the corresponding
        pandas.DataFrame table containing the data.
        """
        # Regex pattern used to recognize csv lines containing financial data.
        num_commas = 5
        pat_commas = r'(.*,){%d,}' % num_commas
        # Resulting array of pairs (table_name, table_frame).
        tables = []
        table_name = None
        table_rows = None
        for line in response:
            line = line.decode('utf-8').strip()
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

    @staticmethod
    def _parse_frames(tables: list, response_structure: list) -> list:
        """Returns an array of processed pandas.DataFrames based on the
        original list of tables and the special response_structure list.

        :param tables: Original list of tables (obtained from _parse_tables).
        :param response_structure: List of pairs (expected table name, new name
        assigned to the corresponding (processed) pandas.DataFrame).
        """
        period_start = tables[0][1].ix[0][1]
        period_month = pd.datetime.strptime(period_start, '%Y-%m').month
        period_freq = pd.datetools.YearEnd(month=period_month)
        frames = []
        for index, (check_name, frame_name) in enumerate(response_structure):
            if frame_name and tables[index][0] == check_name:
                frame = KeyRatiosDownloader._process_frame(
                    tables[index][1], frame_name, period_start, period_freq)
                if frame is not None and frame.index.size > 0:
                    frames.append(frame)
        return frames

    @staticmethod
    def _process_frame(frame: pd.DataFrame, frame_name: str, period_start,
                       period_freq) -> pd.DataFrame:
        """Returns a processed pandas.DataFrame based on the original frame.

        :param frame: Original pandas.DataFrame to be processed.
        :param frame_name: New name assigned to the processed pandas.DataFrame.
        :param period_start: Start of the period.
        :param period_freq: Frequency of the period.
        :return Processed pandas.DataFrame based on the original frame.
        """
        output_frame = frame.set_index(frame[0])
        del output_frame[0]
        output_frame.index.name = frame_name
        output_frame.columns = pd.period_range(period_start,
                                               periods=len(output_frame.ix[0]),
                                               freq=period_freq)
        output_frame.columns.name = 'Period'
        if re.match(r'^\d{4}-\d{2}$', output_frame.ix[0][0]):
            output_frame.drop(output_frame.index[0], inplace=True)
        output_frame.replace(',', '', regex=True, inplace=True)
        output_frame.replace('^\s*$', 'NaN', regex=True, inplace=True)
        return output_frame.astype(float)

    def _upload_frames_to_db(self, ticker: str, frames: list,
                             conn: pymysql.Connection):
        """Uploads the given array of pandas.DataFrames to the MySQL database.

        :param ticker: Morningstar ticker.
        :param frames: Array of pandas.DataFrames to be uploaded.
        :param conn: MySQL connection.
        """
        for frame in frames:
            table_name = self._get_db_table_name(frame)
            if not _db_table_exists(table_name, conn):
                _db_execute(self._get_db_create_table(frame), conn)
            _db_execute(self._get_db_replace_values(ticker, frame), conn)

    @staticmethod
    def _get_db_name(name: str) -> str:
        """Returns a new (cleaned) name that can be used in a MySQL database.

        :param name: Original name.
        :return Name that can be used in a MySQL database.
        """
        name = (name.lower()
                .replace('/', ' per ')
                .replace('&', ' and ')
                .replace('%', ' percent '))
        name = re.sub(r'[^a-z0-9]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name.replace(' ', '_')

    def _get_db_table_name(self, frame: pd.DataFrame) -> str:
        """Returns the MySQL TABLE name for the given pandas.DataFrame.

        :param frame: pandas.DataFrame.
        :return MySQL TABLE name.
        """
        return self._table_prefix + self._get_db_name(frame.index.name)

    def _get_db_create_table(self, frame: pd.DataFrame) -> str:
        """Returns the MySQL CREATE TABLE statement for the given
        pandas.DataFrame.

        :param frame: pandas.DataFrame.
        :return MySQL CREATE TABLE statement.
        """
        columns = (',\n'.
                   join(['  `%s` DECIMAL(20,5) DEFAULT NULL COMMENT "%s"' %
                         (self._get_db_name(name), name) for name in
                         frame.index.values]))
        table_name = self._get_db_table_name(frame)
        return (
            'CREATE TABLE `%s` (\n' % table_name +
            '  `ticker` VARCHAR(50) NOT NULL COMMENT "Exchange:Ticker",\n' +
            '  `period` DATE NOT NULL COMMENT "Period",\n' +
            '%s,\n' % columns +
            '  PRIMARY KEY USING BTREE (`ticker`, `period`),\n' +
            '  KEY `ix_ticker` USING BTREE (`ticker`))\n' +
            'ENGINE=MyISAM DEFAULT CHARSET=utf8\n' +
            'COMMENT = "%s"' % frame.index.name)

    def _get_db_replace_values(self, ticker: str, frame: pd.DataFrame) -> str:
        """Returns the MySQL REPLACE INTO statement for the given
        Morningstar ticker and the corresponding pandas.DataFrame.

        :param ticker: Morningstar ticker.
        :param frame: pandas.DataFrame.
        :return MySQL REPLACE INTO statement.
        """
        columns = (['`ticker`', '`period`'] +
                   ['`%s`' % self._get_db_name(name) for name in
                    frame.index.values])
        return (
            'REPLACE INTO `%s`\n' % self._get_db_table_name(frame) +
            '  (%s)\nVALUES\n' % ',\n   '.join(columns) +
            ',\n'.join(['("' + ticker + '", "' + column.strftime('%Y-%m-%d') +
                        '", ' +
                        ', '.join(['NULL' if np.isnan(x) else '%.5f' % x
                                   for x in frame[column].values]) +
                        ')' for column in frame.columns]))


class FinancialsDownloader(object):
    """Downloads financials from http://financials.morningstar.com/
    """

    def __init__(self, table_prefix: str = 'morningstar_'):
        """Constructs the FinancialsDownloader instance.

        :param table_prefix: Prefix of the MySQL tables.
        """
        self._table_prefix = table_prefix

    def download(self, ticker: str, conn: pymysql.Connection = None) -> dict:
        """Downloads and returns a dictionary containing pandas.DataFrames
        representing the financials (i.e. income statement, balance sheet,
        cash flow) for the given Morningstar ticker. If the MySQL connection
        is specified then the downloaded financials are uploaded to the MySQL
        database.

        :param ticker: Morningstar ticker.
        :param conn: MySQL connection.
        :return Dictionary containing pandas.DataFrames representing the
        financials for the given Morningstar ticker.
        """
        result = {}
        for report_type, table_name in [
                ('is', 'income_statement'),
                ('bs', 'balance_sheet'),
                ('cf', 'cash_flow')]:
            frame = self._download(ticker, report_type)
            result[table_name] = frame
            if conn:
                self._upload_frame(
                    frame, ticker, self._table_prefix + table_name, conn)
        if conn:
            self._upload_unit(ticker, self._table_prefix + 'unit', conn)
        result['period_range'] = self._period_range
        result['fiscal_year_end'] = self._fiscal_year_end
        result['currency'] = self._currency
        return result

    def _download(self, ticker: str, report_type: str) -> pd.DataFrame:
        """Downloads and returns a pandas.DataFrame corresponding to the
        given Morningstar ticker and the given type of the report.

        :param ticker: Morningstar ticker.
        :param report_type: Type of the report ('is', 'bs', 'cf').
        :return  pandas.DataFrame corresponding to the given Morningstar ticker
        and the given type of the report.
        """
        url = (r'http://financials.morningstar.com/ajax/' +
               r'ReportProcess4HtmlAjax.html?&t=' + ticker +
               r'&region=usa&culture=en-US&cur=USD' +
               r'&reportType=' + report_type + r'&period=12' +
               r'&dataType=A&order=asc&columnYear=5&rounding=3&view=raw')
        with urllib.request.urlopen(url) as response:
            json_text = response.read().decode('utf-8')
            json_data = json.loads(json_text)
            result_soup = BeautifulSoup(json_data['result'])
            return self._parse(result_soup)

    def _parse(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Extracts and returns a pandas.DataFrame corresponding to the
        given parsed HTML response from financials.morningstar.com.

        :param soup: Parsed HTML response by BeautifulSoup.
        :return pandas.DataFrame corresponding to the given parsed HTML response
        from financials.morningstar.com.
        """
        # Left node contains the labels.
        left = soup.find('div', 'left').div
        # Main node contains the (raw) data.
        main = soup.find('div', 'main').find('div', 'rf_table')
        year = main.find('div', {'id': 'Year'})
        self._year_ids = [node.attrs['id'] for node in year]
        period_month = pd.datetime.strptime(year.div.text, '%Y-%m').month
        self._period_range = pd.period_range(
            year.div.text, periods=len(self._year_ids),
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
                            columns=['parent_index', 'title'] + list(
                                self._period_range))

    def _read_labels(self, root_node: Tag, parent_label_index: int = None):
        """Recursively reads labels from the parsed HTML response.
        """
        for node in root_node:
            if node.has_attr('class') and 'r_content' in node.attrs['class']:
                self._read_labels(node, self._label_index - 1)
            if (node.has_attr('id') and
                    node.attrs['id'].startswith('label') and
                    not node.attrs['id'].endswith('padding') and
                    (not node.has_attr('style') or
                        'display:none' not in node.attrs['style'])):
                label_id = node.attrs['id'][6:]
                label_title = (node.div.attrs['title']
                               if node.div.has_attr('title')
                               else node.div.text)
                self._data.append({
                    'id': label_id,
                    'index': self._label_index,
                    'parent_index': (parent_label_index
                                     if parent_label_index is not None
                                     else self._label_index),
                    'title': label_title})
                self._label_index += 1

    def _read_data(self, root_node: Tag):
        """Recursively reads data from the parsed HTML response.
        """
        for node in root_node:
            if node.has_attr('class') and 'r_content' in node.attrs['class']:
                self._read_data(node)
            if (node.has_attr('id') and
                    node.attrs['id'].startswith('data') and
                    not node.attrs['id'].endswith('padding') and
                    (not node.has_attr('style') or
                        'display:none' not in node.attrs['style'])):
                data_id = node.attrs['id'][5:]
                while (self._data_index < len(self._data) and
                       self._data[self._data_index]['id'] != data_id):
                    # In some cases we do not have data for all labels.
                    self._data_index += 1
                assert(self._data_index < len(self._data) and
                       self._data[self._data_index]['id'] == data_id)
                for (i, child) in enumerate(node.children):
                    try:
                        value = float(child.attrs['rawvalue'])
                    except ValueError:
                        value = None
                    self._data[self._data_index][
                        self._period_range[i]] = value
                self._data_index += 1

    def _upload_frame(self, frame: pd.DataFrame, ticker: str, table_name: str,
                      conn: pymysql.Connection):
        """Uploads the given pandas.DataFrame to the MySQL database.

        :param frame: pandas.DataFrames to be uploaded.
        :param ticker: Morningstar ticker.
        :param table_name: Name of the MySQL table.
        :param conn: MySQL connection.
        """
        if not _db_table_exists(table_name, conn):
            _db_execute(self._get_db_create_table(table_name), conn)
        _db_execute(self._get_db_replace_values(
            ticker, frame, table_name), conn)

    def _upload_unit(self, ticker: str, table_name: str,
                     conn: pymysql.Connection):
        """Uploads the fiscal_year_end and the currency to the MySQL database.

        :param ticker: Morningstar ticker.
        :param table_name: Name of the MySQL table.
        :param conn: MySQL connection.
        """
        if not _db_table_exists(table_name, conn):
            _db_execute(
                'CREATE TABLE `%s` (\n' % table_name +
                '  `ticker` varchar(50) NOT NULL\n' +
                '    COMMENT "Exchange:Ticker",\n' +
                '  `fiscal_year_end` int(10) unsigned NOT NULL\n' +
                '    COMMENT  "Fiscal Year End Month",\n' +
                '  `currency` varchar(50) NOT NULL\n' +
                '    COMMENT "Currency",\n' +
                '  PRIMARY KEY USING BTREE (`ticker`))\n' +
                'ENGINE=MyISAM DEFAULT CHARSET=utf8', conn)
        _db_execute(
            'REPLACE INTO `%s`\n' % table_name +
            '  (`ticker`, `fiscal_year_end`, `currency`)\nVALUES\n' +
            '("%s", %d, "%s")' % (
                ticker, self._fiscal_year_end, self._currency), conn)

    @staticmethod
    def _get_db_create_table(table_name: str) -> str:
        """Returns the MySQL CREATE TABLE statement for the given table_name.

        :param table_name: Name of the MySQL table.
        :return MySQL CREATE TABLE statement.
        """
        year = date.today().year
        year_range = range(year - 6, year + 2)
        columns = ',\n'.join(
            ['  `year_%d` DECIMAL(20,5) DEFAULT NULL ' % year +
             'COMMENT "Year %d"' % year
             for year in year_range])
        return (
            'CREATE TABLE `%s` (\n' % table_name +
            '  `ticker` VARCHAR(50) NOT NULL COMMENT "Exchange:Ticker",\n' +
            '  `id` int(10) unsigned NOT NULL COMMENT "Id",\n' +
            '  `parent_id` int(10) unsigned NOT NULL COMMENT "Parent Id",\n' +
            '  `item` varchar(500) NOT NULL COMMENT "Item",\n' +
            '%s,\n' % columns +
            '  PRIMARY KEY USING BTREE (`ticker`, `id`),\n' +
            '  KEY `ix_ticker` USING BTREE (`ticker`))\n' +
            'ENGINE=MyISAM DEFAULT CHARSET=utf8')

    @staticmethod
    def _get_db_replace_values(ticker: str, frame: pd.DataFrame,
                               table_name: str) -> str:
        """Returns the MySQL REPLACE INTO statement for the given
        Morningstar ticker and the corresponding pandas.DataFrame.

        :param ticker: Morningstar ticker.
        :param frame: pandas.DataFrame.
        :param table_name: Name of the MySQL table.
        :return MySQL REPLACE INTO statement.
        """
        columns = ['`ticker`', '`id`, `parent_id`, `item`'] + \
                  ['`year_%d`' % period.year for period in
                   frame.columns[2:]]
        return (
            'REPLACE INTO `%s`\n' % table_name +
            '  (%s)\nVALUES\n' % ', '.join(columns) +
            ',\n'.join(['("' + ticker + '", %d, %d, "%s", ' %
                        (index, frame.ix[index, 'parent_index'],
                         frame.ix[index, 'title']) +
                        ', '.join(
                            ['NULL' if np.isnan(frame.ix[index, period])
                             else '%.5f' % frame.ix[index, period]
                             for period in frame.columns[2:]]) + ')'
                        for index in frame.index]))


def _db_table_exists(table_name: str, conn: pymysql.Connection) -> bool:
    """Helper method for checking whether the given MySQL table exists.

    :param table_name: Name of the MySQL table to be checked.
    :param conn: MySQL connection.
    :return True iff the given MySQL table exists.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'""".format(table_name))
    table_exists = cursor.fetchone()[0] == 1
    cursor.close()
    return table_exists


def _db_execute(query: str, conn: pymysql.Connection):
    """Helper method for executing the given MySQL non-query.

    :param query: MySQL query to be executed.
    :param conn: MySQL connection.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
