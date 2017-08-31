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
import urllib.request
from bs4 import BeautifulSoup
from datetime import date

class KeyRatiosDownloader(object):
    u"""Downloads key ratios from http://financials.morningstar.com/
    """

    def __init__(self, table_prefix = u'morningstar_'):
        u"""Constructs the KeyRatiosDownloader instance.

        :param table_prefix: Prefix of the MySQL tables.
        """
        self._table_prefix = table_prefix

    def download(self, ticker, conn = None, region = 'GBR', culture = 'en_US', currency = 'USD'):
        u"""Downloads and returns key ratios for the given Morningstar ticker.

        Downloads and returns an array of pandas.DataFrames containing the key
        ratios for the given Morningstar ticker. If the MySQL connection is
        specified then the downloaded key ratios are uploaded to the MySQL
        database.

        :param ticker: Morningstar ticker.
        :param conn: MySQL connection.
        :param region: Sets the region.
        :param culture: Sets culture.
        :param currency: Sets currency.
        :return: List of pandas.DataFrames containing the key ratios.
        """
        url = (r'http://financials.morningstar.com/ajax/exportKR2CSV.html?' +
               r'&callback=?&t={t}&region={reg}&culture={cult}&cur={cur}'.format(
                   t=ticker, reg=region, cult=culture, cur=currency))
        with urllib.request.urlopen(url) as response:
            tables = self._parse_tables(response)
            response_structure = [
                # Original Name, New pandas.DataFrame Name
                (u'Financials', u'Key Financials'),
                (u'Key Ratios -> Profitability', u'Key Margins % of Sales'),
                (u'Key Ratios -> Profitability', u'Key Profitability'),
                (u'Key Ratios -> Growth', None),
                (u'Revenue %', u'Key Revenue %'),
                (u'Operating Income %', u'Key Operating Income %'),
                (u'Net Income %', u'Key Net Income %'),
                (u'EPS %', u'Key EPS %'),
                (u'Key Ratios -> Cash Flow', u'Key Cash Flow Ratios'),
                (u'Key Ratios -> Financial Health',
                 u'Key Balance Sheet Items (in %)'),
                (u'Key Ratios -> Financial Health',
                 u'Key Liquidity/Financial Health'),
                (u'Key Ratios -> Efficiency Ratios', u'Key Efficiency Ratios')]
            frames = self._parse_frames(tables, response_structure)

            ############################
            # Error Handling for Ratios
            ############################

            # Empty String
            if len(ticker) == 0:
                raise ValueError("You did not enter a ticker symbol.  Please"
                                 " try again.")
            # Wrong ticker symbol
            elif frames == "MorningStar could not find the ticker":
                raise ValueError("MorningStar cannot find the ticker symbol "
                                 "you entered or it is INVALID. Please try "
                                 "again.")

            currency = re.match(u'^.* ([A-Z]+) Mil$',
                                frames[0].index[0]).group(1)
            frames[0].index.name += u' ' + currency
            if conn:
                self._upload_frames_to_db(ticker, frames, conn)
            return frames

    @staticmethod
    def _parse_tables(response):
        u"""Parses the given csv response from financials.morningstar.com.

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
            line = line.decode(u'utf-8').strip()
            match = re.match(pat_commas, line)
            if match:
                for row in csv.reader([line]):
                    table_rows.append(row)
            else:
                if table_name and table_rows:
                    tables.append([table_name, pd.DataFrame(table_rows)])
                if line != u'':
                    table_name = line
                table_rows = []
        if table_name and table_rows:
            tables.append([table_name, pd.DataFrame(table_rows)])
        return tables

    @staticmethod
    def _parse_frames(tables, response_structure):
        u"""Returns an array of processed pandas.DataFrames based on the
        original list of tables and the special response_structure list.

        :param tables: Original list of tables (obtained from _parse_tables).
        :param response_structure: List of pairs (expected table name, new name
        assigned to the corresponding (processed) pandas.DataFrame).
        """

        #############################
        # Error Handling
        #############################

        # Fail Early on Empty String
        if len(tables) == 0:
            return ("MorningStar could not find the ticker")

        period_start = tables[0][1].ix[0][1]
        period_month = pd.datetime.strptime(period_start, u'%Y-%m').month
        #period_freq = pd.datetools.YearEnd(month=period_month)
        period_freq = pd.tseries.offsets.YearEnd(month=period_month)
        frames = []
        for index, (check_name, frame_name) in enumerate(response_structure):
            if frame_name and tables[index][0] == check_name:
                frame = KeyRatiosDownloader._process_frame(
                    tables[index][1], frame_name, period_start, period_freq)
                if frame is not None and frame.index.size > 0:
                    frames.append(frame)
        return frames

    @staticmethod
    def _process_frame(frame, frame_name, period_start,
                       period_freq):
        u"""Returns a processed pandas.DataFrame based on the original frame.

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
        output_frame.columns.name = u'Period'
        if re.match(r'^\d{4}-\d{2}$', output_frame.ix[0][0]):
            output_frame.drop(output_frame.index[0], inplace=True)
        output_frame.replace(u',', u'', regex=True, inplace=True)
        output_frame.replace(u'^\s*$', u'NaN', regex=True, inplace=True)
        return output_frame.astype(float)

    def _upload_frames_to_db(self, ticker, frames,
                             conn):
        u"""Uploads the given array of pandas.DataFrames to the MySQL database.

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
    def _get_db_name(name):
        u"""Returns a new (cleaned) name that can be used in a MySQL database.

        :param name: Original name.
        :return Name that can be used in a MySQL database.
        """
        name = (name.lower()
                .replace(u'/', u' per ')
                .replace(u'&', u' and ')
                .replace(u'%', u' percent '))
        name = re.sub(r'[^a-z0-9]', u' ', name)
        name = re.sub(r'\s+', u' ', name).strip()
        return name.replace(u' ', u'_')

    def _get_db_table_name(self, frame):
        u"""Returns the MySQL TABLE name for the given pandas.DataFrame.

        :param frame: pandas.DataFrame.
        :return MySQL TABLE name.
        """
        return self._table_prefix + self._get_db_name(frame.index.name)

    def _get_db_create_table(self, frame):
        u"""Returns the MySQL CREATE TABLE statement for the given
        pandas.DataFrame.

        :param frame: pandas.DataFrame.
        :return MySQL CREATE TABLE statement.
        """
        columns = (u',\n'.
                   join([u'  `%s` DECIMAL(20,5) DEFAULT NULL COMMENT "%s"' %
                         (self._get_db_name(name), name) for name in
                         frame.index.values]))
        table_name = self._get_db_table_name(frame)
        return (
            u'CREATE TABLE `%s` (\n' % table_name +
            u'  `ticker` VARCHAR(50) NOT NULL COMMENT "Exchange:Ticker",\n' +
            u'  `period` DATE NOT NULL COMMENT "Period",\n' +
            u'%s,\n' % columns +
            u'  PRIMARY KEY USING BTREE (`ticker`, `period`),\n' +
            u'  KEY `ix_ticker` USING BTREE (`ticker`))\n' +
            u'ENGINE=MyISAM DEFAULT CHARSET=utf8\n' +
            u'COMMENT = "%s"' % frame.index.name)

    def _get_db_replace_values(self, ticker, frame):
        u"""Returns the MySQL REPLACE INTO statement for the given
        Morningstar ticker and the corresponding pandas.DataFrame.

        :param ticker: Morningstar ticker.
        :param frame: pandas.DataFrame.
        :return MySQL REPLACE INTO statement.
        """
        columns = ([u'`ticker`', u'`period`'] +
                   [u'`%s`' % self._get_db_name(name) for name in
                    frame.index.values])
        return (
            u'REPLACE INTO `%s`\n' % self._get_db_table_name(frame) +
            u'  (%s)\nVALUES\n' % u',\n   '.join(columns) +
            u',\n'.join([u'("' + ticker + u'", "' + column.strftime(u'%Y-%m-%d') +
                        u'", ' +
                        u', '.join([u'NULL' if np.isnan(x) else u'%.5f' % x
                                   for x in frame[column].values]) +
                        u')' for column in frame.columns]))


class FinancialsDownloader(object):
    u"""Downloads financials from http://financials.morningstar.com/
    """

    def __init__(self, table_prefix = u'morningstar_'):
        u"""Constructs the FinancialsDownloader instance.

        :param table_prefix: Prefix of the MySQL tables.
        """
        self._table_prefix = table_prefix

    def download(self, ticker, conn = None):
        u"""Downloads and returns a dictionary containing pandas.DataFrames
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

        ##########################
        # Error Handling
        ##########################

        # Empty String
        if len(ticker) == 0:
            raise ValueError("You did not enter a ticker symbol.  Please"
                             " try again.")

        for report_type, table_name in [
                (u'is', u'income_statement'),
                (u'bs', u'balance_sheet'),
                (u'cf', u'cash_flow')]:
            frame = self._download(ticker, report_type)
            result[table_name] = frame
            if conn:
                self._upload_frame(
                    frame, ticker, self._table_prefix + table_name, conn)
        if conn:
            self._upload_unit(ticker, self._table_prefix + u'unit', conn)
        result[u'period_range'] = self._period_range
        result[u'fiscal_year_end'] = self._fiscal_year_end
        result[u'currency'] = self._currency
        return result

    def _download(self, ticker, report_type):
        u"""Downloads and returns a pandas.DataFrame corresponding to the
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
            json_text = response.read().decode(u'utf-8')

            ##############################
            # Error Handling
            ##############################

            # Wrong ticker
            if len(json_text)==0:
                raise ValueError("MorningStar cannot find the ticker symbol "
                                 "you entered or it is INVALID. Please try "
                                 "again.")

            json_data = json.loads(json_text)
            result_soup = BeautifulSoup(json_data[u'result'],u'html.parser')
            return self._parse(result_soup)

    def _parse(self, soup):
        u"""Extracts and returns a pandas.DataFrame corresponding to the
        given parsed HTML response from financials.morningstar.com.

        :param soup: Parsed HTML response by BeautifulSoup.
        :return pandas.DataFrame corresponding to the given parsed HTML response
        from financials.morningstar.com.
        """
        # Left node contains the labels.
        left = soup.find(u'div', u'left').div
        # Main node contains the (raw) data.
        main = soup.find(u'div', u'main').find(u'div', u'rf_table')
        year = main.find(u'div', {u'id': u'Year'})
        self._year_ids = [node.attrs[u'id'] for node in year]
        period_month = pd.datetime.strptime(year.div.text, u'%Y-%m').month
        self._period_range = pd.period_range(
            year.div.text, periods=len(self._year_ids),
            # freq=pd.datetools.YearEnd(month=period_month))
            freq = pd.tseries.offsets.YearEnd(month=period_month))
        unit = left.find(u'div', {u'id': u'unitsAndFiscalYear'})
        self._fiscal_year_end = int(unit.attrs[u'fyenumber'])
        self._currency = unit.attrs[u'currency']
        self._data = []
        self._label_index = 0
        self._read_labels(left)
        self._data_index = 0
        self._read_data(main)
        return pd.DataFrame(self._data,
                            columns=[u'parent_index', u'title'] + list(
                                self._period_range))

    def _read_labels(self, root_node, parent_label_index = None):
        u"""Recursively reads labels from the parsed HTML response.
        """
        for node in root_node:
            if node.has_attr(u'class') and u'r_content' in node.attrs[u'class']:
                self._read_labels(node, self._label_index - 1)
            if (node.has_attr(u'id') and
                    node.attrs[u'id'].startswith(u'label') and
                    not node.attrs[u'id'].endswith(u'padding') and
                    (not node.has_attr(u'style') or
                        u'display:none' not in node.attrs[u'style'])):
                label_id = node.attrs[u'id'][6:]
                label_title = (node.div.attrs[u'title']
                               if node.div.has_attr(u'title')
                               else node.div.text)
                self._data.append({
                    u'id': label_id,
                    u'index': self._label_index,
                    u'parent_index': (parent_label_index
                                     if parent_label_index is not None
                                     else self._label_index),
                    u'title': label_title})
                self._label_index += 1

    def _read_data(self, root_node):
        u"""Recursively reads data from the parsed HTML response.
        """
        for node in root_node:
            if node.has_attr(u'class') and u'r_content' in node.attrs[u'class']:
                self._read_data(node)
            if (node.has_attr(u'id') and
                    node.attrs[u'id'].startswith(u'data') and
                    not node.attrs[u'id'].endswith(u'padding') and
                    (not node.has_attr(u'style') or
                        u'display:none' not in node.attrs[u'style'])):
                data_id = node.attrs[u'id'][5:]
                while (self._data_index < len(self._data) and
                       self._data[self._data_index][u'id'] != data_id):
                    # In some cases we do not have data for all labels.
                    self._data_index += 1
                assert(self._data_index < len(self._data) and
                       self._data[self._data_index][u'id'] == data_id)
                for (i, child) in enumerate(node.children):
                    try:
                        value = float(child.attrs[u'rawvalue'])
                    except ValueError:
                        value = None
                    self._data[self._data_index][
                        self._period_range[i]] = value
                self._data_index += 1

    def _upload_frame(self, frame, ticker, table_name,
                      conn):
        u"""Uploads the given pandas.DataFrame to the MySQL database.

        :param frame: pandas.DataFrames to be uploaded.
        :param ticker: Morningstar ticker.
        :param table_name: Name of the MySQL table.
        :param conn: MySQL connection.
        """
        if not _db_table_exists(table_name, conn):
            _db_execute(self._get_db_create_table(table_name), conn)
        _db_execute(self._get_db_replace_values(
            ticker, frame, table_name), conn)

    def _upload_unit(self, ticker, table_name,
                     conn):
        u"""Uploads the fiscal_year_end and the currency to the MySQL database.

        :param ticker: Morningstar ticker.
        :param table_name: Name of the MySQL table.
        :param conn: MySQL connection.
        """
        if not _db_table_exists(table_name, conn):
            _db_execute(
                u'CREATE TABLE `%s` (\n' % table_name +
                u'  `ticker` varchar(50) NOT NULL\n' +
                u'    COMMENT "Exchange:Ticker",\n' +
                u'  `fiscal_year_end` int(10) unsigned NOT NULL\n' +
                u'    COMMENT  "Fiscal Year End Month",\n' +
                u'  `currency` varchar(50) NOT NULL\n' +
                u'    COMMENT "Currency",\n' +
                u'  PRIMARY KEY USING BTREE (`ticker`))\n' +
                u'ENGINE=MyISAM DEFAULT CHARSET=utf8', conn)
        _db_execute(
            u'REPLACE INTO `%s`\n' % table_name +
            u'  (`ticker`, `fiscal_year_end`, `currency`)\nVALUES\n' +
            u'("%s", %d, "%s")' % (
                ticker, self._fiscal_year_end, self._currency), conn)

    @staticmethod
    def _get_db_create_table(table_name):
        u"""Returns the MySQL CREATE TABLE statement for the given table_name.

        :param table_name: Name of the MySQL table.
        :return MySQL CREATE TABLE statement.
        """
        year = date.today().year
        year_range = xrange(year - 6, year + 2)
        columns = u',\n'.join(
            [u'  `year_%d` DECIMAL(20,5) DEFAULT NULL ' % year +
             u'COMMENT "Year %d"' % year
             for year in year_range])
        return (
            u'CREATE TABLE `%s` (\n' % table_name +
            u'  `ticker` VARCHAR(50) NOT NULL COMMENT "Exchange:Ticker",\n' +
            u'  `id` int(10) unsigned NOT NULL COMMENT "Id",\n' +
            u'  `parent_id` int(10) unsigned NOT NULL COMMENT "Parent Id",\n' +
            u'  `item` varchar(500) NOT NULL COMMENT "Item",\n' +
            u'%s,\n' % columns +
            u'  PRIMARY KEY USING BTREE (`ticker`, `id`),\n' +
            u'  KEY `ix_ticker` USING BTREE (`ticker`))\n' +
            u'ENGINE=MyISAM DEFAULT CHARSET=utf8')

    @staticmethod
    def _get_db_replace_values(ticker, frame,
                               table_name):
        u"""Returns the MySQL REPLACE INTO statement for the given
        Morningstar ticker and the corresponding pandas.DataFrame.

        :param ticker: Morningstar ticker.
        :param frame: pandas.DataFrame.
        :param table_name: Name of the MySQL table.
        :return MySQL REPLACE INTO statement.
        """
        columns = [u'`ticker`', u'`id`, `parent_id`, `item`'] + \
                  [u'`year_%d`' % period.year for period in
                   frame.columns[2:]]
        return (
            u'REPLACE INTO `%s`\n' % table_name +
            u'  (%s)\nVALUES\n' % u', '.join(columns) +
            u',\n'.join([u'("' + ticker + u'", %d, %d, "%s", ' %
                        (index, frame.ix[index, u'parent_index'],
                         frame.ix[index, u'title']) +
                        u', '.join(
                            [u'NULL' if np.isnan(frame.ix[index, period])
                             else u'%.5f' % frame.ix[index, period]
                             for period in frame.columns[2:]]) + u')'
                        for index in frame.index]))


def _db_table_exists(table_name, conn):
    u"""Helper method for checking whether the given MySQL table exists.

    :param table_name: Name of the MySQL table to be checked.
    :param conn: MySQL connection.
    :return True iff the given MySQL table exists.
    """
    cursor = conn.cursor()
    cursor.execute(u"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'""".format(table_name))
    table_exists = cursor.fetchone()[0] == 1
    cursor.close()
    return table_exists


def _db_execute(query, conn):
    u"""Helper method for executing the given MySQL non-query.

    :param query: MySQL query to be executed.
    :param conn: MySQL connection.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
