# Copyright (c) 2014 Peter Cerno
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
import numpy as np
import pandas as pd
import re
import urllib2

def get_key_ratios(ticker):
    """Downloads and returns an array of pandas.DataFrames containing
    the key ratios for the given morningstar ticker.
    """
    url = r"http://financials.morningstar.com/ajax/exportKR2CSV.html?" +\
        r"&callback=?&t=" + ticker + r"&region=usa&culture=en-US&cur=USD"
    response = urllib2.urlopen(url)
    special_cases = []
    tables = parse_tables(response, special_cases)
    response_structure = [
        # Original Name, New pandas.DataFrame Name
        ('Financials', 'KR Financials'),
        ('Key Ratios -> Profitability', 'KR Margins % of Sales'),
        ('Key Ratios -> Profitability', 'KR Profitability'),
        ('Key Ratios -> Growth', None),
        ('Revenue %', 'KR Revenue %'),
        ('Operating Income %', 'KR Operating Income %'),
        ('Net Income %', 'KR Net Income %'),
        ('EPS %', 'KR EPS %'),
        ('Key Ratios -> Cash Flow', 'KR Cash Flow Ratios'),
        ('Key Ratios -> Financial Health', 'KR Balance Sheet Items (in %)'),
        ('Key Ratios -> Financial Health', 'KR Liquidity/Financial Health'),
        ('Key Ratios -> Efficiency Ratios', 'KR Efficiency')]
    return parse_frames(tables, response_structure)

def get_income_statement(ticker):
    """Downloads and returns an array of pandas.DataFrames containing
    the income statement items for the given morningstar ticker.
    """
    url = r"http://financials.morningstar.com/ajax/ReportProcess4CSV.html?" +\
        r"&t=" + ticker + r"&region=usa&culture=en-US&cur=" +\
        r"&reportType=is&period=12&dataType=A&order=asc&columnYear=5" +\
        r"&rounding=3&view=raw&denominatorView=raw&number=3"
    response = urllib2.urlopen(url)
    special_cases = ['Operating income', 'EBITDA']
    tables = parse_tables(response, special_cases)
    tables[0][0] = 'Financials'
    response_structure = [
        # Original Name, New pandas.DataFrame Name
        ('Financials', 'IS Financials USD Mil'),
        ('Operating expenses', 'IS Operating Expenses USD Mil'),
        ('Operating income', 'IS Operating Income USD Mil'),
        ('Earnings per share', 'IS EPS'),
        ('Weighted average shares outstanding',
         'IS Weighted Average Shares Outstanding'),
        ('EBITDA', 'IS EBITDA USD Mil')]
    return parse_frames(tables, response_structure)

def get_balance_sheet(ticker):
    """Downloads and returns an array of pandas.DataFrames containing
    the balance sheet items for the given morningstar ticker.
    """
    url = r"http://financials.morningstar.com/ajax/ReportProcess4CSV.html?" +\
        r"&t=" + ticker + r"&region=usa&culture=en-US&cur=&reportType=bs" +\
        r"&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw" +\
        r"&denominatorView=raw&number=3"
    response = urllib2.urlopen(url)
    special_cases = [
        'Total assets',
        'Total liabilities and stockholders\' equity',
        'Total liabilities']
    tables = parse_tables(response, special_cases)
    tables[0][0] = 'Balance Sheet'
    response_structure = [
        # Original Name, New pandas.DataFrame Name
        ('Balance Sheet', 'BS Balance Sheet USD Mil'),
        ('Cash', 'BS Cash USD Mil'),
        ('"Property, plant and equipment"',
         'BS Property, Plant and Equipment USD Mil'),
        ('Total assets', 'BS Total Assets USD Mil'),
        ('Current liabilities', 'BS Current Liabilities USD Mil'),
        ('Non-current liabilities', 'BS Non-Current Liabilities USD Mil'),
        ('Total liabilities', 'BS Total Liabilities USD Mil'),
        ('Stockholders\' equity', 'BS Stockholders Equity USD Mil'),
        ('Total liabilities and stockholders\' equity',
         'BS Total Liabilities and Stockholders Equity USD Mil')]
    return parse_frames(tables, response_structure)

def get_cash_flow(ticker):
    """Downloads and returns an array of pandas.DataFrames containing
    the cash flow items for the given morningstar ticker.
    """
    url = r"http://financials.morningstar.com/ajax/ReportProcess4CSV.html?" +\
        r"&t=" + ticker + r"&region=usa&culture=en-US&cur=&reportType=cf" +\
        r"&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw" +\
        r"&denominatorView=raw&number=3"
    response = urllib2.urlopen(url)
    special_cases = []
    tables = parse_tables(response, special_cases)
    tables[0][0] = 'Cash Flow'
    response_structure = [
        # Original Name, New pandas.DataFrame Name
        ('Cash Flow', 'CF Cash Flow USD Mil'),
        ('Cash Flows From Operating Activities',
         'CF Cash Flows From Operating Activities USD Mil'),
        ('Cash Flows From Investing Activities',
         'CF Cash Flows From Investing Activities USD Mil'),
        ('Cash Flows From Financing Activities',
         'CF Cash Flows From Financing Activities USD Mil'),
        ('Free Cash Flow', 'CF Free Cash Flow USD Mil')]
    return parse_frames(tables, response_structure)

def upload_frames_to_db(conn, ticker, frames):
    """Uploads the given array of pandas.DataFrames to the MySQL database.
    conn: MySQLdb connection.
    ticker: morningstar ticker.
    frames: array of pandas.DataFrames to be uploaded.
    """
    for frame in frames:
        upload_frame_to_db(conn, ticker, frame)

def upload_frame_to_db(conn, ticker, frame):
    """Uploads the given pandas.DataFrame to the MySQL database table (with
    name based on the name of the frame).
    conn: MySQLdb connection.
    ticker: morningstar ticker.
    frame: pandas.DataFrames to be uploaded.
    """
    db_execute(conn, get_db_create_table(frame))
    db_execute(conn, get_db_replace_values(frame, ticker))

def parse_tables(response, special_cases):
    """Parses the given csv response from financials.morningstar.com.
    Returns an array of pairs, where the first item is the name of the table
    (extracted from the response) and the second item is the corresponding
    pandas.DataFrame table containing the data.
    response: csv response from financials.morningstar.com.
    special_cases: array of names that we want to put to separate tables.
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
        for special_case in special_cases:
            if line.startswith(special_case):
                tables.append([table_name, pd.DataFrame(table_rows)])
                table_rows = []
                table_name = special_case
                break
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

def parse_frames(tables, response_structure):
    """Returns the array of processed pandas.DataFrames based on the original
    array of tables and the special response_structure array.
    tables: original array of tables (obtained from the parse_tables method).
    response_structure: array of pairs (expected table name, new name assigned
        to the corresponding (processed) pandas.DataFrame).
    """
    period_start = tables[0][1].ix[0][1]
    period_freq = 'Y'
    frames = []
    for index, (check_name, frame_name) in enumerate(response_structure):
        if frame_name:
            frame = check_frame(tables[index], check_name, frame_name, 
                                period_start, period_freq)
            if (frame is not None and frame.index.size > 0):
                frames.append(frame)
    return frames

def check_frame(table, check_name, frame_name, period_start, period_freq='Y'):
    """If the given table has the required (expected) name, returns a processed
    pandas.DataFrame based on the original table. Otherwise, returns None.
    table: pair (table_name, table_frame).
    check_name: expected table name.
    frame_name: new name assigned to the new (processed) pandas.DataFrame.
    period_start: start of the period.
    period_freq: frequency of the period.
    """
    if table[0] == check_name:
        return process_frame(table[1], frame_name, period_start, period_freq)
    return None

def process_frame(frame, frame_name, period_start, period_freq='Y'):
    """Returns a new (processed) pandas.DataFrame based on the original frame.
    frame: original pandas.DataFrame to be processed.
    frame_name: new name assigned to the new (processed) pandas.DataFrame.
    period_start: start of the period.
    period_freq: frequency of the period.
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

def get_db_name(name):
    """Returns a new (cleaned) name that can be used in a MySQL database.
    name: original name.
    """
    name = name.lower()\
        .replace('/', ' per ')\
        .replace('&', ' and ')\
        .replace('%', ' percent ')
    name = re.sub(r'[^a-z0-9]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.replace(' ', '_')

def get_db_table_name(frame):
    """Returns the MySQL TABLE name for the given pandas.DataFrame.
    frame: pandas.DataFrame.
    """
    return "`%s`" % get_db_name(frame.index.name)
    
def get_db_create_table(frame):
    """Returns the MySQL CREATE TABLE statement for the given pandas.DataFrame.
    frame: pandas.DataFrame.
    """
    columns = ',\n'.join(['  `%s` DECIMAL(20,5) DEFAULT NULL COMMENT "%s"' %\
        (get_db_name(name), name) for name in frame.index.values])
    return \
      'CREATE TABLE IF NOT EXISTS %s (\n' % get_db_table_name(frame) +\
      '  `ticker` VARCHAR(50) NOT NULL COMMENT "Exchange:Ticker",\n' +\
      '  `period` DATE NOT NULL COMMENT "Period",\n' +\
      '%s,\n' % columns +\
      '  PRIMARY KEY USING BTREE (`ticker`, `period`))\n' +\
      'ENGINE = MyISAM\n' +\
      'COMMENT = "%s"' % frame.index.name

def get_db_replace_values(frame, ticker):
    """Returns the MySQL REPLACE INTO statement for the given pandas.DataFrame.
    frame: pandas.DataFrame.
    """
    columns = ['`ticker`', '`period`'] +\
      ['`%s`' % get_db_name(name) for name in frame.index.values]
    return \
      'REPLACE INTO %s\n' % get_db_table_name(frame) +\
      '  (%s)\nVALUES\n' % ',\n   '.join(columns) +\
      ',\n'.join([
          '  ("' + ticker + '", "' + column.strftime('%Y-%m-%d') + '", ' +
          ', '.join(['NULL' if np.isnan(x) else '%.2f' % x
                     for x in frame[column].values]) +
          ')' for column in frame.columns])

def db_execute(conn, query):
    """MySQLdb helper method to execute a MySQL query.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
