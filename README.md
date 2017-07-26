
Good Morning
============

Good Morning (morningstar) is a simple Python module for downloading fundamental financial data from [financials.morningstar.com](http://financials.morningstar.com/). It will work as long as the structure of the responses from [financials.morningstar.com](http://financials.morningstar.com/) do not change.

**Prerequisites:** 

- [Python 3](https://www.python.org/), [bs4](http://www.crummy.com/software/BeautifulSoup/bs4/doc/), [csv](https://docs.python.org/3/library/csv.html), [datetime](https://docs.python.org/3/library/datetime.html), [http.client](https://docs.python.org/3/library/http.client.html), [json](https://docs.python.org/3/library/json.html), [numpy](http://www.numpy.org/), [pandas](http://pandas.pydata.org/), [pymysql](https://pypi.python.org/pypi/PyMySQL), [re](https://docs.python.org/3/library/re.html), [urllib.request](https://docs.python.org/3/library/urllib.request.html).

Motivation
==========

Good Morning is intended to be used as an extension to [QSToolKit (QSTK)](http://wiki.quantsoftware.org/index.php?title=QuantSoftware_ToolKit) library. By using [QSTK](http://wiki.quantsoftware.org/index.php?title=QuantSoftware_ToolKit) you can easily download historical stock market data from [Yahoo Finance](http://finance.yahoo.com/). You can also download fundamental financial data from [Compustat](https://www.capitaliq.com/home/what-we-offer/information-you-need/financials-valuation/compustat-financials.aspx). However, most individuals and institutions do not have access to [Compustat](https://www.capitaliq.com/home/what-we-offer/information-you-need/financials-valuation/compustat-financials.aspx). Good Morning attempts to mitigate this limitation by providing a very simple Python interface for downloading fundamental financial data from [financials.morningstar.com](http://financials.morningstar.com/).

Example of Downloading Key Ratios from MorningStar
==================================================

    import good_morning as gm
    kr = gm.KeyRatiosDownloader()
    kr_frames = kr.download('AAPL')

The variable `kr_frames` now holds an array of [`pandas.DataFrame`](http://pandas.pydata.org/pandas-docs/dev/generated/pandas.DataFrame.html)s containing the key ratios for the morningstar ticker [`AAPL`](http://financials.morningstar.com/ratios/r.html?t=AAPL&region=usa&culture=en-US).

    print kr_frames[0]

Outputs:

    Period                            2005      2006      2007 ...
    Key Financials USD                                         ...
    Revenue USD Mil               13931.00  19315.00  24006.00 ...
    Gross Margin %                   29.00     29.00     34.00 ...
    Operating Income USD Mil       1650.00   2453.00   4409.00 ...
    Operating Margin %               11.80     12.70     18.40 ...
    Net Income USD Mil             1335.00   1989.00   3496.00 ...
    Earnings Per Share USD            0.22      0.32      0.56 ...
    ...

Example of Downloading Financial data 
=====================================

    import good_morning as gm
    kr = gm.FinancialsDownloader()
    kr_fins = kr.download('AAPL')
    
Different from the `KeyRatiosDownloader` class,  `kr_fins` now holds a dictionary containing the financials for the morningstar ticker [`AAPL`](http://financials.morningstar.com/ratios/r.html?t=AAPL&region=usa&culture=en-US). The financials **may differ** from company to company.

    print (fins.keys())
    
Output:

    dict_keys(['income_statement', 'balance_sheet', 'cash_flow', 'period_range', 'fiscal_year_end', 'currency'])

Storing Good Morning Data in a Database 
======================================================

We can also store this data in a relational database.  If we specify the MySQL connection `conn` the retrieved data will be uploaded to the MySQL database:

    import pymysql
    conn = pymysql.connect(
        host = DB_HOST, user = DB_USER, passwd = DB_PASS, db = DB_NAME)
    kr_frames = kr.download('AAPL', conn)

Every [`pandas.DataFrame`](http://pandas.pydata.org/pandas-docs/dev/generated/pandas.DataFrame.html) in the array `kr_frames` will be uploaded to a different database table. In our case the following tables will be created: 

    `morningstar_key_balance_sheet_items_in_percent`
    `morningstar_key_cash_flow_ratios`
    `morningstar_key_efficiency_ratios`
    `morningstar_key_eps_percent`
    `morningstar_key_financials_usd`
    `morningstar_key_liquidity_per_financial_health`
    `morningstar_key_margins_percent_of_sales`
    `morningstar_key_net_income_percent`
    `morningstar_key_operating_income_percent`
    `morningstar_key_profitability`
    `morningstar_key_revenue_percent`

For example, the following MySQL query:

    SELECT * FROM `morningstar_key_cash_flow_ratios`;

Outputs:

    ticker      period     ...
      AAPL  2005-09-30  171.41  200.13  1.87  16.33  1.70
      AAPL  2006-09-30  -12.43  -31.30  3.40   8.09  0.79
      AAPL  2007-09-30  146.40  186.88  4.11  18.68  1.28
      AAPL  2008-09-30   75.43   87.27  3.69  25.85  1.74
    ...

Where the columns are:

    ticker
    period
    operating_cash_flow_growth_percent_yoy
    free_cash_flow_growth_percent_yoy
    cap_ex_as_a_percent_of_sales
    free_cash_flow_per_sales_percent
    free_cash_flow_per_net_income

Unit Tests
----------

We include unittest to troubleshoot your use of the library.  It's a simple command line process to run the test. Navigate to the base directory of `good-morning` and use the standard library `unittest` command line interface:

    python -m unittest test/
    
Output:

    ----------------------------------------------------------------------
    Ran 3 tests in 0.626s
    
    OK

If you see anything other than this, you should get an error report. Before submitting an issue, run the test and try to paste the output if the error persists.


Available Classes
-----------------

There are two classes in `good_morning`:

- `KeyRatiosDownloader` - Used to download key ratios. Key ratios share the same structure across all Morningstar tickers.
- `FinancialsDownloader` - Used to download financials (i.e. income statement, balance sheet, cash flow). Financials may differ in structure across the Morningstar tickers.

LICENSE
=======

Good Morning is licensed to you under MIT.X11:

Copyright (c) 2015 Peter Cerno

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
