
Good Morning
============

Good Morning is a simple Python module for downloading fundamental financial data from [financials.morningstar.com](http://financials.morningstar.com/). It will work as long as the structure of the csv responses from [financials.morningstar.com](http://financials.morningstar.com/) does not change.

Bitcoin Donation Address: `15p4RbBxbfWYE91wcd3JzuJFj6e1jJeqyU`

**Prerequisites:** 

- [Python 2.7](https://www.python.org/download/releases/2.7/), [csv](https://docs.python.org/2/library/csv.html), [MySQLdb](http://sourceforge.net/projects/mysql-python/), [NumPy](http://www.numpy.org/), [Pandas](http://pandas.pydata.org/), [re](https://docs.python.org/2/library/re.html), [urllib2](https://docs.python.org/2/library/urllib2.html).

Motivation
==========

Good Morning is intended to be used as a tiny extension to [QSToolKit (QSTK)](http://wiki.quantsoftware.org/index.php?title=QuantSoftware_ToolKit) library. By using [QSTK](http://wiki.quantsoftware.org/index.php?title=QuantSoftware_ToolKit) you can easily download historical stock market data from [Yahoo Finance](http://finance.yahoo.com/). You can also download fundamental financial data from [Compustat](https://www.capitaliq.com/home/what-we-offer/information-you-need/financials-valuation/compustat-financials.aspx). However, most individuals and institutions do not have access to [Compustat](https://www.capitaliq.com/home/what-we-offer/information-you-need/financials-valuation/compustat-financials.aspx). Good Morning attempts to mitigate this limitation by providing a very simple Python interface for downloading fundamental financial data from [financials.morningstar.com](http://financials.morningstar.com/).

Example
=======

    import good_morning as gm
    ticker = 'XNYS:IBM'
    income_statement = gm.get_income_statement(ticker)

The variable `income_statement` now holds an array of [`pandas.DataFrame`](http://pandas.pydata.org/pandas-docs/dev/generated/pandas.DataFrame.html)s containing the income statement items for the morningstar ticker [`XNYS:IBM`](http://financials.morningstar.com/income-statement/is.html?t=IBM&region=usa&culture=en-US).

    print income_statement[0]

Outputs:

    Period                  2009   2010    2011    2012   2013   2014
    IS Financials USD Mil                                            
    Revenue                95758  99870  106916  104507  99751  97449
    Cost of revenue        51973  53857   56778   54209  51246  49746
    Gross profit           43785  46013   50138   50298  48505  47703

We can easily upload the retrieved data to a MySQL database:

    import MySQLdb
    conn = MySQLdb.connect(
        host = DB_HOST, user = DB_USER, passwd = DB_PASS, db = DB_NAME)
    gm.upload_frames_to_db(conn, ticker, income_statement)

All [`pandas.DataFrame`](http://pandas.pydata.org/pandas-docs/dev/generated/pandas.DataFrame.html)s in the array `income_statement` will be uploaded to separate database tables. In our case the following tables will be updated: `is_ebitda_usd_mil`, `is_eps`, `is_financials_usd_mil`, `is_operating_expenses_usd_mil`, `is_operating_income_usd_mil`.

    SELECT * FROM `is_financials_usd_mil`;

Outputs:

      ticker      period    revenue  cost_of_revenue  gross_profit
    XNYS:IBM  2009-12-31   95758.00         51973.00      43785.00
    XNYS:IBM  2010-12-31   99870.00         53857.00      46013.00
    XNYS:IBM  2011-12-31  106916.00         56778.00      50138.00
    XNYS:IBM  2012-12-31  104507.00         54209.00      50298.00
    XNYS:IBM  2013-12-31   99751.00         51246.00      48505.00
    XNYS:IBM  2014-12-31   97449.00         49746.00      47703.00

Available Methods
-----------------

- `get_key_ratios(ticker)`
- `get_income_statement(ticker)`
- `get_balance_sheet(ticker)`
- `get_cash_flow(ticker)`
- `upload_frames_to_db(conn, ticker, frames)`
- `upload_frame_to_db(conn, ticker, frame)`

LICENSE
=======

Good Morning is licensed to you under MIT.X11:

Copyright (c) 2014 Peter Cerno

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.