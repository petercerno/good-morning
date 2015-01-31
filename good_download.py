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
"""Example showing how to download financial data from
financials.morningstar.com for all tickers in S&P 500 (2015).
"""

import MySQLdb
import time
import good_morning as gm

DB_HOST = 'db_host'
DB_USER = 'db_user'
DB_PASS = 'db_pass'
DB_NAME = 'db_name'

conn = MySQLdb.connect(
    host = DB_HOST, user = DB_USER, passwd = DB_PASS, db = DB_NAME)

kr = gm.KeyRatiosDownloader()
fd = gm.FinancialsDownloader()

sp500_2015 = ["A", "AA", "AAPL", "ABBV", "ABC", "ABT", "ACE", "ACN", "ACT",
  "ADBE", "ADI", "ADM", "ADP", "ADS", "ADSK", "ADT", "AEE", "AEP", "AES",
  "AET", "AFL", "AGN", "AIG", "AIV", "AIZ", "AKAM", "ALL", "ALLE", "ALTR",
  "ALXN", "AMAT", "AME", "AMG", "AMGN", "AMP", "AMT", "AMZN", "AN", "ANTM",
  "AON", "APA", "APC", "APD", "APH", "ARG", "ATI", "AVB", "AVGO", "AVP",
  "AVY", "AXP", "AZO", "BA", "BAC", "BAX", "BBBY", "BBT", "BBY", "BCR", "BDX",
  "BEN", "BF.B", "BHI", "BIIB", "BK", "BLK", "BLL", "BMY", "BRCM", "BRK.B",
  "BSX", "BWA", "BXP", "C", "CA", "CAG", "CAH", "CAM", "CAT", "CB", "CBG",
  "CBS", "CCE", "CCI", "CCL", "CELG", "CERN", "CF", "CFN", "CHK", "CHRW",
  "CI", "CINF", "CL", "CLX", "CMA", "CMCSA", "CME", "CMG", "CMI", "CMS",
  "CNP", "CNX", "COF", "COG", "COH", "COL", "COP", "COST", "CPB", "CRM",
  "CSC", "CSCO", "CSX", "CTAS", "CTL", "CTSH", "CTXS", "CVC", "CVS", "CVX",
  "D", "DAL", "DD", "DE", "DFS", "DG", "DGX", "DHI", "DHR", "DIS", "DISCA",
  "DISCK", "DLPH", "DLTR", "DNB", "DNR", "DO", "DOV", "DOW", "DPS", "DRI",
  "DTE", "DTV", "DUK", "DVA", "DVN", "EA", "EBAY", "ECL", "ED", "EFX", "EIX",
  "EL", "EMC", "EMN", "EMR", "ENDP", "EOG", "EQR", "EQT", "ESRX", "ESS",
  "ESV", "ETFC", "ETN", "ETR", "EW", "EXC", "EXPD", "EXPE", "F", "FAST", "FB",
  "FCX", "FDO", "FDX", "FE", "FFIV", "FIS", "FISV", "FITB", "FLIR", "FLR",
  "FLS", "FMC", "FOSL", "FOXA", "FSLR", "FTI", "FTR", "GAS", "GCI", "GD",
  "GE", "GGP", "GILD", "GIS", "GLW", "GM", "GMCR", "GME", "GNW", "GOOG",
  "GOOGL", "GPC", "GPS", "GRMN", "GS", "GT", "GWW", "HAL", "HAR", "HAS",
  "HBAN", "HCA", "HCBK", "HCN", "HCP", "HD", "HES", "HIG", "HOG", "HON",
  "HOT", "HP", "HPQ", "HRB", "HRL", "HRS", "HSP", "HST", "HSY", "HUM", "IBM",
  "ICE", "IFF", "INTC", "INTU", "IP", "IPG", "IR", "IRM", "ISRG", "ITW",
  "IVZ", "JCI", "JEC", "JNJ", "JNPR", "JOY", "JPM", "JWN", "K", "KEY", "KIM",
  "KLAC", "KMB", "KMI", "KMX", "KO", "KORS", "KR", "KRFT", "KSS", "KSU", "L",
  "LB", "LEG", "LEN", "LH", "LLL", "LLTC", "LLY", "LM", "LMT", "LNC", "LO",
  "LOW", "LRCX", "LUK", "LUV", "LVLT", "LYB", "M", "MA", "MAC", "MAR", "MAS",
  "MAT", "MCD", "MCHP", "MCK", "MCO", "MDLZ", "MDT", "MET", "MHFI", "MHK",
  "MJN", "MKC", "MLM", "MMC", "MMM", "MNK", "MNST", "MO", "MON", "MOS", "MPC",
  "MRK", "MRO", "MS", "MSFT", "MSI", "MTB", "MU", "MUR", "MWV", "MYL", "NAVI",
  "NBL", "NBR", "NDAQ", "NE", "NEE", "NEM", "NFLX", "NFX", "NI", "NKE",
  "NLSN", "NOC", "NOV", "NRG", "NSC", "NTAP", "NTRS", "NU", "NUE", "NVDA",
  "NWL", "NWSA", "OI", "OKE", "OMC", "ORCL", "ORLY", "OXY", "PAYX", "PBCT",
  "PBI", "PCAR", "PCG", "PCL", "PCLN", "PCP", "PDCO", "PEG", "PEP", "PETM",
  "PFE", "PFG", "PG", "PGR", "PH", "PHM", "PKI", "PLD", "PLL", "PM", "PNC",
  "PNR", "PNW", "POM", "PPG", "PPL", "PRGO", "PRU", "PSA", "PSX", "PVH",
  "PWR", "PX", "PXD", "QCOM", "QEP", "R", "RAI", "RCL", "REGN", "RF", "RHI",
  "RHT", "RIG", "RL", "ROK", "ROP", "ROST", "RRC", "RSG", "RTN", "SBUX",
  "SCG", "SCHW", "SE", "SEE", "SHW", "SIAL", "SJM", "SLB", "SNA", "SNDK",
  "SNI", "SO", "SPG", "SPLS", "SRCL", "SRE", "STI", "STJ", "STT", "STX",
  "STZ", "SWK", "SWN", "SYK", "SYMC", "SYY", "T", "TAP", "TDC", "TE", "TEG",
  "TEL", "TGT", "THC", "TIF", "TJX", "TMK", "TMO", "TRIP", "TROW", "TRV",
  "TSCO", "TSN", "TSO", "TSS", "TWC", "TWX", "TXN", "TXT", "TYC", "UA", "UHS",
  "UNH", "UNM", "UNP", "UPS", "URBN", "URI", "USB", "UTX", "V", "VAR", "VFC",
  "VIAB", "VLO", "VMC", "VNO", "VRSN", "VRTX", "VTR", "VZ", "WAT", "WBA",
  "WDC", "WEC", "WFC", "WFM", "WHR", "WIN", "WM", "WMB", "WMT", "WU", "WY",
  "WYN", "WYNN", "XEC", "XEL", "XL", "XLNX", "XOM", "XRAY", "XRX", "XYL",
  "YHOO", "YUM", "ZION", "ZMH", "ZTS"]

for ticker in sp500_2015:
    print ticker,
    try:
        kr.download(ticker, conn)
        fd.download(ticker, conn)
        time.sleep(1)
        print " ... success"
    except Exception, e:
        print " ... failed"
        print e