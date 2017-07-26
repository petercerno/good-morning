#!/usr/bin/env python

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

"""Download historical MorningStart data."""

from __future__ import absolute_import

from good_morning.good_morning import KeyRatiosDownloader,FinancialsDownloader

__name__ = 'good_morning'
__author__ = 'Peter Cerno'
__email__ = 'petercerno@gmail.com'
__license__ = 'MIT.X11'
__version__ = '0.1.0'
__url__ = 'https://github.com/petercerno/good-morning'
__description__ = 'Good Morning (good_morning) is a simple Python module for downloading fundamental financial data from [financials.morningstar.com](http://financials.morningstar.com/).'
