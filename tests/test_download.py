#!/usr/bin/python
# -*- coding: utf-8 -*-


from unittest import TestCase

from good_morning import good_morning as gm


class TestDownloadReturns(TestCase):
    def test_downloadreturn(self):
        kr = gm.KeyRatiosDownloader()
        frames = kr.download('aapl')
        test = len(frames)
        exp = 11
        self.assertEqual(test, exp, "Download is working")

    def test_download_fail_empty(self):
        kr = gm.KeyRatiosDownloader()
        tickersym = ''
        exp = "You did not enter a ticker symbol.  Please try again."
        with self.assertRaises(Exception) as context:
            checked = kr.download(tickersym)
        the_exception = context.exception
        return self.assertEqual(exp, str(the_exception),
                                "Passing an empty string to "
                                "good_morning fails")

    def test_download_fail_invalid(self):
        kr = gm.KeyRatiosDownloader()
        tickersym = 'nothing'
        exp = "MorningStar cannot find the ticker symbol you entered " \
              "or it is INVALID. Please try again."
        with self.assertRaises(Exception) as context:
            checked = kr.download(tickersym)
        the_exception = context.exception
        return self.assertEqual(exp, str(the_exception),
                                "Passing an invalid ticker symbol to"
                                " good_morning fails")



