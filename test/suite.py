#!/usr/bin/env python
#
# Copyright 2015-2015 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
"""
Suite to run all test modules in the test dir.

@author: Andy Georges
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import test.inode_log as inode_log
import unittest

from vsc.utils import fancylogger
fancylogger.logToScreen(enable=False)

suite = unittest.TestSuite(
    [x.suite() for x in
        (
            inode_log,
        )
    ]
)

try:
    import xmlrunner
    rs = xmlrunner.XMLTestRunner(output="test-reports").run(suite)
except ImportError, err:
    rs = unittest.TextTestRunner().run(suite)

if not rs.wasSuccessful():
    sys.exit(1)