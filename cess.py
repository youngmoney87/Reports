import os
import re
import pandas as pd
from Report import get_report, CESS


working_file = get_report()

self = CESS(working_file, name=input('Please provide client name: '))

self.main_report()

self.comparison_report()

raw_data = self.raw_data()
