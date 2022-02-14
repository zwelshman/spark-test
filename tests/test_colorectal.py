#!/usr/bin/env python
# coding: utf-8

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd
from functools import reduce


# initialize sparksession
spark = SparkSession.builder.appName('app').getOrCreate()
# initialize list of lists

data = [['Rand1', 'C34,C18,C19', '2021-01-01'], 
        ['Rand1', 'C34,C18,C19', '2020-12-29'],
        ['Rand3', 'C50', '2021-07-10'], 
        ['Rand3', 'C50,C21,C50', '2021-04-01'], 
        ['Rand3', 'J12, C20', '2021-06-01'],
        ['Rand3', 'J12, C20', '2021-06-02'] ]
# Create the pandas DataFrame
df = pd.DataFrame(data, columns = ['dummy_name', 'dummy_condition','dummy_admis'])

df.show()
