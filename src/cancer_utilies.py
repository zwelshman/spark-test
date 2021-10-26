#!/usr/bin/env python
# coding: utf-8

# In[5]:


from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when, col


# In[6]:


class CreateCancerHighLevel_ML(DataFrame):
    def __init__(self, df):
        super().__init__(df._jdf, df.sql_ctx)
        self._df = df

    def chapters_C18_C21_Colorectal(self, column):
        try:
            df = self
            return df.withColumn('Colorectal',
                                when(
                                    (df[column].contains('C18')) |
                                    (df[column].contains('C19')) |
                                    (df[column].contains('C20')) |
                                    (df[column].contains('C21')), 1))

        except Exception as e:
            print(e)
    
    def chapters_C34_Lung(self, column):
    
        try:
            df = self
            return df.withColumn('Lung',
                                    when((df[column].contains('C34')), 1))

        except Exception as e:
            print(e)


# In[7]:


print('notebook run')


# In[ ]:




