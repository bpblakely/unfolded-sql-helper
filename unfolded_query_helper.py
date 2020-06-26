# The goal of this program is to provide an api-like way to interact with the unfolded SQL database
# Hopefully it helps you query the database even if you don't know much SQL
# SQL database being used for this https://druid.ws.demo.rilldata.io

import json
import requests as rq
import pandas as pd
import datetime
# for QoL state calls, source https://gist.github.com/rogerallen/1583593
us_state_abbrev = {
    'alabama': 'AL',
    'alaska': 'AK',
    'american samoa': 'AS',
    'arizona': 'AZ',
    'arkansas': 'AR',
    'california': 'CA',
    'colorado': 'CO',
    'connecticut': 'CT',
    'delaware': 'DE',
    'district of columbia': 'DC',
    'florida': 'FL',
    'georgia': 'GA',
    'guam': 'GU',
    'hawaii': 'HI',
    'idaho': 'ID',
    'illinois': 'IL',
    'indiana': 'IN',
    'iowa': 'IA',
    'kansas': 'KS',
    'kentucky': 'KY',
    'louisiana': 'LA',
    'maine': 'ME',
    'maryland': 'MD',
    'massachusetts': 'MA',
    'michigan': 'MI',
    'minnesota': 'MN',
    'mississippi': 'MS',
    'missouri': 'MO',
    'montana': 'MT',
    'nebraska': 'NE',
    'nevada': 'NV',
    'new hampshire': 'NH',
    'new jersey': 'NJ',
    'new mexico': 'NM',
    'new york': 'NY',
    'north carolina': 'NC',
    'north dakota': 'ND',
    'northern mariana islands':'MP',
    'ohio': 'OH',
    'oklahoma': 'OK',
    'oregon': 'OR',
    'pennsylvania': 'PA',
    'puerto rico': 'PR',
    'rhode island': 'RI',
    'south carolina': 'SC',
    'south dakota': 'SD',
    'tennessee': 'TN',
    'texas': 'TX',
    'utah': 'UT',
    'vermont': 'VT',
    'virgin islands': 'VI',
    'virginia': 'VA',
    'washington': 'WA',
    'west virginia': 'WV',
    'wisconsin': 'WI',
    'wyoming': 'WY'
}
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))

class session:
    def __init__(self,token, database_url='https://druid.ws.demo.rilldata.io/druid/v2/sql',
                 datasource = """"druid"."covid_safegraph_facteus_nytimes" """):
        self.database = database_url
        self.headers = {
                'Authorization': 'Bearer {}'.format(token) }
        self.datasource = datasource
    
    # query with no limitation (directly with SQL) by providing a string with an SQL query block
    # returns a pandas dataframe of the resulting query by default, otherwise returns a response
    def query_default(self,sql_string, return_dataframe= True):
        res = rq.post(self.database, headers=self.headers, json= {"query": sql_string })
        if not return_dataframe:
            return res
        print("Status Code:", res.status_code," (200 is what you want)")
        return pd.read_json(json.dumps(res.json()))
    
    # variables = string defining the variables to select with a comma seperating them, use * to get all variables
    # start_date,end_date = strings of the form "year-month-day"
        # optional: specify time window (UTC timezone) "year-month-day 00:00:00"
    # state and county can be used to filter by county, state, or both.
    # limit to specify how much data to pull, "ALL" for all data within the time frame
    def query_time(self, start_date,end_date, variables='*',state = None, county = None, limit = "ALL"):
        where = f'''__time >= TIMESTAMP '{start_date}' AND __time < TIMESTAMP '{end_date}' '''
        if state is not None:
            state = self.__state_verification(state)
            where = where + f'''AND state_abbr= '{state}' '''
        if county is not None:
            where = where + f'''AND County_State LIKE '{county}' ''' # add %county% to get things with the county name in it
        sql = self.build_sql_query([variables,where,str(limit)])
        return self.query_default(sql)
        
    # Takes county and zip then queries the County_State column
    def query_county_state(self, county, state, variables = "*",limit = "ALL"):
        state = self.__state_verification(state)
        where = f'''County_State = '{county + ', '+state}' '''
        sql = self.build_sql_query([variables,where,str(limit)])
        return self.query_default(sql)
        
    # unlike previous functions that have specific query conditions, this supports any condition (however, no state verification)
    # where = string, it is the WHERE part of an SQL query
    def query_any_condition(self, where,variables = "*", limit = "ALL"):
        sql = self.build_sql_query([variables,where,str(limit)])
        return self.query_default(sql)
    
    # helps build the conditions for an SQL query since in python using triple quotes is cumbersome
    # where = list[string], each string in the list is a seperate condition
    # returns a string block of your conditions (to be used in a query where statement)
    def where_builder(self, where):
        conditions = """"""
        for condition in where:
            conditions += condition +' AND '
        conditions = conditions[:-5] # drop the extra ' AND '
        return conditions
    
    # Takes a state abbrevation, validates it, and returns it.
    # or takes a state full name, turns it into abbrevation, and returns the 2 letter abbreviation 
    # always take a state input and then call this function.
    def __state_verification(self,state):
        if len(state) == 2:
            if not state.upper() in abbrev_us_state.keys():
                raise ValueError(f"'{state.upper()}' is not a valid 2 letter state abbreviation")
            else:
                return state.upper()
        state = state.lower()
        if not state in us_state_abbrev:
            raise ValueError(f"'{state}' is not a valid US state or territory")
        else:
            return us_state_abbrev[state]
            
    # Removes time from the date
    # if your dataframe contains __time, return the dataframe with the time truncated
    def truncate_time(self, df):
        if '__time' in list(df.columns):
            df['__time'] = df['__time'].apply(lambda x: x.date())
            return df
        else:
            print("Dataframe does not contain '__time', returning original dataframe")
            return df
        
    # If easy, then simply provide the string in the following form
    # strings = ['select statement', 'where conditional', 'limit number, "ALL" if no limit']
    # If not easy, then you must provide keywords such as SELECT, FROM, ect.
        # This is mainly for supporting more detailed queries automatically for future functions
    def build_sql_query(self,strings, easy=True):
        if easy:
            query = '''
            SELECT {0}
            FROM {1}
            WHERE {2}
            LIMIT {3}
            '''
           
            return query.format(strings[0],self.datasource,strings[1],strings[2])
        query = """"""
        for s in strings:
            query += s +'\n'
        return query