import pandas as pd 
import numpy as np 
class QueryEngine:
   def __init__(self, dataframe):
         self.df = dataframe
         
   # select columns
   def select(self, columns):
         return self.df[columns]
   def head_5(self):
         return self.df.head()
   def head_n(self,n):
         self.n = n
         return self.df.head(n)
   def tail_5(self,n=5):
         return self.df.tail(n)
   def tail_n(self,n):
      self.n = n
      return self.df.tail(n)
   def info(self):
         return self.df.info()
   def describe(self):
         return self.df.describe() 
   def dtypes(self):
         return self.df.dtypes
      
   # checking null values
   def isNull(self):
         return self.df.isnull().sum()
   # checking for duplicates
   def isDuplicate(self):
         return self.df.duplicated().sum()
   # checking for white spaces
   def isWhiteSpace(self):
         return np.where(self.df.map(lambda x: isinstance(x, str) and x.isspace()))      
   # cleaning process
   def dropNull(self):
         return self.df.dropna(inplace=True)
   def fillna(self, value, column_name):
         self.df[column_name] = self.df[column_name].fillna(value)
         return self.df
   def dropDuplicate(self):
         return self.df.drop_duplicates(inplace=True)
   def trimWhiteSpace(self):
         return self.df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
   # Filtering
   def filter(self, condition):
         return self.df.query(condition)
   def order_by(self, by, ascending=True):
         self.by = by
         self.ascending = ascending
         return self.df.sort_values(by=by, ascending=ascending)  
   def group_by(self, by, agg_func):
         # Only apply aggregation to numeric columns to avoid TypeError
         numeric_cols = self.df.select_dtypes(include=[np.number]).columns
         return self.df.groupby(by)[numeric_cols].agg(agg_func)

   # Group by with multiple aggregations
   def group_by_multi(self, by, agg_dict):
         """
         Group by columns and apply multiple aggregations.
         by: list of columns to group by
         agg_dict: dict of column: aggregation_function pairs
         Example: {'Age': 'mean', 'Fare': ['sum', 'max']}
         """
         return self.df.groupby(by).agg(agg_dict) 
   def pivot_table(self, index, columns, values, aggfunc='mean'):
         return pd.pivot_table(self.df, index=index, columns=columns, values=values, aggfunc=aggfunc)

   # Joins
   def inner_join(self, other_df, on, how='inner'):
         return self.df.join(other_df.set_index(on), on=on, how=how) 
   def outer_join(self, other_df, on, how='outer'):
         return self.df.join(other_df.set_index(on), on=on, how=how) 
   def left_join(self, other_df, on, how='left'):
         return self.df.join(other_df.set_index(on), on=on, how=how) 
   def right_join(self, other_df, on, how='right'):
         return self.df.join(other_df.set_index(on), on=on, how=how) 
   def cross_join(self, other_df, on, how='cross'):
         return pd.merge(self.df, other_df, on=on, how=how) 
   
   def execute(self, query: str):
      q = query.strip().upper()

      # === Special non-SQL shortcuts ===
      if q.startswith("HEAD"):
            n = int(query.split()[-1])
            return self.head_n(n)

      if q.startswith("TAIL"):
            n = int(query.split()[-1])
            return self.tail_n(n)

      if q.startswith("INFO"):
            return self.info()

      if q.startswith("DESCRIBE"):
            return self.describe()

      # === Otherwise: treat as SQL SELECT ===
      return self._execute_sql(query)


   def _execute_sql(self, query: str):
      """Handles SELECT/WHERE/ORDER BY/LIMIT queries"""
      import sqlparse
      
      parsed = sqlparse.parse(query)[0]
      tokens = [t for t in parsed.tokens if not t.is_whitespace]

      # Projection
      select_idx = query.upper().find("SELECT") + 6
      from_idx = query.upper().find("FROM")
      columns_str = query[select_idx:from_idx].strip()
      columns = [c.strip() for c in columns_str.split(",")] if columns_str != "*" else None

      # WHERE
      where_clause = None
      if "WHERE" in query.upper():
            where_idx = query.upper().find("WHERE") + 5
            rest = query[where_idx:]
            if "ORDER BY" in rest.upper():
                  where_clause = rest[:rest.upper().find("ORDER BY")].strip()
            elif "LIMIT" in rest.upper():
                  where_clause = rest[:rest.upper().find("LIMIT")].strip()
            else:
                  where_clause = rest.strip()

      # ORDER BY
      order_clause = None
      if "ORDER BY" in query.upper():
            order_idx = query.upper().find("ORDER BY") + 8
            rest = query[order_idx:]
            if "LIMIT" in rest.upper():
                  order_clause = rest[:rest.upper().find("LIMIT")].strip()
            else:
                  order_clause = rest.strip()

      # LIMIT
      limit_clause = None
      if "LIMIT" in query.upper():
            limit_idx = query.upper().find("LIMIT") + 5
            limit_clause = int(query[limit_idx:].strip())

      # === Apply operations ===
      result = self.df.copy()

      if where_clause:
            result = result.query(where_clause)

      if columns and columns != ["*"]:
            result = result[columns]

      if order_clause:
            order_parts = order_clause.split()
            col = order_parts[0]
            ascending = not (len(order_parts) > 1 and order_parts[1].upper() == "DESC")
            result = result.sort_values(by=col, ascending=ascending)

      if limit_clause:
            result = result.head(limit_clause)

      return result