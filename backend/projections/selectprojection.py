class QueryEngine: 
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
