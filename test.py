import pandas as pd

s = pd.date_range(start='20040301', end='20220301', freq='3M').tolist()
s = [str(i).replace(' 00:00:00','') for i in s]
print(s)
