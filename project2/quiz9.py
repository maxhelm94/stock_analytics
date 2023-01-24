import pandas as pd
technologies = {
    'Fee' :[20000,25000,22000,23000,30000],
    'Discount':[1000,2300,1200,2000,1000]
              }
index_labels=['r1','r2','r3','r4','r5']
df = pd.DataFrame(technologies,index=index_labels)
#print(df)

print(type(df.mean()))
