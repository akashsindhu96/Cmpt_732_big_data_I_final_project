import pandas as pd

df = pd.read_csv("part-00000-27b595e9-68a3-4fe8-884b-df24009df931-c000.csv")

for i in df.blink_spot:
    for j in i:
        for k in j:
            lat =k[0]
            long = k[1]