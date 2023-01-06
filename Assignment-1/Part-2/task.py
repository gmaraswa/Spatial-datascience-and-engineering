import numpy as np
import pandas as pd


def get_bands_max(df):
	pdf = df.toPandas()
	num= np.array(df.collect())
	numpyArray=[]
	for index, row in pdf.iterrows():
		dataInBands=np.array_split(row["data"], row["bands"])
		in_arr1 =[]
		for x in dataInBands:
			in_arr1.append(np.max(x))
		numpyArray.append(in_arr1)
	return np.array(numpyArray)