import pandas as pd

#%%
dataset1 = pd.read_csv('dataset/historical_stock_prices.csv')
# rimuovo la colonna che non serve
dataset1.drop(columns='adj_close', inplace=True)

dataset2 = pd.read_csv('dataset/historical_stocks.csv')


#%%
num_rows_with_null1 = dataset1.isnull().any(axis=1).sum()
print(f"\nNumero di righe con almeno un valore nullo dataset1: {num_rows_with_null1}")

num_rows_with_null2 = dataset2.isnull().any(axis=1).sum()
print(f"\nNumero di righe con almeno un valore nullo dataset2: {num_rows_with_null2}")


#%%
print(f"\nNumero di record per dataset 1: {dataset1.shape[0]}, Valori univoci: {dataset1['ticker'].nunique()}")
print(f"\nNumero di record per dataset 2: {dataset2.shape[0]}, Valori univoci: {dataset2['ticker'].nunique()}")
#print(f"\nNumero di valori univoci per dataset 1: {dataset1['ticker'].nunique()}")

#print(f"\nNumero di record per dataset 2: {cleaned_dataset2.shape[0]}")
#print(f"\nNumero di valori univoci per dataset 2: {cleaned_dataset2['ticker'].nunique()}")


#%%
filtered_dataset2 = dataset2[dataset2['ticker'].isin(dataset1['ticker'])]
print(f"\nNumero di valori univoci per dataset 2 dopo aver rimosso gli elementi"
      f"\n che non compaiono in dataset 1: {filtered_dataset2['ticker'].nunique()}")

print(f"valore che mi aspetto: {dataset1['ticker'].nunique()}")
#%%
print(filtered_dataset2['ticker'].nunique())
#%%
num_rows_with_null2 = filtered_dataset2.isnull().any(axis=1).sum()
print(f"\nNumero di righe con almeno un valore nullo dataset2: {num_rows_with_null2}")
filteredds2 = filtered_dataset2.dropna(inplace=False)
num_rows_with_null2 = filteredds2.isnull().any(axis=1).sum()
print(f"\nNumero di righe con almeno un valore nullo dataset2 dopo il dropNa: {num_rows_with_null2}")

#%%
#merge dataset per job 1
merge_dataset = pd.merge(dataset1, filteredds2, on='ticker', how='inner')
#%%
print(f"\nNumero di record per dataset 1: {merge_dataset.shape[0]}, Valori univoci: {merge_dataset['ticker'].nunique()}")
num_rows_with_null2 = merge_dataset.isnull().any(axis=1).sum()
print(f"\nNumero di righe con almeno un valore nullo dataset2: {num_rows_with_null2}")
#%%
#contiene il merge dei due dataset una volta eliminati i valori nulli e la virgola dal campo name
merge_dataset['name'] = merge_dataset['name'].str.replace(',', '')
#%%
dataset_job1 = merge_dataset[['ticker', 'close', 'low', 'high', 'volume', 'date', 'name']]
#modificare dataset per aggiunta di open price
dataset_job2 = merge_dataset[['ticker', 'date', 'close', 'open', 'industry', 'sector', 'volume']]
dataset_job2['industry'] = dataset_job2['industry'].str.replace(',', '')
#%%
print(dataset_job1.columns)
#%%
print(dataset_job1.columns)
#merge_dataset.to_csv('dataset/merge_dataset.csv', index=False)
#%%
test_dataset_job1 = dataset_job1.head(10000)
test_dataset_job10 = dataset_job1.head(500)
test_dataset_job10.to_csv('dataset/test_dataset_job10.csv', index=False)
test_dataset_job20 = dataset_job2.head(500)
test_dataset_job20.to_csv('dataset/test_dataset_job20.csv', index=False)


#dataset_job1.to_csv('dataset/dataset_job1.csv', index=False)
#test_dataset_job1.to_csv('dataset/test_dataset_job1.csv', index=False)
#%%
test_dataset_job2 = dataset_job2.head(10000)
dataset_job2.to_csv('dataset/dataset_job2.csv', index=False)
#test_dataset_job2.to_csv('dataset/test_dataset_job2.csv', index=False)

#%%
print("ecco i valiri unicvoci")
print(merge_dataset["ticker"].value_counts().sum())
print(merge_dataset["industry"].value_counts().sum())
