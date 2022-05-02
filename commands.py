# Renomear varias colunas ao mesmo tempo. PySpark.
a_dict = {'antigo_1': 'novo_1', 'antigo_2': 'novo_2'}

for key, value in a_dict.items():
    df= df.withColumnRenamed(value, key)
