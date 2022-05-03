# Renomear varias colunas ao mesmo tempo. PySpark.
a_dict = {'antigo_1': 'novo_1', 'antigo_2': 'novo_2'}

for key, value in a_dict.items():
    df= df.withColumnRenamed(value, key)

# COPIAR TABELA
# modo_1
df_aux = tabela_original

# modo_2
copy_in_stats = in_stats.select('*')

# REORDENAR AS COLUNAS (PYSPARK)
# modo_1 - ordem manual
df_reordered = df.select("col_A","col_C","col_B")
df_basket_reordered.show()

#modo_2 - ordem decresente
df_reordered = df.select(sorted(df.columns))
df_reordered.show()
