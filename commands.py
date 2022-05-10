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

# FILTRAR DF POR DATA
dadosE_copia_editado = dadosE_copia.filter(dadosE_copia('ds').gt(lit('2016-01-20'))) #filtrar datas maiores que
dadosE_copia_editado = dadosE_copia.filter(dadosE_copia('ds').lt(lit('2016-01-20'))) #filtrar datas menores que
dadosE_copia_editado = dadosE_copia.filter(dadosE_copia('ds') === lit('2016-01-20')) #filtrar data iguais a

## COMANDO PARA PEGAR O VALOR MÁXIMO DE UMA VARIAVEL
display(df.groupby('company').agg({'ds':'max'}))
