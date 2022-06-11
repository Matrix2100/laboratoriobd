import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job
import pandas as pd
import numpy as np

# getting the Job Name
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Creating Glue and Spark Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# catalog:  s3 output bucket
s3_write_bucket = "s3://un2-labdb/outfiles"

############################
#        EXTRACT           #
############################

# creating datasource using the catalog table
dsAlunos = glueContext.create_dynamic_frame.from_catalog(
    database="raw-data", table_name="operacional_public_alunos")
    
dsCursos = glueContext.create_dynamic_frame.from_catalog(
    database="raw-data", table_name="operacional_public_cursos")
    
dsDepartamentos = glueContext.create_dynamic_frame.from_catalog(
    database="raw-data", table_name="operacional_public_departamentos")
    
dsDisciplinas = glueContext.create_dynamic_frame.from_catalog(
    database="raw-data", table_name="operacional_public_disciplinas")
    
dsMatriculas = glueContext.create_dynamic_frame.from_catalog(
    database="raw-data", table_name="operacional_public_matriculas")

# converting from Glue DynamicFrame to Spark Dataframe and then to pandas
alunos = dsAlunos.toDF().toPandas()
cursos = dsCursos.toDF().toPandas()
departamentos = dsDepartamentos.toDF().toPandas()
disciplinas = dsDisciplinas.toDF().toPandas()
matriculas = dsMatriculas.toDF().toPandas()

############################
#        TRANSFORM         #
############################

"""# Transform
## dm_departamentos
"""

dm_departamentos = pd.DataFrame()
dm_departamentos['id_dpto'] = departamentos.cod_dpto
dm_departamentos['nome_dpto'] = departamentos.nome_dpto


"""## dm_cursos"""

dm_cursos = pd.DataFrame()
dm_cursos['id_curso'] = cursos.cod_curso
dm_cursos['nome_curso'] = cursos.nom_curso


"""## dm_disciplinas"""

dm_disciplinas = pd.DataFrame()
dm_disciplinas['id_disc'] = disciplinas.cod_disc
dm_disciplinas['nome_disc'] = disciplinas.nome_disc
dm_disciplinas['carga_horaria'] = disciplinas.carga_horaria


"""## dm_tempo"""

dm_tempo = pd.DataFrame(
    {
        'id_tempo': [
            semestre for semestre in matriculas.semestre.unique()
        ],
        'ano': [
            int(str(semestre)[:4]) for semestre in matriculas.semestre.unique()
        ],
        'semestre': [
            int(str(semestre)[4:]) for semestre in matriculas.semestre.unique()
        ],
        'semestre_str': [
            f"{str(semestre)[:4]}/{str(semestre)[4:]}" for semestre in matriculas.semestre.unique()
        ],
    }
)


"""## ft_reprovacao_cotas"""



df = pd.merge(left=matriculas, right=alunos, how='left', on='mat_alu')
df['id'] = df.semestre.astype(str) + df.cod_disc.astype(str)


df_cotistas = df[df['cotista'].isin(["S"])]


df_reprovados = df[df['status'].isin(["R"])]


df_cotistas_reprovados = df_cotistas[df_cotistas['status'].isin(["R"])]


df = df.groupby(['id']).count()[['mat_alu']]
df = df.reset_index()
df.rename(columns={'mat_alu': 'total_matriculas'}, inplace=True)


df_cotistas = df_cotistas.groupby(['id']).count()[['mat_alu']]
df_cotistas = df_cotistas.reset_index()
df_cotistas.rename(columns={'mat_alu': 'total_matriculas_cotas'}, inplace=True)


df_reprovados = df_reprovados.groupby(['id']).count()[['mat_alu']]
df_reprovados = df_reprovados.reset_index()
df_reprovados.rename(columns={'mat_alu': 'total_reprovações'}, inplace=True)


df_cotistas_reprovados = df_cotistas_reprovados.groupby(['id']).count()[['mat_alu']]
df_cotistas_reprovados = df_cotistas_reprovados.reset_index()
df_cotistas_reprovados.rename(columns={'mat_alu': 'total_reprovacoes_cotas'}, inplace=True)


ft_reprovacao_cotas = pd.merge(left=df, right=df_cotistas, how='left', on='id')


ft_reprovacao_cotas = pd.merge(left=ft_reprovacao_cotas, right=df_reprovados, how='left', on='id')


ft_reprovacao_cotas = pd.merge(left=ft_reprovacao_cotas, right=df_cotistas_reprovados, how='left', on='id')


ft_reprovacao_cotas['total_matriculas_cotas'] = ft_reprovacao_cotas['total_matriculas_cotas'].fillna(0).astype('Int64')
ft_reprovacao_cotas['total_reprovações'] = ft_reprovacao_cotas['total_reprovações'].fillna(0).astype('Int64')
ft_reprovacao_cotas['total_reprovacoes_cotas'] = ft_reprovacao_cotas['total_reprovacoes_cotas'].fillna(0).astype('Int64')


ft_reprovacao_cotas['id_tempo'] = (
    ft_reprovacao_cotas['id'].astype(str).str.slice(0, 5)
)
ft_reprovacao_cotas['id_disc'] = (
    ft_reprovacao_cotas['id'].astype(str).str.slice(5)
)
ft_reprovacao_cotas.drop(columns=['id'], inplace=True)


"""## ft_reprovação"""

df = pd.merge(left=matriculas, right=alunos, how='left', on='mat_alu')
df = pd.merge(left=df, right=cursos, how='left', on='cod_curso')
df

df['id'] = df.semestre.astype(str) + df.cod_disc.astype(str) + df.cod_curso.astype(str) + df.cod_dpto.astype(str)
df.drop(columns=['nota', 'faltas', 'nome', 'dat_entrada', 'cotista', 'nom_curso'], inplace=True)


df_reprovados = df[df['status'].isin(["R"])]


df = df.groupby(['id', 'semestre', 'cod_disc', 'cod_curso', 'cod_dpto']).count()[['mat_alu']]
df = df.reset_index()
df.rename(columns={'mat_alu': 'total_matriculas'}, inplace=True)


df_reprovados = df_reprovados.groupby(['id']).count()[['mat_alu']]
df_reprovados = df_reprovados.reset_index()
df_reprovados.rename(columns={'mat_alu': 'total_reprovações'}, inplace=True)


ft_reprovacao = df.rename(columns={'semestre': 'id_tempo', 'cod_disc': 'id_disc', 'cod_dpto': 'id_dpto', 'cod_curso': 'id_curso'})


ft_reprovacao = pd.merge(
    left=ft_reprovacao, right=df_reprovados, how='left', on='id')
ft_reprovacao['total_reprovações'] = ft_reprovacao['total_reprovações'].fillna(
    0).astype('Int64')
ft_reprovacao.drop(columns=['id'], inplace=True)


############################
#        LOAD              #
############################

#conversions
dm_departamentos_transform = DynamicFrame.fromDF(
    spark.createDataFrame(dm_departamentos), glueContext, 'dm_departamentos_transform')
    
dm_cursos_transform = DynamicFrame.fromDF(
    spark.createDataFrame(dm_cursos), glueContext, 'dm_cursos_transform')
    
dm_disciplinas_transform = DynamicFrame.fromDF(
    spark.createDataFrame(dm_disciplinas), glueContext, 'dm_disciplinas_transform')
    
dm_tempo_transform = DynamicFrame.fromDF(
    spark.createDataFrame(dm_tempo), glueContext, 'dm_tempo_transform')
    
ft_reprovacao_cotas_transform = DynamicFrame.fromDF(
    spark.createDataFrame(ft_reprovacao_cotas), glueContext, 'ft_reprovacao_cotas_transform')
    
ft_reprovacao_transform = DynamicFrame.fromDF(
    spark.createDataFrame(ft_reprovacao), glueContext, 'ft_reprovacao_transform')

# conection settings
connection_options = {
    "url": "jdbc:postgresql://database-1.cgqqyrx9bwnj.us-east-1.rds.amazonaws.com:5432/dimensional",
    "user": "postgres", 
    "password": "12345678",
}

#real loads
dm_departamentos_transform = glueContext.write_dynamic_frame.from_options(
    frame=dm_departamentos_transform,
    connection_type="postgresql", 
    connection_options={**connection_options, **{"dbtable": "public.dm_departamentos"}},
    transformation_ctx="sample_dynF"
)

dm_cursos_transform = glueContext.write_dynamic_frame.from_options(
    frame=dm_cursos_transform,
    connection_type="postgresql", 
    connection_options={**connection_options, **{"dbtable": "public.dm_cursos"}},
    transformation_ctx="sample_dynF"
)

dm_disciplinas_transform = glueContext.write_dynamic_frame.from_options(
    frame=dm_disciplinas_transform,
    connection_type="postgresql", 
    connection_options={**connection_options, **{"dbtable": "public.dm_disciplinas"}},
    transformation_ctx="sample_dynF"
)

dm_tempo_transform = glueContext.write_dynamic_frame.from_options(
    frame=dm_tempo_transform,
    connection_type="postgresql", 
    connection_options={**connection_options, **{"dbtable": "public.dm_tempo"}},
    transformation_ctx="sample_dynF"
)

ft_reprovacao_cotas_transform = glueContext.write_dynamic_frame.from_options(
    frame=ft_reprovacao_cotas_transform,
    connection_type="postgresql", 
    connection_options={**connection_options, **{"dbtable": "public.ft_reprovacao_cotas"}},
    transformation_ctx="sample_dynF"
)

ft_reprovacao_transform = glueContext.write_dynamic_frame.from_options(
    frame=ft_reprovacao_transform,
    connection_type="postgresql", 
    connection_options={**connection_options, **{"dbtable": "public.ft_reprovacao"}},
    transformation_ctx="sample_dynF"
)
                         
job.commit()
