# Imports
import sqlalchemy
import pandas as pd

# Criação da engine do sql alchemy para o banco operacional.
db_connection_in = sqlalchemy.create_engine(
    'postgresql+pg8000://postgres:123456@localhost:5433/operacional',
    client_encoding='utf8',
)

departamentos = pd.read_sql('SELECT * FROM departamentos', db_connection_in)

cursos = pd.read_sql('SELECT * FROM cursos', db_connection_in)

disciplinas = pd.read_sql('SELECT * FROM disciplinas', db_connection_in)

matrizes_cursos = pd.read_sql('SELECT * FROM matrizes_cursos', db_connection_in)

matriculas = pd.read_sql('SELECT * FROM matriculas', db_connection_in)

alunos = pd.read_sql('SELECT * FROM alunos', db_connection_in)

dm_departamentos = pd.DataFrame()
dm_departamentos['id_dpto'] = departamentos.cod_dpto
dm_departamentos['nome_dpto'] = departamentos.nome_dpto

dm_cursos = pd.DataFrame()
dm_cursos['id_curso'] = cursos.cod_curso
dm_cursos['nome_curso'] = cursos.nom_curso

dm_disciplinas = pd.DataFrame()
dm_disciplinas['id_disc'] = disciplinas.cod_disc
dm_disciplinas['nome_disc'] = disciplinas.nome_disc
dm_disciplinas['carga_horaria'] = disciplinas.carga_horaria

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