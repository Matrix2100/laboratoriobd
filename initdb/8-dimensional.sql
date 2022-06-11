CREATE DATABASE dimensional;

\c dimensional;

CREATE TABLE IF NOT EXISTS dm_departamentos (
        id_dpto varchar NOT NULL PRIMARY KEY,
        nome_dpto varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dm_disciplinas (
        id_disc varchar  NOT NULL PRIMARY KEY,
        nome_disc varchar NOT NULL,
        carga_horaria varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dm_cursos (
        id_curso varchar NOT NULL PRIMARY KEY,
        nome_curso varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dm_tempo (
        id_tempo varchar NOT NULL PRIMARY KEY,
        semestre varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS ft_reprovacao_cotas (
        id_tempo varchar NOT NULL REFERENCES dm_tempo ON DELETE CASCADE,
        id_disc varchar NOT NULL REFERENCES dm_disciplinas ON DELETE CASCADE,
        total_reprovacoes_cotas varchar NOT NULL,
        total_matriculas_cotas varchar NOT NULL,
        total_reprovacoes varchar NOT NULL,
        total_matriculas varchar NOT NULL,
        PRIMARY KEY (id_tempo, id_disc)
);

CREATE TABLE IF NOT EXISTS ft_reprovacao (
        id_tempo varchar NOT NULL REFERENCES dm_tempo ON DELETE CASCADE,
        id_disc varchar NOT NULL REFERENCES dm_disciplinas ON DELETE CASCADE,
        id_curso varchar NOT NULL REFERENCES dm_cursos ON DELETE CASCADE,
        id_dpto varchar NOT NULL REFERENCES dm_departamentos ON DELETE CASCADE,
        total_reprovacoes varchar NOT NULL,
        total_matriculas varchar NOT NULL,
        PRIMARY KEY (id_tempo, id_disc, id_curso, id_dpto)
);