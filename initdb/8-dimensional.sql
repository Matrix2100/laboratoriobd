CREATE DATABASE dimensional;

\c dimensional;

CREATE TABLE dm_departamento (
        id_dpto varchar NOT NULL PRIMARY KEY,
        nom_dpto varchar NOT NULL
);

CREATE TABLE dm_disciplina (
        id_disc varchar  NOT NULL PRIMARY KEY,
        nome_disc varchar NOT NULL,
        carga_horaria varchar NOT NULL
);

CREATE TABLE dm_curso (
        id_curso varchar NOT NULL PRIMARY KEY,
        nom_curso varchar NOT NULL
);

CREATE TABLE dm_tempo (
        id_tempo varchar NOT NULL PRIMARY KEY,
        semestre varchar NOT NULL
);

CREATE TABLE ft_reprovacao_cotas (
        id_tempo varchar NOT NULL REFERENCES dm_tempo,
        id_disc varchar NOT NULL REFERENCES dm_disciplina,
        total_rep_cot_disc varchar NOT NULL,
        total_mat_alu_cot varchar NOT NULL,
        total_reprovados varchar NOT NULL,
        total_mat varchar NOT NULL,
        PRIMARY KEY (id_tempo, id_disc)
);

CREATE TABLE ft_reprovacao (
        id_tempo varchar NOT NULL REFERENCES dm_tempo,
        id_disc varchar NOT NULL REFERENCES dm_disciplina,
        id_curso varchar NOT NULL REFERENCES dm_curso,
        id_dpto varchar NOT NULL REFERENCES dm_departamento,
        total_rep_disc varchar NOT NULL,
        total_mat_alu varchar NOT NULL,
        PRIMARY KEY (id_tempo, id_disc, id_curso, id_dpto)
);