create table public.dm_departamentos
(
    id_dpto   bigint,
    nome_dpto text
);

alter table public.dm_departamentos
    owner to postgres;

create table public.dm_cursos
(
    id_curso   bigint,
    nome_curso text
);

alter table public.dm_cursos
    owner to postgres;

create table public.dm_disciplinas
(
    id_disc       bigint,
    nome_disc     text,
    carga_horaria numeric(38, 18)
);

alter table public.dm_disciplinas
    owner to postgres;

create table public.dm_tempo
(
    id_tempo     bigint,
    ano          bigint,
    semestre     bigint,
    semestre_str text
);

alter table public.dm_tempo
    owner to postgres;

create table public.ft_reprovacao_cotas
(
    total_matriculas        bigint,
    total_matriculas_cotas  bigint,
    total_reprovações       bigint,
    total_reprovacoes_cotas bigint,
    id_tempo                text,
    id_disc                 text
);

alter table public.ft_reprovacao_cotas
    owner to postgres;

create table public.ft_reprovacao
(
    id_tempo          bigint,
    id_disc           bigint,
    id_curso          bigint,
    id_dpto           bigint,
    total_matriculas  bigint,
    total_reprovações bigint
);

alter table public.ft_reprovacao
    owner to postgres;

