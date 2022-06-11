CREATE TABLE  IF NOT EXISTS departamentos (
    cod_dpto    INT NOT NULL PRIMARY KEY,
    nome_dpto   VARCHAR(50) NOT NULL
);

CREATE TABLE  IF NOT EXISTS cursos (
    cod_curso   INT NOT NULL PRIMARY KEY,
    nom_curso   VARCHAR(80) NOT NULL,
    cod_dpto    INT NOT NULL REFERENCES departamentos
);

CREATE TABLE  IF NOT EXISTS alunos (
    mat_alu       INT NOT NULL PRIMARY KEY,
    nome          VARCHAR(100) NOT NULL,
    dat_entrada   DATE NOT NULL,
    cod_curso     INT NOT NULL REFERENCES cursos,
    cotista       CHAR(1) NOT NULL
);

CREATE TABLE  IF NOT EXISTS disciplinas (
    cod_disc        INT NOT NULL PRIMARY KEY,
    nome_disc       VARCHAR(60) NOT NULL,
    carga_horaria   NUMERIC NOT NULL
);

CREATE TABLE  IF NOT EXISTS matriculas (
    semestre   INT NOT NULL,
    mat_alu    INT NOT NULL REFERENCES alunos,
    cod_disc   INT NOT NULL REFERENCES disciplinas,
    nota       NUMERIC,
    faltas     INT,
    status     CHAR(1),
  	PRIMARY KEY (mat_alu, semestre)
);

CREATE TABLE  IF NOT EXISTS matrizes_cursos (
    cod_curso   INT NOT NULL REFERENCES cursos,
    cod_disc    INT NOT NULL REFERENCES disciplinas,
    periodo     INT NOT NULL,
    PRIMARY KEY (cod_curso, cod_disc)
);
