CREATE TABLE alunos (
    mat_alu       NUMBER(10) NOT NULL,
    nome          VARCHAR2(100) NOT NULL,
    dat_entrada   DATE NOT NULL,
    cod_curso     NUMBER(3) NOT NULL,
    cotista       CHAR(1) NOT NULL
);

ALTER TABLE alunos ADD CONSTRAINT alu_fk PRIMARY KEY ( mat_alu );

CREATE TABLE cursos (
    cod_curso   NUMBER(4) NOT NULL,
    nom_curso   VARCHAR2(80) NOT NULL,
    cod_dpto    NUMBER(3) NOT NULL
);

ALTER TABLE cursos ADD CONSTRAINT cur_pk PRIMARY KEY ( cod_curso );

CREATE TABLE departamentos (
    cod_dpto    NUMBER(3) NOT NULL,
    nome_dpto   VARCHAR2(50) NOT NULL
);

ALTER TABLE departamentos ADD CONSTRAINT departamentos_pk PRIMARY KEY ( cod_dpto );

CREATE TABLE disciplinas (
    cod_disc        NUMBER(5) NOT NULL,
    nome_disc       VARCHAR2(60) NOT NULL,
    carga_horaria   NUMBER(5, 2) NOT NULL
);

ALTER TABLE disciplinas ADD CONSTRAINT disc_pk PRIMARY KEY ( cod_disc );

CREATE TABLE matriculas (
    semestre   NUMBER(6) NOT NULL,
    mat_alu    NUMBER(10) NOT NULL,
    cod_disc   NUMBER(5) NOT NULL,
    nota       NUMBER(5, 2),
    faltas     NUMBER(3),
    status     CHAR(1)
);

ALTER TABLE matriculas ADD CONSTRAINT mat_pk PRIMARY KEY ( mat_alu,
                                                           semestre );

CREATE TABLE matrizes_cursos (
    cod_curso   NUMBER(4) NOT NULL,
    cod_disc    NUMBER(5) NOT NULL,
    periodo     NUMBER(2) NOT NULL
);

ALTER TABLE matrizes_cursos ADD CONSTRAINT mcu_pk PRIMARY KEY ( cod_curso,
                                                                cod_disc );

ALTER TABLE alunos
    ADD CONSTRAINT alu_cur_fk FOREIGN KEY ( cod_curso )
        REFERENCES cursos ( cod_curso );

ALTER TABLE cursos
    ADD CONSTRAINT cur_der_fk FOREIGN KEY ( cod_dpto )
        REFERENCES departamentos ( cod_dpto );

ALTER TABLE matriculas
    ADD CONSTRAINT mat_alu_fk FOREIGN KEY ( mat_alu )
        REFERENCES alunos ( mat_alu );

ALTER TABLE matriculas
    ADD CONSTRAINT mat_dis_fk FOREIGN KEY ( cod_disc )
        REFERENCES disciplinas ( cod_disc );

ALTER TABLE matrizes_cursos
    ADD CONSTRAINT mcu_cur_fk FOREIGN KEY ( cod_curso )
        REFERENCES cursos ( cod_curso );

ALTER TABLE matrizes_cursos
    ADD CONSTRAINT mcu_dis_fk FOREIGN KEY ( cod_disc )
        REFERENCES disciplinas ( cod_disc );

