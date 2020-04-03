CREATE OR REPLACE PACKAGE APPS.TKT_ATENDIMENTO_PKG IS

/*
+===========================================================================+
|                                                                           |
|   Apoio na sustentação para atendimento de cadastro                       |
|   de clientes e estabelecimentos.                                         |
|                                                                           |
| NOTES                                                                     |
|   Created by      Tiago Rodrigues     15/08/2019                          |
+===========================================================================+
*/

  PROCEDURE TKT_ATENDIMENTO_MSG (P_TIPO                 IN     VARCHAR2
                                ,P_CHAMADO              IN     VARCHAR2);
--
  PROCEDURE TKT_ATENDIMENTO_MSG (P_TIPO                 IN     VARCHAR2
                                ,P_CORRELATION          IN     NUMBER
                                ,P_ORIGEM               IN     VARCHAR2);
--
  PROCEDURE TKT_ATENDIMENTO_PROC_CONTRATO (P_CHAMADO              IN     VARCHAR2);
--
  PROCEDURE TKT_ATENDIMENTO_PROC_CONTRATO (P_CORRELATION          IN     NUMBER
                                          ,P_ORIGEM               IN     VARCHAR2);
--
  PROCEDURE TKT_ATENDIMENTO_REQUEST_BUREAU (P_CHAMADO              IN     VARCHAR2);
--
  PROCEDURE TKT_ATENDIMENTO_REQUEST_BUREAU (P_CORRELATION          IN     NUMBER
                                           ,P_ORIGEM               IN     VARCHAR2);
--
  PROCEDURE TKT_ATENDIMENTO (ERRBUF                 OUT    VARCHAR2
                            ,RETCODE                OUT    NUMBER
                            ,P_OPCAO                IN     NUMBER
                            ,P_CORRELATION          IN     NUMBER
                            ,P_ORIGEM               IN     VARCHAR2
                            ,P_TIPO                 IN     VARCHAR2
                            ,P_CHAMADO              IN     VARCHAR2);                         
                         
--
END TKT_ATENDIMENTO_PKG;


CREATE OR REPLACE PACKAGE BODY APPS.TKT_ATENDIMENTO_PKG AS
/*
+===========================================================================+
|                                                                           |
|   Apoio na sustentação para atendimento de cadastro                       |
|   de clientes e estabelecimentos.                                         |
|                                                                           |
| NOTES                                                                     |
|   Created by      Tiago Rodrigues     15/08/2019                          |
+===========================================================================+
*/


-- PUBLICAR MENSAGEM NA FILA AQ COM APOIO DA TABELA TEMPORÁRIA
PROCEDURE TKT_ATENDIMENTO_MSG (P_TIPO                 IN     VARCHAR2
                              ,P_CHAMADO              IN     VARCHAR2) 
IS
  CURSOR C IS
    SELECT VALOR1 CORRELATION,
           SISTEMA_PEDIDO ORIGEM
      FROM APPS.TKT_CONTR_CLI_PEDIDO
     WHERE COMENTARIO = P_CHAMADO;
   BEGIN
    FOR R IN C LOOP
      CASE P_TIPO
           WHEN 'CLIENTE'          THEN APPS.TKT_AR_BACEN_PKG.PROCESSAR_MENSAGEM (P_SISTEMA_ORIGEM => R.ORIGEM, P_CLIENTE_ID => R.CORRELATION);
           WHEN 'ESTABELECIMENTO'  THEN APPS.TKT_AR_BACEN_PKG.PROCESSAR_MENSAGEM_ESTAB (P_SISTEMA_ORIGEM => R.ORIGEM, P_ESTABELECIMENTO_ID => R.CORRELATION);
           WHEN 'CONTRATO'         THEN APPS.TKT_OKC_BACEN_PKG.PROCESSAR_MENSAGEM (P_SISTEMA_ORIGEM => R.ORIGEM, P_CONTRATO_ID => R.CORRELATION);
      END CASE;
    END LOOP;
   COMMIT;
END;
--
-- PUBLICAR MENSAGEM NA FILA AQ AVULSO
PROCEDURE TKT_ATENDIMENTO_MSG (P_TIPO                 IN     VARCHAR2
                              ,P_CORRELATION          IN     NUMBER
                              ,P_ORIGEM               IN     VARCHAR2)
IS
  BEGIN
    CASE P_TIPO
      WHEN 'CLIENTE'          THEN APPS.TKT_AR_BACEN_PKG.PROCESSAR_MENSAGEM (P_SISTEMA_ORIGEM => P_ORIGEM, P_CLIENTE_ID => P_CORRELATION);
      WHEN 'ESTABELECIMENTO'  THEN APPS.TKT_AR_BACEN_PKG.PROCESSAR_MENSAGEM_ESTAB (P_SISTEMA_ORIGEM => P_ORIGEM, P_ESTABELECIMENTO_ID => P_CORRELATION);
      WHEN 'CONTRATO'         THEN APPS.TKT_OKC_BACEN_PKG.PROCESSAR_MENSAGEM (P_SISTEMA_ORIGEM => P_ORIGEM, P_CONTRATO_ID => P_CORRELATION);
    END CASE;
    COMMIT;
END;

-- PROCESSAR CONTRATO COM APOIO DA TABELA TEMPORÁRIA
PROCEDURE TKT_ATENDIMENTO_PROC_CONTRATO (P_CHAMADO              IN     VARCHAR2) 

IS
  CURSOR C IS
    SELECT VALOR1 CORRELATION,
           SISTEMA_PEDIDO ORIGEM
      FROM APPS.TKT_CONTR_CLI_PEDIDO
     WHERE COMENTARIO = P_CHAMADO;
   BEGIN
    FOR R IN C LOOP
      IF R.ORIGEM IN ('TEP', 'TICKETSHOP') THEN
        APPS.TKT_OKC_BACEN_PKG.PROCESSAR_TEP_CONTRATO(P_SISTEMA_ORIGEM => R.ORIGEM,
                                                      P_CONTRATO_ID    => R.CORRELATION);
      ELSIF R.ORIGEM IN ('SIMULADOR', 'SALESFORCE') THEN
        APPS.TKT_OKC_BACEN_PKG.PROCESSAR_CN_CONTRATO(P_SISTEMA_ORIGEM => R.ORIGEM,
                                                     P_CONTRATO_ID    => R.CORRELATION);
      ELSIF R.ORIGEM = 'NGAT' THEN
        APPS.TKT_OKC_BACEN_PKG.PROCESSAR_NGAT_CONTRATO(P_SISTEMA_ORIGEM => R.ORIGEM,
                                                       P_CONTRATO_ID    => R.CORRELATION);
      END IF;
    END LOOP;
  COMMIT;
END;

-- PROCESSAR CONTRATO AVULSO
PROCEDURE TKT_ATENDIMENTO_PROC_CONTRATO (P_CORRELATION          IN     NUMBER
                                        ,P_ORIGEM               IN     VARCHAR2) 
IS
  BEGIN
    CASE
      WHEN P_ORIGEM = 'TEP' AND P_ORIGEM = 'TICKETSHOP' THEN APPS.TKT_OKC_BACEN_PKG.PROCESSAR_TEP_CONTRATO(P_SISTEMA_ORIGEM => P_ORIGEM, P_CONTRATO_ID => P_CORRELATION);
      WHEN P_ORIGEM = 'SIMULADOR' AND P_ORIGEM = 'SALESFORCE' THEN APPS.TKT_OKC_BACEN_PKG.PROCESSAR_CN_CONTRATO(P_SISTEMA_ORIGEM => P_ORIGEM, P_CONTRATO_ID => P_CORRELATION);
      WHEN P_ORIGEM = 'NGAT' THEN APPS.TKT_OKC_BACEN_PKG.PROCESSAR_NGAT_CONTRATO(P_SISTEMA_ORIGEM => P_ORIGEM, P_CONTRATO_ID => P_CORRELATION);
    END CASE;
    COMMIT;
END;

-- REQUEST COM APOIO DA TABELA TEMPORÁRIA
PROCEDURE TKT_ATENDIMENTO_REQUEST_BUREAU (P_CHAMADO              IN     VARCHAR2) 

IS

  CURSOR CUR_CLI_CPF IS
        SELECT CNPJ CPF, A.ORG_ID ORG, MAX(REQUEST_ID) REQUEST
        FROM TKT.TKT_BACEN_CLI_ALL A
        WHERE A.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND A.TIPO_INSCR = 1
          AND A.CLIENTE_ID IN (SELECT VALOR1
                               FROM APPS.TKT_CONTR_CLI_PEDIDO B
                               WHERE COMENTARIO = P_CHAMADO)
  GROUP BY CNPJ, A.ORG_ID;
  --
  CURSOR CUR_CLI_CNPJ IS
        SELECT CNPJ CNPJ, A.ORG_ID ORG, MAX(REQUEST_ID) REQUEST
        FROM TKT.TKT_BACEN_CLI_ALL A
        WHERE A.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND A.TIPO_INSCR = 2
          AND A.CLIENTE_ID IN (SELECT VALOR1
                               FROM APPS.TKT_CONTR_CLI_PEDIDO B
                               WHERE COMENTARIO = P_CHAMADO)
  GROUP BY CNPJ, A.ORG_ID;
  --
  CURSOR CUR_ESTAB IS
       SELECT CNPJ CNPJ, A.ORG_ID ORG, MAX(REQUEST_ID) REQUEST
        FROM TKT.TKT_BACEN_ESTAB_ALL A
        WHERE A.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND A.ESTABELECIMENTO_ID IN (SELECT VALOR1
                                       FROM APPS.TKT_CONTR_CLI_PEDIDO B
                                       WHERE COMENTARIO = P_CHAMADO)
  GROUP BY CNPJ, A.ORG_ID;

  L_NREQUESTID NUMBER;
  L_NREQUESTID NUMBER;

  R_CURSOR_C_CPF   CUR_CLI_CPF%ROWTYPE;
  R_CURSOR_C_CNPJ  CUR_CLI_CNPJ%ROWTYPE;
  R_CURSOR_ESTAB   CUR_ESTAB%ROWTYPE;

  BEGIN
    OPEN CUR_CLI_CPF;
    LOOP
      FETCH CUR_CLI_CPF
        INTO R_CURSOR_C_CPF;
      EXIT WHEN CUR_CLI_CPF%NOTFOUND;

      APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE      => 'ERP'
                                                 , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                 , P_ORG_ID             => R_CURSOR_C_CPF.ORG
                                                 , P_TYPE               => 'C'
                                                 , P_THIRD_PARTY_TYPE   => 'CPF'
                                                 , P_THIRD_PARTY_NUMBER => R_CURSOR_C_CPF.CPF
                                                 , P_THIRD_PARTY_ID     => 1
                                                 , P_PERSON_BIRTH_DATE  => '29-OUT-1969'
                                                 , P_REQUEST_ID         => R_CURSOR_C_CPF.REQUEST
                                                 );

      --
      DBMS_OUTPUT.PUT_LINE('R_CURSOR_C_CPF.REQUEST: ' ||
                           R_CURSOR_C_CPF.REQUEST);
    END LOOP;
    CLOSE CUR_CLI_CPF;

    OPEN CUR_CLI_CNPJ;
    LOOP
      FETCH CUR_CLI_CNPJ
        INTO R_CURSOR_C_CNPJ;
      EXIT WHEN CUR_CLI_CNPJ%NOTFOUND;

      APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE      => 'ERP'
                                                   , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                   , P_ORG_ID             => R_CURSOR_C_CNPJ.ORG
                                                   , P_TYPE               => 'C'
                                                   , P_THIRD_PARTY_TYPE   => 'CNPJ'
                                                   , P_THIRD_PARTY_NUMBER => R_CURSOR_C_CNPJ.CNPJ
                                                   , P_THIRD_PARTY_ID     => 1
                                                   , P_REQUEST_ID         => R_CURSOR_C_CNPJ.REQUEST
                                                   );

      --
      DBMS_OUTPUT.PUT_LINE('R_CURSOR_C_CNPJ.REQUEST: ' || R_CURSOR_C_CNPJ.REQUEST);
    END LOOP;
    CLOSE CUR_CLI_CNPJ;

    OPEN CUR_ESTAB;
    LOOP
      FETCH CUR_ESTAB
        INTO R_CURSOR_ESTAB;
      EXIT WHEN CUR_ESTAB%NOTFOUND;

        APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE      => 'ERP'
                                                   , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                   , P_ORG_ID             => R_CURSOR_ESTAB.ORG
                                                   , P_TYPE               => 'C'
                                                   , P_THIRD_PARTY_TYPE   => 'CNPJ'
                                                   , P_THIRD_PARTY_NUMBER => R_CURSOR_ESTAB.CNPJ
                                                   , P_THIRD_PARTY_ID     => 1
                                                   , P_REQUEST_ID         => R_CURSOR_ESTAB.REQUEST
                                                   );

      --
      DBMS_OUTPUT.PUT_LINE('R_CURSOR_ESTAB.REQUEST: ' || R_CURSOR_ESTAB.REQUEST);
    END LOOP;
    CLOSE CUR_ESTAB;
  COMMIT;
END;

-- REQUEST AVULSO
PROCEDURE TKT_ATENDIMENTO_REQUEST_BUREAU (P_CORRELATION          IN     NUMBER
                                         ,P_ORIGEM               IN     VARCHAR2) 
IS

  CURSOR CUR_CLI_CPF IS
        SELECT CNPJ CPF, A.ORG_ID ORG, MAX(REQUEST_ID) REQUEST
        FROM TKT.TKT_BACEN_CLI_ALL A
        WHERE A.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND A.TIPO_INSCR = 1
          AND A.SISTEMA_ORIGEM = P_ORIGEM
          AND A.CLIENTE_ID = P_CORRELATION
  GROUP BY CNPJ, A.ORG_ID;
  --
  CURSOR CUR_CLI_CNPJ IS
        SELECT CNPJ CNPJ, A.ORG_ID ORG, MAX(REQUEST_ID) REQUEST
        FROM TKT.TKT_BACEN_CLI_ALL A
        WHERE A.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND A.TIPO_INSCR = 2
          AND A.SISTEMA_ORIGEM = P_ORIGEM
          AND A.CLIENTE_ID = P_CORRELATION
  GROUP BY CNPJ, A.ORG_ID;
  --
  CURSOR CUR_ESTAB IS
       SELECT CNPJ CNPJ, A.ORG_ID ORG, MAX(REQUEST_ID) REQUEST
        FROM TKT.TKT_BACEN_ESTAB_ALL A
        WHERE A.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND A.SISTEMA_ORIGEM = P_ORIGEM
          AND A.ESTABELECIMENTO_ID = P_CORRELATION
  GROUP BY CNPJ, A.ORG_ID;

  L_NREQUESTID NUMBER;
  L_NREQUESTID NUMBER;

  R_CURSOR_C_CPF   CUR_CLI_CPF%ROWTYPE;
  R_CURSOR_C_CNPJ  CUR_CLI_CNPJ%ROWTYPE;
  R_CURSOR_ESTAB   CUR_ESTAB%ROWTYPE;

  BEGIN
    OPEN CUR_CLI_CPF;
    LOOP
      FETCH CUR_CLI_CPF
        INTO R_CURSOR_C_CPF;
      EXIT WHEN CUR_CLI_CPF%NOTFOUND;

      APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE      => 'ERP'
                                                 , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                 , P_ORG_ID             => R_CURSOR_C_CPF.ORG
                                                 , P_TYPE               => 'C'
                                                 , P_THIRD_PARTY_TYPE   => 'CPF'
                                                 , P_THIRD_PARTY_NUMBER => R_CURSOR_C_CPF.CPF
                                                 , P_THIRD_PARTY_ID     => 1
                                                 , P_PERSON_BIRTH_DATE  => '29-OUT-1969'
                                                 , P_REQUEST_ID         => R_CURSOR_C_CPF.REQUEST
                                                 );

      --
      DBMS_OUTPUT.PUT_LINE('R_CURSOR_C_CPF.REQUEST: ' ||
                           R_CURSOR_C_CPF.REQUEST);
    END LOOP;
    CLOSE CUR_CLI_CPF;

    OPEN CUR_CLI_CNPJ;
    LOOP
      FETCH CUR_CLI_CNPJ
        INTO R_CURSOR_C_CNPJ;
      EXIT WHEN CUR_CLI_CNPJ%NOTFOUND;

      APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE      => 'ERP'
                                                   , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                   , P_ORG_ID             => R_CURSOR_C_CNPJ.ORG
                                                   , P_TYPE               => 'C'
                                                   , P_THIRD_PARTY_TYPE   => 'CNPJ'
                                                   , P_THIRD_PARTY_NUMBER => R_CURSOR_C_CNPJ.CNPJ
                                                   , P_THIRD_PARTY_ID     => 1
                                                   , P_REQUEST_ID         => R_CURSOR_C_CNPJ.REQUEST
                                                   );

      --
      DBMS_OUTPUT.PUT_LINE('R_CURSOR_C_CNPJ.REQUEST: ' || R_CURSOR_C_CNPJ.REQUEST);
    END LOOP;
    CLOSE CUR_CLI_CNPJ;

    OPEN CUR_ESTAB;
    LOOP
      FETCH CUR_ESTAB
        INTO R_CURSOR_ESTAB;
      EXIT WHEN CUR_ESTAB%NOTFOUND;

        APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE      => 'ERP'
                                                   , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                   , P_ORG_ID             => R_CURSOR_ESTAB.ORG
                                                   , P_TYPE               => 'C'
                                                   , P_THIRD_PARTY_TYPE   => 'CNPJ'
                                                   , P_THIRD_PARTY_NUMBER => R_CURSOR_ESTAB.CNPJ
                                                   , P_THIRD_PARTY_ID     => 1
                                                   , P_REQUEST_ID         => R_CURSOR_ESTAB.REQUEST
                                                   );

      --
      DBMS_OUTPUT.PUT_LINE('R_CURSOR_ESTAB.REQUEST: ' || R_CURSOR_ESTAB.REQUEST);
    END LOOP;
    CLOSE CUR_ESTAB;
  COMMIT;
END;
--
PROCEDURE TKT_ATENDIMENTO ( ERRBUF                 OUT    VARCHAR2
                           ,RETCODE                OUT    NUMBER
                           ,P_OPCAO                IN     NUMBER
                           ,P_CORRELATION          IN     NUMBER
                           ,P_ORIGEM               IN     VARCHAR2
                           ,P_TIPO                 IN     VARCHAR2
                           ,P_CHAMADO              IN     VARCHAR2) 
IS

  BEGIN

    --P_OPCAO = 0 MSG FILA AQ CLIENTE / ESTABELECIMENTO / CONTRATO / FATURA
    --P_OPCAO = 1 PROCESSAR CONTRATO TEP / TICKETSHOP / SIMULADOR / SALESFORCE / NGAT
    --P_OPCAO = 2 REQUEST BUREAU

    --
    IF P_OPCAO IN (0) THEN
      IF P_CHAMADO IS NULL THEN
         TKT_ATENDIMENTO_MSG(P_TIPO, P_CORRELATION, P_ORIGEM);
      ELSE
         TKT_ATENDIMENTO_MSG(P_TIPO, P_CHAMADO);
      END IF;
    END IF;
    --
    IF P_OPCAO IN (1) THEN
      IF P_CHAMADO IS NULL THEN
         TKT_ATENDIMENTO_PROC_CONTRATO (P_CORRELATION, P_ORIGEM);
      ELSE
         TKT_ATENDIMENTO_PROC_CONTRATO (P_CHAMADO);
      END IF;
    END IF;
    --
    IF P_OPCAO IN (2) THEN
      IF P_CHAMADO IS NULL THEN
         TKT_ATENDIMENTO_REQUEST_BUREAU (P_CORRELATION, P_ORIGEM);
      ELSE
         TKT_ATENDIMENTO_REQUEST_BUREAU (P_CHAMADO);
      END IF;
    END IF;
    --
END;
--

END TKT_ATENDIMENTO_PKG;
--NAO REMOVER INDICADOR DE FINAL DE ARQUIVO

