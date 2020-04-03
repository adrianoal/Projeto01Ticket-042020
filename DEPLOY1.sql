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
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+-- Autor: Adriano Lima                                                                                   	   				 	 --+\\+--
--+//+-- Data: 06/01/2020                                                                                 			   				 --+\\+--
--+//+-- Objetivo: Enviar solicitação em massa dos casos que por algum motivo nao retornaram para o ERP 		 					 --+\\+--
--+//+-- Atualização: 06/03/2020                                                                         			   				 --+\\+--
--+//+-- versao V_1                                                                                                    				 --+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--							
                            
  PROCEDURE PROC_SEM_RET_CNPJ;

  PROCEDURE PROC_SEM_RET_CPF;

  PROCEDURE PROC_SEM_RET_EST;	                            
                          
--
END TKT_ATENDIMENTO_PKG;
/

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
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+-- Autor: Adriano Lima                                                                                   	   				 	 --+\\+--
--+//+-- Data: 06/01/2020                                                                                 			   				 --+\\+--
--+//+-- Objetivo: Enviar solicitação em massa dos casos que por algum motivo nao retornaram para o ERP 		 					 --+\\+--
--+//+-- Atualização: 06/03/2020                                                                         			   				 --+\\+--
--+//+-- versao V_1                                                                                                    				 --+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
--+//+---------------------------------------------------------------------------------------------------------------------------------+\\+--
PROCEDURE PROC_SEM_RET_CNPJ AS
    BEGIN
     DECLARE

          VCOUNT_CLI_SEM INT;
          CHECK_VER1 INT := 1;
          CHECK_FAL1 INT := 0;
          G_COUNT NUMBER := 0;
          G_LIMIT NUMBER;
          

          CURSOR CUR_BUREAU IS
            SELECT /*+ ALL_ROWS*/
                 THRT.STATUS_HIGIENIZACAO,
                 THRT.REQUEST_ID AS REQUEST,
                 THRT.THIRD_PARTY_NUMBER
            FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT
            WHERE 1=1
            AND TRUNC(THRT.REQUEST_DATE) >= TRUNC(SYSDATE-7)
            AND THRT.SUBSYSTEM_SOURCE !=  'REPOM'
            AND THRT.THIRD_PARTY_TYPE = 'CNPJ'
            AND THRT.STATUS_HIGIENIZACAO IS NULL
            AND THRT.STATUS_RECEITA_FEDERAL IS NULL
            AND THRT.REQUEST_DATE IN (SELECT MAX(THRT1.REQUEST_DATE) FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT1 WHERE THRT1.THIRD_PARTY_NUMBER = THRT.THIRD_PARTY_NUMBER);

          VCUR_BUREAU CUR_BUREAU%ROWTYPE;

          CURSOR CUR_CNPJ IS
            SELECT /*+ FIRST_ROWS(4)*/
                 BCA.CLIENTE_ID,
                 BCA.PROGRAM_UPDATE_DATE,
                 BCA.STATUS_PROCESSAMENTO,
                 BCA.ORG_ID AS ORG,
                 BCA.CNPJ
            FROM APPS.TKT_BACEN_CLI_ALL BCA
            WHERE 1=1
            AND BCA.CNPJ = VCUR_BUREAU.THIRD_PARTY_NUMBER
            AND TRUNC(BCA.LAST_UPDATE_DATE) >= TRUNC(SYSDATE-7)
            AND BCA.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
            AND ((TRUNC(BCA.PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (BCA.PROGRAM_UPDATE_DATE IS NULL))
            AND BCA.SISTEMA_ORIGEM !=  'REPOM'
            AND BCA.TIPO_INSCR = 2
            AND BCA.CLIENTE_ID IN(SELECT MAX(BCA1.CLIENTE_ID)FROM APPS.TKT_BACEN_CLI_ALL BCA1 WHERE 1 = 1 AND BCA1.STATUS_PROCESSAMENTO = BCA.STATUS_PROCESSAMENTO AND BCA1.SISTEMA_ORIGEM = BCA.SISTEMA_ORIGEM AND BCA1.LAST_UPDATE_DATE = BCA1.LAST_UPDATE_DATE AND BCA1.CNPJ = BCA.CNPJ);

          VCUR_CNPJ CUR_CNPJ%ROWTYPE;

      BEGIN
		  SELECT VALOR40 INTO G_LIMIT FROM APPS.TKT_CONTR_CLI_PEDIDO WHERE VALOR40 IS NOT NULL;
          SELECT COUNT(*) INTO VCOUNT_CLI_SEM FROM APPS.TKT_BACEN_CLI_ALL BCA INNER JOIN APPS.TKT_HIGI_REQUEST_TRACKING THRT ON BCA.CNPJ = THRT.THIRD_PARTY_NUMBER WHERE 1=1 AND BCA.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO' AND TRUNC(THRT.REQUEST_DATE) >= TRUNC(SYSDATE-7) AND ((TRUNC(BCA.PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (BCA.PROGRAM_UPDATE_DATE IS NULL)) AND BCA.SISTEMA_ORIGEM !=  'REPOM' AND BCA.TIPO_INSCR = 2 AND THRT.THIRD_PARTY_TYPE = 'CNPJ' AND THRT.STATUS_HIGIENIZACAO IS NULL AND THRT.STATUS_RECEITA_FEDERAL IS NULL AND THRT.REQUEST_DATE IN (SELECT MAX(THRT1.REQUEST_DATE) FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT1 WHERE THRT1.THIRD_PARTY_NUMBER = THRT.THIRD_PARTY_NUMBER) AND BCA.CLIENTE_ID IN(SELECT MAX(BCA1.CLIENTE_ID)FROM APPS.TKT_BACEN_CLI_ALL BCA1 WHERE 1 = 1 AND BCA1.STATUS_PROCESSAMENTO = BCA.STATUS_PROCESSAMENTO AND BCA1.SISTEMA_ORIGEM = BCA.SISTEMA_ORIGEM AND BCA1.LAST_UPDATE_DATE = BCA1.LAST_UPDATE_DATE AND BCA1.CNPJ = BCA.CNPJ);

          IF (VCOUNT_CLI_SEM > 0)THEN
              VCOUNT_CLI_SEM := CHECK_VER1;
            ELSE
              VCOUNT_CLI_SEM := CHECK_FAL1;
          END IF;

          IF (VCOUNT_CLI_SEM = 1) THEN

            OPEN CUR_BUREAU;
               LOOP
                 FETCH CUR_BUREAU INTO VCUR_BUREAU;
                 EXIT WHEN CUR_BUREAU%NOTFOUND;
                  OPEN CUR_CNPJ;
                     LOOP
                       FETCH CUR_CNPJ INTO VCUR_CNPJ;
                       EXIT WHEN CUR_CNPJ%NOTFOUND OR G_COUNT >= G_LIMIT;                  

                         UPDATE APPS.TKT_BACEN_CLI_ALL SET PROGRAM_UPDATE_DATE = SYSDATE WHERE CLIENTE_ID IN VCUR_CNPJ.CLIENTE_ID AND ((TRUNC(PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (PROGRAM_UPDATE_DATE IS NULL));                                      
                         APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE        => 'ERP'
                                                                      , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                                      , P_ORG_ID             => VCUR_CNPJ.ORG
                                                                      , P_TYPE               => 'C'
                                                                      , P_THIRD_PARTY_TYPE   => 'CNPJ'
                                                                      , P_THIRD_PARTY_NUMBER => VCUR_CNPJ.CNPJ
                                                                      , P_THIRD_PARTY_ID     => 1
                                                                      , P_REQUEST_ID         => VCUR_BUREAU.REQUEST
                                                                     );

                       
                       G_COUNT := G_COUNT + 1;
                       COMMIT;
                     END LOOP;
                  CLOSE CUR_CNPJ;
               END LOOP;
            CLOSE CUR_BUREAU;
          ELSE
             VCOUNT_CLI_SEM := CHECK_FAL1;
             DBMS_OUTPUT.PUT_LINE(VCOUNT_CLI_SEM);
          END IF;

           EXCEPTION
            WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO'||SQLCODE||' MSG '||SQLERRM);
              DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

      END;

  END PROC_SEM_RET_CNPJ;
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE PROC_SEM_RET_CPF AS
  
    BEGIN
      DECLARE

        VCOUNT_CLI_SEM INT;
        CHECK_VER1 INT := 1;
        CHECK_FAL1 INT := 0;
        G_COUNT NUMBER := 0;
        G_LIMIT NUMBER;

        CURSOR CUR_BUREAU IS
          SELECT /*+ ALL_ROWS*/
               THRT.STATUS_HIGIENIZACAO,
               THRT.REQUEST_ID AS REQUEST,
               THRT.THIRD_PARTY_NUMBER
          FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT
          WHERE 1=1
          AND TRUNC(THRT.REQUEST_DATE) >= TRUNC(SYSDATE-7)
          AND THRT.SUBSYSTEM_SOURCE !=  'REPOM'
          AND THRT.THIRD_PARTY_TYPE = 'CPF'
          AND THRT.STATUS_HIGIENIZACAO IS NULL
          AND THRT.STATUS_RECEITA_FEDERAL IS NULL
          AND THRT.REQUEST_DATE IN (SELECT MAX(THRT1.REQUEST_DATE) FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT1 WHERE THRT1.THIRD_PARTY_NUMBER = THRT.THIRD_PARTY_NUMBER);

        VCUR_BUREAU CUR_BUREAU%ROWTYPE;

        CURSOR CUR_CPF IS
          SELECT /*+ FIRST_ROWS(4)*/
               BCA.CLIENTE_ID,
               BCA.PROGRAM_UPDATE_DATE,
               BCA.STATUS_PROCESSAMENTO,
               BCA.ORG_ID AS ORG,
               BCA.CNPJ AS CPF
          FROM APPS.TKT_BACEN_CLI_ALL BCA
          WHERE 1=1
          AND BCA.CNPJ = VCUR_BUREAU.THIRD_PARTY_NUMBER
          AND TRUNC(BCA.LAST_UPDATE_DATE) >= TRUNC(SYSDATE-7) AND BCA.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND ((TRUNC(BCA.PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (BCA.PROGRAM_UPDATE_DATE IS NULL))
          AND BCA.SISTEMA_ORIGEM !=  'REPOM'
          AND BCA.TIPO_INSCR = 1
          AND BCA.CLIENTE_ID IN(SELECT MAX(BCA1.CLIENTE_ID)FROM APPS.TKT_BACEN_CLI_ALL BCA1 WHERE 1 = 1 AND BCA1.STATUS_PROCESSAMENTO = BCA.STATUS_PROCESSAMENTO AND BCA1.SISTEMA_ORIGEM = BCA.SISTEMA_ORIGEM AND BCA1.LAST_UPDATE_DATE = BCA1.LAST_UPDATE_DATE AND BCA1.CNPJ = BCA.CNPJ);

        VCUR_CPF CUR_CPF%ROWTYPE;

      BEGIN
          SELECT VALOR40 INTO G_LIMIT FROM APPS.TKT_CONTR_CLI_PEDIDO WHERE VALOR40 IS NOT NULL;
          SELECT COUNT(*) INTO VCOUNT_CLI_SEM FROM APPS.TKT_BACEN_CLI_ALL BCA INNER JOIN APPS.TKT_HIGI_REQUEST_TRACKING THRT ON BCA.CNPJ = THRT.THIRD_PARTY_NUMBER WHERE 1=1 AND BCA.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO' AND TRUNC(THRT.REQUEST_DATE) >= TRUNC(SYSDATE-7) AND ((TRUNC(BCA.PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (BCA.PROGRAM_UPDATE_DATE IS NULL)) AND BCA.SISTEMA_ORIGEM !=  'REPOM' AND BCA.TIPO_INSCR = 1 AND THRT.THIRD_PARTY_TYPE = 'CPF' AND THRT.STATUS_HIGIENIZACAO IS NULL AND THRT.STATUS_RECEITA_FEDERAL IS NULL AND THRT.REQUEST_DATE IN (SELECT MAX(THRT1.REQUEST_DATE) FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT1 WHERE THRT1.THIRD_PARTY_NUMBER = THRT.THIRD_PARTY_NUMBER) AND BCA.CLIENTE_ID IN(SELECT MAX(BCA1.CLIENTE_ID)FROM APPS.TKT_BACEN_CLI_ALL BCA1 WHERE 1 = 1 AND BCA1.STATUS_PROCESSAMENTO = BCA.STATUS_PROCESSAMENTO AND BCA1.SISTEMA_ORIGEM = BCA.SISTEMA_ORIGEM AND BCA1.LAST_UPDATE_DATE = BCA1.LAST_UPDATE_DATE AND BCA1.CNPJ = BCA.CNPJ);

          IF (VCOUNT_CLI_SEM > 0)THEN
             VCOUNT_CLI_SEM := CHECK_VER1;
            ELSE
             VCOUNT_CLI_SEM := CHECK_FAL1;
          END IF;

          IF (VCOUNT_CLI_SEM = 1) THEN

            OPEN CUR_BUREAU;
               LOOP
                 FETCH CUR_BUREAU INTO VCUR_BUREAU;
                 EXIT WHEN CUR_BUREAU%NOTFOUND;
                  OPEN CUR_CPF;
                     LOOP
                       FETCH CUR_CPF INTO VCUR_CPF;
                       EXIT WHEN CUR_CPF%NOTFOUND OR G_COUNT >= G_LIMIT;

                       UPDATE APPS.TKT_BACEN_CLI_ALL SET PROGRAM_UPDATE_DATE = SYSDATE WHERE CLIENTE_ID IN VCUR_CPF.CLIENTE_ID AND ((TRUNC(PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (PROGRAM_UPDATE_DATE IS NULL));
                       APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE        => 'ERP'
                                                                    , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                                    , P_ORG_ID             => VCUR_CPF.ORG
                                                                    , P_TYPE               => 'C'
                                                                    , P_THIRD_PARTY_TYPE   => 'CPF'
                                                                    , P_THIRD_PARTY_NUMBER => VCUR_CPF.CPF
                                                                    , P_THIRD_PARTY_ID     => 1
                                                                    , P_PERSON_BIRTH_DATE  => '29-OUT-1969'
                                                                    , P_REQUEST_ID         => VCUR_BUREAU.REQUEST
                                                                   );


                       G_COUNT := G_COUNT + 1;
                       COMMIT;
                     END LOOP;
                  CLOSE CUR_CPF;
               END LOOP;
            CLOSE CUR_BUREAU;
          ELSE
             VCOUNT_CLI_SEM := CHECK_FAL1;
             DBMS_OUTPUT.PUT_LINE(VCOUNT_CLI_SEM);
          END IF;

         EXCEPTION
          WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO'||SQLCODE||' MSG '||SQLERRM);
           DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

    END;
	
  END PROC_SEM_RET_CPF;
-----------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE PROC_SEM_RET_EST AS
    BEGIN
      DECLARE

        VCOUNT_CLI_SEM INT;
        CHECK_VER1 INT := 1;
        CHECK_FAL1 INT := 0;
        G_COUNT NUMBER := 0;
        G_LIMIT NUMBER;

        CURSOR CUR_BUREAU IS
          SELECT /*+ ALL_ROWS*/
               THRT.STATUS_HIGIENIZACAO,
               THRT.REQUEST_ID AS REQUEST,
               THRT.THIRD_PARTY_NUMBER
          FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT
          WHERE 1=1
          AND TRUNC(THRT.REQUEST_DATE) >= TRUNC(SYSDATE-7)
          AND THRT.SUBSYSTEM_SOURCE !=  'REPOM'
          AND THRT.THIRD_PARTY_TYPE = 'CNPJ'
          AND THRT.STATUS_HIGIENIZACAO IS NULL
          AND THRT.STATUS_RECEITA_FEDERAL IS NULL
          AND THRT.REQUEST_DATE IN (SELECT MAX(THRT1.REQUEST_DATE) FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT1 WHERE THRT1.THIRD_PARTY_NUMBER = THRT.THIRD_PARTY_NUMBER);

        VCUR_BUREAU CUR_BUREAU%ROWTYPE;

        CURSOR CUR_CNPJ IS
          SELECT /*+ PARALLEL(30)*/
               TBEA.ESTABELECIMENTO_ID,
               TBEA.PROGRAM_UPDATE_DATE,
               TBEA.STATUS_PROCESSAMENTO,
               TBEA.ORG_ID AS ORG,
               TBEA.CNPJ
          FROM APPS.TKT_BACEN_ESTAB_ALL TBEA
          WHERE 1=1
          AND TBEA.CNPJ = VCUR_BUREAU.THIRD_PARTY_NUMBER
          AND TRUNC(TBEA.LAST_UPDATE_DATE) >= TRUNC(SYSDATE-7)
          AND TBEA.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO'
          AND ((TRUNC(TBEA.PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (TBEA.PROGRAM_UPDATE_DATE IS NULL))
          AND TBEA.SISTEMA_ORIGEM !=  'REPOM'
          AND TBEA.TIPO_INSCR = 2
          AND TBEA.ESTABELECIMENTO_ID IN(SELECT MAX(TBEA.ESTABELECIMENTO_ID)FROM APPS.TKT_BACEN_ESTAB_ALL TBEA1 WHERE 1 = 1 AND TBEA1.STATUS_PROCESSAMENTO = TBEA.STATUS_PROCESSAMENTO AND TBEA1.CNPJ = TBEA.CNPJ);

        VCUR_CNPJ CUR_CNPJ%ROWTYPE;

      BEGIN
          SELECT VALOR40 INTO G_LIMIT FROM APPS.TKT_CONTR_CLI_PEDIDO WHERE VALOR40 IS NOT NULL;
          SELECT COUNT(*) INTO VCOUNT_CLI_SEM FROM APPS.TKT_BACEN_ESTAB_ALL TBEA INNER JOIN APPS.TKT_HIGI_REQUEST_TRACKING THRT ON TBEA.CNPJ = THRT.THIRD_PARTY_NUMBER WHERE 1=1 AND TBEA.STATUS_PROCESSAMENTO = 'SOLICITADA HIGIENIZACAO' AND TRUNC(THRT.REQUEST_DATE) >= TRUNC(SYSDATE-7) AND ((TRUNC(TBEA.PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (TBEA.PROGRAM_UPDATE_DATE IS NULL)) AND TBEA.SISTEMA_ORIGEM !=  'REPOM' AND TBEA.TIPO_INSCR = 2 AND THRT.THIRD_PARTY_TYPE = 'CNPJ' AND THRT.STATUS_HIGIENIZACAO IS NULL AND THRT.STATUS_RECEITA_FEDERAL IS NULL AND THRT.REQUEST_DATE IN (SELECT MAX(THRT1.REQUEST_DATE) FROM APPS.TKT_HIGI_REQUEST_TRACKING THRT1 WHERE THRT1.THIRD_PARTY_NUMBER = THRT.THIRD_PARTY_NUMBER) AND TBEA.ESTABELECIMENTO_ID IN(SELECT MAX(TBEA1.ESTABELECIMENTO_ID)FROM APPS.TKT_BACEN_ESTAB_ALL TBEA1 WHERE 1=1 AND TBEA1.CNPJ = TBEA.CNPJ);

          IF (VCOUNT_CLI_SEM > 0)THEN
             VCOUNT_CLI_SEM := CHECK_VER1;
            ELSE
             VCOUNT_CLI_SEM := CHECK_FAL1;
          END IF;

          IF (VCOUNT_CLI_SEM = 1) THEN

            OPEN CUR_BUREAU;
               LOOP
                 FETCH CUR_BUREAU INTO VCUR_BUREAU;
                 EXIT WHEN CUR_BUREAU%NOTFOUND;
                  OPEN CUR_CNPJ;
                     LOOP
                       FETCH CUR_CNPJ INTO VCUR_CNPJ;
                       EXIT WHEN CUR_CNPJ%NOTFOUND OR G_COUNT >= G_LIMIT;

                       UPDATE APPS.TKT_BACEN_ESTAB_ALL SET PROGRAM_UPDATE_DATE = SYSDATE WHERE ESTABELECIMENTO_ID IN VCUR_CNPJ.ESTABELECIMENTO_ID AND ((TRUNC(PROGRAM_UPDATE_DATE) = TRUNC(SYSDATE-1)) OR (PROGRAM_UPDATE_DATE IS NULL));
                       APPS.TKT_HIGIENIZ_UTIL_PKG.CREATE_REQUEST_P( P_SYSTEM_SOURCE        => 'ERP'
                                                                    , P_SUBSYSTEM_SOURCE   => 'STAGING-CUSTOMERS'
                                                                    , P_ORG_ID             => VCUR_CNPJ.ORG
                                                                    , P_TYPE               => 'C'
                                                                    , P_THIRD_PARTY_TYPE   => 'CNPJ'
                                                                    , P_THIRD_PARTY_NUMBER => VCUR_CNPJ.CNPJ
                                                                    , P_THIRD_PARTY_ID     => 1
                                                                    , P_REQUEST_ID         => VCUR_BUREAU.REQUEST
                                                                   );



                       G_COUNT := G_COUNT + 1;
                       COMMIT;
                     END LOOP;
                  CLOSE CUR_CNPJ;
               END LOOP;
            CLOSE CUR_BUREAU;
          ELSE
             VCOUNT_CLI_SEM := CHECK_FAL1;
             DBMS_OUTPUT.PUT_LINE(VCOUNT_CLI_SEM);
          END IF;

         EXCEPTION
          WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO'||SQLCODE||' MSG '||SQLERRM);
           DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

      END;

  END PROC_SEM_RET_EST;

END TKT_ATENDIMENTO_PKG;
/
--NAO REMOVER INDICADOR DE FINAL DE ARQUIVO

