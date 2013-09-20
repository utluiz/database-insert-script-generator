database-insert-script-generator
================================

Simple procedures for generation of INSERT and UPDATE scripts from a database table. 
Both SQL Server and Oracle version are available.

Examples for SQL Server

--inibe contagem de resultados dos selects (saída desnecessária)
SET NOCOUNT ON
EXEC GET_INSERT_SCRIPT
    @TABELA = 'TAB1',
    @BANCO_ORIGEM = 'MINHA_BASE',
    @BANCO_DESTINO = DEFAULT,
    @OWNER = DEFAULT,
    @WHERE = DEFAULT,
    @GERAR_DELETE = 0

-- Gera INSERTs para todos os registros da tabela TAB2
EXEC GET_INSERT_SCRIPT
    @TABELA = 'TAB2',
    @GERAR_DELETE = 0

-- Gera INSERT para a tabela de produtos somente para o produto 'PROD' da empresa 'EMP1'
EXEC GET_INSERT_SCRIPT
    @TABELA = 'PRODUTOS',
    @WHERE = 'CODPRODUTO = ''PROD'' AND LECOLCOD = ''EMP1'' '

-- Gera INSERT da tabela BOLETOS somente para o boleto '123'
EXEC GET_INSERT_SCRIPT
    @TABELA = 'BOLETOS',
    @BANCO_ORIGEM = 'MINHA_BASE',
    @BANCO_DESTINO = DEFAULT,
    @OWNER = DEFAULT,
    @WHERE = 'CODBOLETO = 123'
