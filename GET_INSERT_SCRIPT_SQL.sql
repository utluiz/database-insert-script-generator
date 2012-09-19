DROP PROCEDURE dbo.GET_INSERT_SCRIPT
GO

CREATE PROCEDURE dbo.GET_INSERT_SCRIPT (
	@TABELA VARCHAR(50),
	@BANCO_ORIGEM VARCHAR(50) = NULL,
	@BANCO_DESTINO VARCHAR(50) = NULL,
	@OWNER VARCHAR(50) = 'dbo',
	@WHERE VARCHAR(100) = NULL,
	@GERAR_DELETE INT = 1,
	@UPDATE_ON_EXISTS INT = 1,
	@SEPARADOR VARCHAR(1000) = '',
	@CAMPOS_POR_LINHA INT = 30
) 
AS
	DECLARE
		@QUERY			NVARCHAR(4000),
		@ORIGEM			VARCHAR(4000),
		@DESTINO		VARCHAR(4000),
		
		@TMP_CAMPOS		VARCHAR(4000),
		@TMP_QUERY		VARCHAR(4000),
		@TMP_UPDATE		VARCHAR(4000),
		@TMP_IFEXISTS	VARCHAR(4000),
		
		@NOME_CAMPO		VARCHAR(50),
		@TIPO_CAMPO		VARCHAR(50),
		
		@ASPAS			VARCHAR(50),
		@TAB			VARCHAR(50),
		@I				INT,
		@N				INT,
		@K				INT,
		@J				INT,
		@TOTAL			INT
BEGIN
		
	DECLARE @TAB_RET TABLE (LINHA VARCHAR(4000))
	DECLARE @TAB_CAMPOS TABLE (LINHA VARCHAR(4000))
	DECLARE @TAB_PK TABLE (LINHA VARCHAR(4000))
	DECLARE @TAB_QUERY TABLE (LINHA VARCHAR(4000))
	DECLARE @TAB_IFEXISTS TABLE (id int identity primary key, LINHA VARCHAR(4000))
	DECLARE @TAB_VALORES TABLE (id int identity primary key, LINHA VARCHAR(4000), POS INT)
	DECLARE @TAB_UPDATE TABLE (id int identity primary key, LINHA VARCHAR(4000), POS INT)
		
	SET @ORIGEM =  
		CASE WHEN @BANCO_ORIGEM IS NULL OR @BANCO_ORIGEM = '' THEN '' ELSE @BANCO_ORIGEM + '.' END + 
		CASE WHEN @OWNER IS NULL OR @OWNER = '' THEN '' ELSE @OWNER + '.' END + 
		@TABELA
	SET @DESTINO =
		CASE WHEN @BANCO_DESTINO IS NULL OR @BANCO_DESTINO = '' THEN '' ELSE @BANCO_DESTINO + '.' END + 
		CASE WHEN @OWNER IS NULL OR @OWNER = '' THEN '' ELSE @OWNER + '.' END + 
		@TABELA
	
	SET @ASPAS = '''''''''';
	SET @TAB = ''
	
	-- verifica se a tabela existe
	IF EXISTS (SELECT 1 FROM SYSOBJECTS WHERE ID = OBJECT_ID(@ORIGEM))
	BEGIN
	
		--cabeçalho
		IF @WHERE IS NULL OR @WHERE = ''
			INSERT INTO @TAB_RET SELECT '---------- CARGA DA TABELA ' + @TABELA + ' ----------'
		ELSE
			INSERT INTO @TAB_RET SELECT '---------- CARGA PARCIAL DA TABELA ' + @TABELA + ' ----------'
		
		SET @WHERE = (CASE WHEN @WHERE IS NULL OR @WHERE = '' THEN '' ELSE ' WHERE ' + @WHERE END)
		
		--adiciona linha para DELETE
		IF @GERAR_DELETE > 0
		BEGIN
			INSERT INTO @TAB_RET SELECT 'DELETE FROM ' + @DESTINO + @WHERE
			INSERT INTO @TAB_RET SELECT 'GO'
		END
		INSERT INTO @TAB_RET SELECT ''

		-- adiciona a primeira linha do INSERT
		INSERT INTO @TAB_CAMPOS SELECT 'INSERT INTO ' + @DESTINO 
		
		-- recupera campos da primary KEY
		IF @UPDATE_ON_EXISTS > 0
		BEGIN
			
			INSERT INTO @TAB_PK 
				SELECT CC.COLUMN_NAME 
				FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
				JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CC ON
					CC.TABLE_NAME = TC.TABLE_NAME
					AND CC.TABLE_SCHEMA = TC.TABLE_SCHEMA
					AND CC.TABLE_CATALOG = TC.TABLE_CATALOG
					AND CC.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
				WHERE TC.TABLE_NAME = @TABELA 
					AND (TC.TABLE_SCHEMA = @OWNER OR @OWNER IS NULL)
					AND (TC.TABLE_CATALOG = @BANCO_ORIGEM OR (@BANCO_ORIGEM IS NULL AND TC.TABLE_CATALOG = DB_NAME()))
					AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'

			SET @TAB = '  '
			
		END
		
		-- inicia variáveis parciais dos campos e dos valores
		SET @TMP_CAMPOS = '  ('
		SET @TMP_QUERY = 'SELECT '
		SET @TMP_UPDATE = 'SELECT '
		SET @TMP_IFEXISTS = '' --'IF EXISTS(SELECT 1 FROM ' + @ORIGEM + ' WHERE '
	
		-- cursor para iterar sobre as colunas da query
		DECLARE CURSCOL CURSOR FAST_FORWARD FOR 
			SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS 
				WHERE TABLE_NAME = @TABELA 
					AND (TABLE_SCHEMA = @OWNER OR @OWNER IS NULL)
					AND (TABLE_CATALOG = @BANCO_ORIGEM OR (@BANCO_ORIGEM IS NULL AND TABLE_CATALOG = DB_NAME()))
				ORDER BY ORDINAL_POSITION
				
		OPEN CURSCOL
		FETCH NEXT FROM CURSCOL INTO @NOME_CAMPO, @TIPO_CAMPO
		
		-- percorre todas as colunas, cria a lista de campos do insert e faz o bind das colunas de retorno
		SET @I = 0
		SET @N = 0
		SET @K = 0
		WHILE @@FETCH_STATUS = 0
		BEGIN
		
			SET @I = @I + 1
					
			--somente coloca a vírgula a partir de segundo elemento
			IF @I > 1
			BEGIN
			
				SET @TMP_CAMPOS = @TMP_CAMPOS + ', '
				
				--quebra linha a cada N elementos
				IF @I % @CAMPOS_POR_LINHA = 0
				BEGIN
				
					SET @N = @N + 1
					
					--adiciona a linha de elementos parcial na lista final
					INSERT INTO @TAB_CAMPOS SELECT @TMP_CAMPOS
					--INSERT INTO @TAB_QUERY SELECT 
					SET @QUERY = @TMP_QUERY + ' + '', '', ' + convert(varchar, @N) + ' FROM ' + @ORIGEM + @WHERE
					INSERT INTO @TAB_VALORES EXEC SP_EXECUTESQL @QUERY
					
					IF @UPDATE_ON_EXISTS > 0
					BEGIN
						IF @TMP_UPDATE = 'SELECT '
							SET @TMP_UPDATE = @TMP_UPDATE + ' '' '
						SET @QUERY = @TMP_UPDATE + ' + '', '', ' + convert(varchar, @N) + ' FROM ' + @ORIGEM + @WHERE
						INSERT INTO @TAB_UPDATE EXEC SP_EXECUTESQL @QUERY
					END
					
					--quantidade de linhas de valores
					IF @N = 1 SELECT @TOTAL = COUNT(*) FROM @TAB_VALORES
					
					--limpa a variável de campos parcial
					SET @TMP_CAMPOS = '  '
					SET @TMP_QUERY = 'SELECT '
					SET @TMP_UPDATE = 'SELECT '
					
				END
				ELSE
				BEGIN
				
					SET @TMP_QUERY = @TMP_QUERY + ' + '', '' + '
					
					IF @NOME_CAMPO NOT IN (SELECT LINHA FROM @TAB_PK) AND @TMP_UPDATE != 'SELECT '
						SET @TMP_UPDATE = @TMP_UPDATE + ' + '', '' + '
					
				END
			END
			
			--adiciona o campo à lista
			SET @TMP_CAMPOS = @TMP_CAMPOS + @NOME_CAMPO
			
			--adiciona campos ao select
			SET @TMP_QUERY = @TMP_QUERY + 'CASE WHEN ' + @NOME_CAMPO + ' IS NULL THEN ''NULL'' ELSE ' +
				CASE
					WHEN @TIPO_CAMPO IN ('int', 'smallint', 'tinyint', 'smallint', 'numeric', 'bit', 'bigint') 
						THEN '''''' + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + ''''''
					WHEN @TIPO_CAMPO IN ('numeric', 'real', 'money', 'float', 'decimal', 'smallmoney') 
						THEN '''''' + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + ''''''
					WHEN @TIPO_CAMPO IN ('smalldatetime', 'datetime', 'date', 'time')  
						THEN @ASPAS + '+ convert(varchar, ' + @NOME_CAMPO + ', 120)+' + @ASPAS
					WHEN @TIPO_CAMPO IN ('text', 'ntext')  
						THEN @ASPAS + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + @ASPAS
					ELSE @ASPAS + '+' + @NOME_CAMPO + '+' + @ASPAS END + ' END'
					
			IF @NOME_CAMPO IN (SELECT LINHA FROM @TAB_PK)
			BEGIN
				
				SET @K = @K + 1
				
				IF @K > 1
					SET @TMP_IFEXISTS = @TMP_IFEXISTS + ' + '' AND '' + '
					
				SET @TMP_IFEXISTS = @TMP_IFEXISTS + '''' + @NOME_CAMPO + ''' + '' = '' + ' +
				CASE
					WHEN @TIPO_CAMPO IN ('int', 'smallint', 'tinyint', 'smallint', 'numeric', 'bit', 'bigint') 
						THEN '''''' + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + ''''''
					WHEN @TIPO_CAMPO IN ('numeric', 'real', 'money', 'float', 'decimal', 'smallmoney') 
						THEN '''''' + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + ''''''
					WHEN @TIPO_CAMPO IN ('smalldatetime', 'datetime', 'date', 'time')  
						THEN @ASPAS + '+ convert(varchar, ' + @NOME_CAMPO + ', 120)+' + @ASPAS
					WHEN @TIPO_CAMPO IN ('text', 'ntext')  
						THEN @ASPAS + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + @ASPAS
					ELSE @ASPAS + '+' + @NOME_CAMPO + '+' + @ASPAS END
					
			END
			ELSE
			BEGIN
			
				--adiciona campos ao UPDATE
				IF @UPDATE_ON_EXISTS > 0 
					SET @TMP_UPDATE = @TMP_UPDATE + '''' + @NOME_CAMPO + ''' + ''='' + ' + 'CASE WHEN ' + @NOME_CAMPO + ' IS NULL THEN ''NULL'' ELSE ' +
						CASE
							WHEN @TIPO_CAMPO IN ('int', 'smallint', 'tinyint', 'smallint', 'numeric', 'bit', 'bigint') 
								THEN '''''' + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + ''''''
							WHEN @TIPO_CAMPO IN ('numeric', 'real', 'money', 'float', 'decimal', 'smallmoney') 
								THEN '''''' + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + ''''''
							WHEN @TIPO_CAMPO IN ('smalldatetime', 'datetime', 'date', 'time')  
								THEN @ASPAS + '+ convert(varchar, ' + @NOME_CAMPO + ', 120)+' + @ASPAS
							WHEN @TIPO_CAMPO IN ('text', 'ntext')  
								THEN @ASPAS + '+ convert(varchar, ' + @NOME_CAMPO + ')+' + @ASPAS
							ELSE @ASPAS + '+' + @NOME_CAMPO + '+' + @ASPAS END + ' END'			
			END
			
			--próxima coluna
			FETCH NEXT FROM CURSCOL INTO @NOME_CAMPO, @TIPO_CAMPO
		
		END
		
		--finaliza cursor das colunas
		CLOSE CURSCOL
		DEALLOCATE CURSCOL
		
		--adiciona última linha de campos
		INSERT INTO @TAB_CAMPOS SELECT @TMP_CAMPOS + ')'
		
		--executa select para pegar valores
		SET @N = @N + 1
		SET @QUERY = @TMP_QUERY + ', ' + convert(varchar, @N) + ' FROM ' + @ORIGEM + @WHERE
		INSERT INTO @TAB_VALORES EXEC SP_EXECUTESQL @QUERY
		
		IF @UPDATE_ON_EXISTS > 0
		BEGIN
			SET @QUERY = @TMP_UPDATE + ', ' + convert(varchar, @N) + ' FROM ' + @ORIGEM + @WHERE
			INSERT INTO @TAB_UPDATE EXEC SP_EXECUTESQL @QUERY
		END		

		--executa select para pegar verificações		
		IF @K > 0
		BEGIN
			SET @QUERY = 'SELECT ' + @TMP_IFEXISTS + ' FROM ' + @ORIGEM + @WHERE
			INSERT INTO @TAB_IFEXISTS EXEC SP_EXECUTESQL @QUERY
		END
		
		--quantidade de linhas de valores
		IF @N = 1 SELECT @TOTAL = COUNT(*) FROM @TAB_VALORES

		--inicia parte dos valores
		INSERT INTO @TAB_CAMPOS SELECT 'VALUES'
		
		--itera sobre o total de registro encontrados para montar o insert de cada um
		SET @I = 1
		WHILE @I <= @TOTAL
		BEGIN
		
			--se for testar
			IF @UPDATE_ON_EXISTS > 0
				INSERT INTO @TAB_RET
				SELECT 'IF NOT EXISTS(SELECT 1 FROM ' + @DESTINO + ' WHERE ' + LINHA +  ')' 
				FROM @TAB_IFEXISTS 
				WHERE ID = @I

			--insere INSERT para cada registro
			INSERT INTO @TAB_RET
			SELECT @TAB + LINHA FROM @TAB_CAMPOS
			
			--insere linhas com valores
			INSERT INTO @TAB_RET
			SELECT 
				CASE WHEN POS = 1 THEN @TAB + '  (' ELSE @TAB + '  ' END + 
				LINHA +
				CASE WHEN POS = @N THEN ')' ELSE '' END 
			FROM @TAB_VALORES WHERE (ID - 1) % @TOTAL = @I - 1
			
			IF @UPDATE_ON_EXISTS > 0
			BEGIN
				INSERT INTO @TAB_RET SELECT 'ELSE'
				
				INSERT INTO @TAB_RET SELECT '  UPDATE ' + @DESTINO + ' SET '
				
				INSERT INTO @TAB_RET
				SELECT '    ' + LINHA
				FROM @TAB_UPDATE WHERE (ID - 1) % @TOTAL = @I - 1
				
				INSERT INTO @TAB_RET
				SELECT '  WHERE ' + LINHA
				FROM @TAB_IFEXISTS 
				WHERE ID = @I
				
			END

			IF @SEPARADOR IS NOT NULL AND @I < @TOTAL
				INSERT INTO @TAB_RET SELECT @SEPARADOR
			
			SET @I = @I + 1
			
		END
		
		INSERT INTO @TAB_RET SELECT 'GO'

	END
	ELSE
	BEGIN
	
		--se a tabela não existir, retorna um comentário de erro
		INSERT INTO @TAB_RET SELECT '-- Tabela ' + @TABELA + ' não existe'

	END
	
	SELECT LINHA as '--' FROM @TAB_RET

END
GO

/*

USO: 

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
*/
