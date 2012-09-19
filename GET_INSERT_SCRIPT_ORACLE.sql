create type linha_script is object (conteudo varchar2(4000));
/

create type tabela_linha is table of linha_script;
/


CREATE OR REPLACE FUNCTION GET_INSERT_SCRIPT (
	V_TABLE_NAME VARCHAR2,
	V_OWNER_ORIGEM VARCHAR2,
	V_OWNER_DESTINO VARCHAR2)
RETURN tabela_linha 
AS
	tabela_existe	BOOLEAN := FALSE;
	
	-- variáveis auxiliares para execução e geração 
	id_cursor 		NUMBER;
	col_count 		NUMBER;
	descricao_tab	dbms_sql.desc_tab;
	
	TYPE cur_typ 	IS REF CURSOR;
    cur        		cur_typ;
	
	-- variáveis com valores parciais de uma linha
	tmp_campos		VARCHAR2(4000) := '';
	nc int := 0;
	tmp_valores		VARCHAR2(4000) := '';
	nv int := 0;

	-- variáveis para recuperar valores dos campos	
	namevar  VARCHAR2(32767);
    numvar   NUMBER;
    datevar  DATE;

	query			VARCHAR2(4000);
	--n				INT;
	
	-- variáveis de tabelas para armazenar todas as linhas de dados
	campos tabela_linha := tabela_linha();
	valores tabela_linha := tabela_linha();
	
BEGIN

	-- procura a tabela e abre um cursor implícito
	FOR TAB_REC IN 
			(SELECT TABLE_NAME, OWNER
			FROM ALL_TABLES
			WHERE TABLE_NAME = UPPER(V_TABLE_NAME)
			AND V_OWNER_ORIGEM = UPPER(OWNER)) LOOP
		
		-- armazena que a tabela foi encontrada
		tabela_existe := true;
		
		-- executa a query para recuperar todos os valores, abrindo um cursor através da biblioteca do oracle
		query := 'SELECT * FROM ' || TAB_REC.OWNER || '.' || TAB_REC.TABLE_NAME;
		id_cursor := dbms_sql.open_cursor;
		dbms_sql.parse(id_cursor, query, dbms_sql.native); 
		
		--recupera descrição das colunas do cursor
		dbms_sql.describe_columns( c => id_cursor, col_cnt => col_count, desc_t  => descricao_tab); 
		
		-- inicia variáveis parciais dos campos e dos valores
		tmp_campos := ' (';
		
		-- adiciona a primeira linha do INSERT
		campos.extend;
		nc := nc + 1;
		campos(nc) := linha_script('INSERT INTO ' || V_OWNER_DESTINO || '.' || TAB_REC.TABLE_NAME);
	
		-- percorre todas as colunas, cria a lista de campos do insert e faz o bind das colunas de retorno
		FOR i IN 1 .. col_count LOOP
		
			--somente coloca a vírgula a partir de segundo elemento
			if i > 1 then
				tmp_campos := tmp_campos || ', ';
				--quebra linha a cada N elementos
				if i mod 30 = 0 then
					--adiciona a linha de elementos parcial na lista final
					campos.extend;
					nc := nc + 1;
					campos(nc) := linha_script(tmp_campos);
					--limpa a variável de campos parcial
					tmp_campos := '  ';
				end if;
			end if;
			
			--adiciona o campo atual na variável parciaç
			tmp_campos := tmp_campos || descricao_tab(i).col_name;		
		
			--bind das colunas de resultado
            IF descricao_tab(i).col_type = 2 THEN
                DBMS_SQL.DEFINE_COLUMN(id_cursor, i, numvar);
            ELSIF descricao_tab(i).col_type = 12 THEN
                DBMS_SQL.DEFINE_COLUMN(id_cursor, i, datevar);
            ELSE
                DBMS_SQL.DEFINE_COLUMN(id_cursor, i, namevar, 4000);
            END IF;
			
        END LOOP;
		
		--adiciona última linha de campos
		campos.extend;
		nc := nc + 1;
		campos(nc) := linha_script(tmp_campos || ')');
		
		--inicia parte dos calores
		campos.extend;
		nc := nc + 1;
		campos(nc) := linha_script('VALUES');		
		
		--executa o select * na tabela
		if dbms_sql.execute(id_cursor) = 0 then
		
			--cabeçalho
			valores.extend;
			nv := nv + 1;
			valores(nv) := linha_script('---------- CARGA DA TABELA ' || TAB_REC.OWNER || '.' || TAB_REC.TABLE_NAME || '----------');
			
			--adiciona linha para DELETE
			valores.extend;
			nv := nv + 1;
			valores(nv) := linha_script('DELETE FROM ' || V_OWNER_DESTINO || '.' || TAB_REC.TABLE_NAME || ';');
		
			--percorre os registros retornados pelo select
			while dbms_sql.fetch_rows(id_cursor) > 0 LOOP
			
				--adiciona as linhas dos campos "INSERT INTO TAB (CAMPO1, CAMPO2, ...) VALUES " para cada conjunto de valores
				FOR i IN 1 .. nc LOOP
					valores.extend;
					nv := nv + 1;
					valores(nv) := campos(i);
				END LOOP;
				
				--inicia a lista de valores
				tmp_valores := ' (';
				
				--percorre cada coluna do registro atual
				FOR i IN 1 .. col_count LOOP
				
					--somente coloca a vírgula a partir de segundo elemento
					if i > 1 then
						tmp_valores := tmp_valores || ', ';
						--quebra linha a cada N elementos
						if i mod 30 = 0 then
							--adiciona a linha de elementos parcial na lista final
							valores.extend;
							nv := nv + 1;
							valores(nv) := linha_script(tmp_valores);
							--limpa a variável de valores parcial
							tmp_valores := '  ';
						end if;
					end if;
				
					--recupera o valor dependendo do tipo e adiciona à variável parcial de valores
					if descricao_tab(i).col_type = 2 then  --number
                        DBMS_SQL.COLUMN_VALUE(id_cursor, i, numvar);
                        if numvar is null then
                            tmp_valores := tmp_valores || 'null';
                        else
                            tmp_valores := tmp_valores || replace(to_char(numvar),',','.');
                        end if;
                    elsif descricao_tab(i).col_type = 12 then --date
                        DBMS_SQL.COLUMN_VALUE(id_cursor, i, datevar);
                        if datevar is null then
                            tmp_valores := tmp_valores || 'null';
                        else
                            if datevar = trunc(datevar) then
                                tmp_valores := tmp_valores || 'to_date('''
                                    || to_char(datevar, 'DD/MM/YYYY') ||''',''DD/MM/YYYY'')';
                            else
                                tmp_valores := tmp_valores || 'to_date('''
                                    || to_char(datevar, 'DD/MM/YYYY hh24:mi:ss') ||''',''DD/MM/YYYY hh24:mi:ss'')';
                            end if;
                        end if;
                    else --varchar2, char, others
                        DBMS_SQL.COLUMN_VALUE(id_cursor, i, namevar); 
                        if namevar is null then
                            tmp_valores := tmp_valores || 'null';
                        else
                            tmp_valores := tmp_valores || '''' || namevar || '''';
                        end if;
                    end if;
						
                end loop;
				
				--adiciona última linha de valores
				valores.extend;
				nv := nv + 1;
				valores(nv) := linha_script(tmp_valores || ');');

			END LOOP;
			
			--adiciona um commit ao final de cada tabela (rodapé)			
			valores.extend;
			nv := nv + 1;
			valores(nv) := linha_script('commit;');
			
			--adiciona uma linha em branco
			valores.extend;
			nv := nv + 1;
			valores(nv) := linha_script('');
			
		end if;
		
		--fecha o cursor
		dbms_sql.close_cursor(id_cursor);
		
	END LOOP;

	--se a tabela não existir, retorna um comentário de erro
	IF NOT tabela_existe THEN
		campos.extend;
		campos(1) := linha_script('-- Table ' || V_TABLE_NAME || ' not found');
		return campos;
	END IF;

	RETURN valores;
END;
/