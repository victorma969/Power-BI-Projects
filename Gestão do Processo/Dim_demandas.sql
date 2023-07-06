WITH
    sit_deman AS (SELECT DISTINCT ON (id_demanda) id_demanda, nm_valor AS "Situação da Demanda"
                    FROM demanda
					  	INNER JOIN demanda_dado USING (id_demanda)
	                    INNER JOIN dado_valor USING (id_dado_valor)
	                WHERE id_versao_processo = 8747
					AND id_dado = 4632
                    ORDER BY id_demanda, demanda_dado.dt_cadastro DESC),

    cliente AS (SELECT id_demanda, substr(nm_valor, 5, 7) AS "Gerência", substr(nm_valor, 1, 3) AS "Superintendência"
            	FROM demanda_dado
            		INNER JOIN dado_valor USING (id_dado_valor)
            	WHERE id_dado = 4599),

    nmr_nup AS (SELECT id_demanda,nm_valor AS nup, substr(nm_valor, 14, 4)::INTEGER AS ano_nup
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado = 35),

    nm_nurac AS (SELECT DISTINCT ON (id_demanda) id_demanda, nm_valor AS nurac
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado = 3780
				ORDER BY id_demanda, demanda_dado.dt_cadastro DESC),

	mes_prev AS (SELECT DISTINCT ON (id_demanda) id_demanda, substr(nm_valor, 6, 10) AS "Mês de Execução Previsto"
				FROM demanda
					INNER JOIN demanda_dado USING (id_demanda)
					INNER JOIN dado_valor USING (id_dado_valor)
				WHERE id_versao_processo = 8747
				AND id_dado = 4605
				ORDER BY id_demanda, demanda_dado.dt_cadastro DESC),

	ano_prev AS (SELECT id_demanda, nm_valor::INTEGER AS ano_prev
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado = 4694),

	data_exec AS (SELECT id_demanda, nm_valor::DATE AS "Data de Execução"
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado = 4654),

	mun_exec AS (SELECT DISTINCT ON(id_demanda) id_demanda, nm_valor AS municipio
                FROM demanda
                	INNER JOIN demanda_dado USING (id_demanda)
                	INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado = 4270
                ORDER BY id_demanda, demanda_dado.dt_cadastro DESC),

	uf_exec AS (SELECT DISTINCT ON(id_demanda) id_demanda, nm_valor AS UF
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
				AND id_dado = 4269
				ORDER BY id_demanda, demanda_dado.dt_cadastro DESC),

	aero_exec AS (SELECT DISTINCT ON (id_demanda) id_demanda, nm_valor AS "Aeródromo"
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
				AND id_dado = 124
				ORDER BY id_demanda, demanda_dado.dt_cadastro DESC),

	local_exec AS (SELECT id_demanda,
				   CASE WHEN municipio IS NOT NULL AND UF IS NOT NULL THEN TRIM(municipio) || ', ' || UF
				   		WHEN UF IS NOT NULL THEN UF
				   		ELSE NULL END AS "Local de Execução", "Aeródromo"
                FROM mun_exec
                    LEFT JOIN uf_exec USING (id_demanda)
                    LEFT JOIN aero_exec USING (id_demanda)),

    linha_pta AS (SELECT id_demanda, nm_valor AS "Linha PTA (SPO)"
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado = 4623),

    tp_insp AS (SELECT id_demanda, nm_valor AS "Tipo de Inspeção"
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado IN (4594, 4595, 4596, 4597, 4598, 4600, 4689, 4656)),

	dem_capt AS (SELECT id_demanda, 'Cadastrada pelo NURAC'::TEXT sit
				FROM demanda_etapa
					INNER JOIN elemento_pn s1 ON s1.id_elemento_pn = id_elemento_pn_inicial
					INNER JOIN elemento_pn s2 ON s2.id_elemento_pn = id_elemento_pn_passagem
				WHERE s1.id_versao_processo = 8747
				AND id_elemento_pn_inicial = 106298
				AND id_elemento_pn_passagem = 106308),

	reg_etapas AS (SELECT id_demanda , nm_login_executor, id_elemento_pn_inicial, nr_lead_time, dt_inicio, rank() OVER(PARTITION BY id_demanda ORDER BY id_demanda_etapa DESC) rank_etapa,
				CASE WHEN dt_fim IS NULL THEN CURRENT_TIMESTAMP ELSE dt_fim END AS data_fim,
				CASE WHEN nm_elemento_pn ILIKE '%entrada%do%processo%sei%' THEN 'Etapa 00'
					WHEN nm_elemento_pn ILIKE '%conferir%cadastro%' THEN 'Etapa 01'
					WHEN nm_elemento_pn ILIKE '%distribuir%atividade%exec%' THEN 'Etapa 02'
					WHEN nm_elemento_pn ILIKE '%verificar%disponibilidade%exec%' THEN 'Etapa 03'
					WHEN nm_elemento_pn ILIKE '%verificar%proposta%andamento%' THEN 'Etapa 04'
					WHEN nm_elemento_pn ILIKE '%emitir%o%s%' THEN 'Etapa 05'
					WHEN nm_elemento_pn ILIKE '%executar%atividade%' THEN 'Etapa 06'
					ELSE 'Demanda Finalizada' END AS "Etapa"
			FROM demanda_etapa
				INNER JOIN elemento_pn ON id_elemento_pn = id_elemento_pn_inicial
			WHERE id_versao_processo = 8747),

	data_ini_prev AS (SELECT id_demanda, "Data de Início Prevista"
					FROM (SELECT id_demanda, nm_valor::DATE AS "Data de Início Prevista",
						  RANK() OVER(PARTITION BY id_demanda ORDER BY nm_valor::DATE DESC)
    						FROM demanda
    							INNER JOIN demanda_dado USING (id_demanda)
    							INNER JOIN dado_valor USING (id_dado_valor)
    						WHERE id_versao_processo = 8747
    						AND id_dado = 4604) AS s_dt_in
					 WHERE rank = 1),

	data_term_prev AS (SELECT id_demanda, "Data de Término Prevista"
	                   FROM (SELECT id_demanda, nm_valor::DATE AS "Data de Término Prevista",
	                            RANK() OVER(PARTITION BY id_demanda ORDER BY nm_valor::DATE DESC)
            					FROM demanda
            						INNER JOIN demanda_dado USING (id_demanda)
            						INNER JOIN dado_valor USING (id_dado_valor)
            					WHERE id_versao_processo = 8747
            					AND id_dado = 4647) AS s_dt_fi
        				WHERE rank=1),

	exe_dat AS (SELECT id_demanda, nm_valor::DATE AS "Data de Execução"
                FROM demanda
                    INNER JOIN demanda_dado USING (id_demanda)
                    INNER JOIN dado_valor USING (id_dado_valor)
                WHERE id_versao_processo = 8747
                AND id_dado = 4654),

	dias_previstos AS (SELECT id_demanda, CASE WHEN "Data de Término Prevista" IS NULL THEN 1
					   						ELSE dias_uteis("Data de Início Prevista","Data de Término Prevista")
					   						END AS "Dias Úteis Previstos"
					  FROM data_ini_prev
					  LEFT JOIN data_term_prev USING (id_demanda))

SELECT DISTINCT id_demanda AS "Nº Demanda", dt_cadastro::DATE AS "Data de Cadastro", nup AS "Nº Processo",ano_nup AS "Ano do Processo",
		"Gerência", "Superintendência", "Tipo de Inspeção", "Linha PTA (SPO)", "Situação da Demanda", nm_login_cadastrador "Solicitante",
		CASE WHEN nurac IS NULL THEN 'N.D.'
			ELSE nurac END AS "NURAC",
		nm_status_demanda AS "Status da Demanda",
		COALESCE(ano_prev, EXTRACT(YEAR FROM "Data de Início Prevista")) "Ano Previsto",
		CASE WHEN "Data de Término Prevista" IS NOT NULL THEN to_char("Data de Término Prevista", 'TMMonth')
		ELSE "Mês de Execução Previsto" END "Mês de Execução Previsto",
		CASE WHEN nm_status_demanda = 'Concluída' THEN to_char("Data de Execução", 'TMMonth')
			END AS "Mês de Execução Realizado",
		"Local de Execução", "Aeródromo", "Etapa",
		CASE WHEN nm_login_executor = '' AND nm_status_demanda = 'Concluída' THEN  'Demanda Finalizada'
		WHEN nm_login_executor = '' THEN 'Sem Executor'
		ELSE nm_login_executor END "Executor Atual", COALESCE(sit, 'Cadastrada pela GTREG') "Situação", "Data de Início Prevista", "Data de Término Prevista","Data de Execução"
        FROM demanda
            INNER JOIN sit_deman USING (id_demanda)
            INNER JOIN status_demanda USING (id_status_demanda)
            INNER JOIN cliente USING (id_demanda)
            INNER JOIN nmr_nup USING (id_demanda)
            LEFT JOIN nm_nurac USING (id_demanda)
			LEFT JOIN mes_prev USING (id_demanda)
			LEFT JOIN ano_prev USING (id_demanda)
			LEFT JOIN data_exec USING (id_demanda)
			LEFT JOIN linha_pta USING (id_demanda)
			LEFT JOIN tp_insp USING (id_demanda)
			LEFT JOIN local_exec USING (id_demanda)
			LEFT JOIN dem_capt USING (id_demanda)
			LEFT JOIN reg_etapas USING (id_demanda)
			LEFT JOIN data_ini_prev USING (id_demanda)
			LEFT JOIN data_term_prev USING (id_demanda)
			LEFT JOIN dias_previstos USING (id_demanda)
        WHERE id_versao_processo = 8747
        AND nurac = 'BHZ'
		AND rank_etapa = 1
ORDER BY id_demanda
