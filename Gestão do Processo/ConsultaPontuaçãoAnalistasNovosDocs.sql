WITH area_sfi AS (SELECT ID_DEMANDA, nm_valor as area, id_status_demanda,
		      CASE WHEN nm_valor = 'Serviços Aéreos' THEN 1
			   WHEN nm_valor = 'Ação Fiscal' THEN 1.8
			   WHEN nm_valor = 'SFI/Infraestrutura' THEN 1.5
			   WHEN nm_valor = 'SFI/Aeronavegabilidade' THEN 2
		      END AS peso_area, demanda.dt_cadastro::DATE AS cadastro_demanda
		      FROM demanda INNER JOIN demanda_dado USING (id_demanda)
				   INNER JOIN dado_valor USING (id_dado_valor)
		      WHERE id_versao_processo IN (5134, 5735, 6209) AND id_dado = 3828
			    AND id_demanda_dado = (SELECT MAX(id_demanda_dado) FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
						   WHERE demanda_dado.id_demanda = demanda.id_demanda AND id_dado = 3828)),
   sn_45 AS (SELECT id_demanda, id_elemento_pn_etapa, id_dado_valor, nm_valor,
		    CASE WHEN (id_dado_valor IN (39740, 207279) AND id_elemento_pn_etapa IN (47584, 55470, 63972)) THEN 1 ELSE 0 END AS sn_45_em_revisao,
		    CASE WHEN id_dado_valor = 207279 THEN 0.3 ELSE 1 END AS sn_forca_tarefa
	     FROM (SELECT id_demanda, id_demanda_dado, id_elemento_pn_etapa, nm_login_cadastro, dt_cadastro,
		    id_dado_valor, nm_valor, RANK() OVER(PARTITION BY id_demanda ORDER BY id_demanda_dado DESC)
		   FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
		   WHERE id_dado = 3770 AND id_elemento_pn_etapa IN (47584, 55470, 63972)) AS sub
	     WHERE rank = 1),
    sn_54 AS (SELECT id_demanda, id_elemento_pn_etapa, id_dado_valor, nm_valor,
			  CASE WHEN (id_dado_valor IN (39739, 209869) AND id_elemento_pn_etapa IN (47587, 55472, 63967)) THEN 0 ELSE 1
		    END AS sn_54_revisado
	     FROM (SELECT id_demanda, id_demanda_dado, id_elemento_pn_etapa, nm_login_cadastro, dt_cadastro,
		    id_dado_valor, nm_valor, RANK() OVER(PARTITION BY id_demanda ORDER BY id_demanda_dado DESC)
		   FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
		   WHERE id_dado = 3770 AND id_elemento_pn_etapa IN (47587, 55472, 63967)) AS sub
	     WHERE rank = 1),

	rev AS (SELECT id_demanda , MAX (nm_login_executor) AS rev
			FROM dado_valor
				INNER JOIN demanda_dado USING (id_dado_valor)
				INNER JOIN demanda USING (id_demanda)
				INNER JOIN demanda_ETAPA USING (id_demanda)
				INNER JOIN versao_processo USING (id_versao_processo)
			WHERE
                                id_PROCESSO = 3551
				AND id_dado = 3770
				AND (nm_login_executor) IN ('bruno.maranhao', 'manoel.souza')
				AND id_demanda_dado IN (SELECT MAX(id_demanda_dado) FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
										INNER JOIN dado USING (id_dado)
										GROUP BY id_dado, id_demanda)
				AND demanda_ETAPA.id_elemento_pn_inicial IN (47587, 55472, 63967) AND SN_ATUAL = TRUE GROUP BY id_demanda),

	nmr_nup AS (SELECT DISTINCT id_demanda, nm_valor as nup, substr(nm_valor, 14, 4)::INTEGER AS nr_ano_nup
	            FROM demanda_dado
	                INNER JOIN dado_valor USING (id_dado_valor)
			        INNER JOIN demanda USING (id_demanda)
	            WHERE id_demanda_dado IN (SELECT MAX(id_demanda_dado)
			    	                        FROM demanda_dado
			    	                            INNER JOIN dado_valor USING (id_dado_valor)
				                            WHERE id_dado = 3845 GROUP BY id_demanda)
		        AND id_versao_processo IN (6209, 5735, 5134)),

	trienal AS (SELECT id_demanda, nm_valor::DATE as trienal
	      FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
				INNER JOIN (SELECT id_demanda  FROM demanda LEFT JOIN status_demanda USING (id_status_demanda)
	      WHERE id_versao_processo IN (5735, 6209, 5134) AND id_status_demanda <= 2
	      ) as subt USING (id_demanda)
	      WHERE id_demanda_dado IN (SELECT MAX(id_demanda_dado)
					FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
					WHERE id_dado = 3765 GROUP BY id_demanda)),

quinquenal AS (SELECT id_demanda, nm_valor::DATE as quinquenal
	      FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
				INNER JOIN (SELECT id_demanda  FROM demanda LEFT JOIN status_demanda USING (id_status_demanda)
	      WHERE id_versao_processo IN (5735, 6209, 5134) AND id_status_demanda <= 2
	      ) as subq USING (id_demanda)
	      WHERE id_demanda_dado IN (SELECT MAX(id_demanda_dado)
					FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
					WHERE id_dado = 3766 GROUP BY id_demanda)),

count_54 AS (SELECT DISTINCT id_demanda, count AS count_54
	    FROM  (SELECT id_demanda, id_elemento_pn_inicial, pn5.nm_elemento_pn, id_elemento_pn_passagem, pn4.nm_elemento_pn, nm_login_executor,
			COUNT(id_demanda_etapa) OVER(PARTITION BY id_demanda)
		   FROM demanda_etapa INNER JOIN elemento_pn pn5 ON (pn5.id_elemento_pn = id_elemento_pn_inicial)
				   INNER JOIN elemento_pn pn4 ON (pn4.id_elemento_pn = id_elemento_pn_passagem)
		   WHERE id_elemento_pn_inicial IN (47587, 55472, 63967) AND id_elemento_pn_passagem IN (47584, 55470, 63972)) AS SUB),

     cte AS  (SELECT distinct id_elemento_pn_documento,
		    (CASE WHEN LOWER(nm_documento) LIKE '%despacho%lig%ncia%' THEN 0.6 /* Despacho de Diligência */
			  WHEN LOWER(nm_documento) LIKE '%decis%conces%50%' THEN 0.3 /* Decisão GTAA - Multa Integral (Com Defesa) */
			  WHEN LOWER(nm_documento) LIKE '%decis%sem%efesa%' THEN 0.6 /* Decisão GTAA - Multa Integral (Sem Defesa) */
			  WHEN LOWER (nm_documento) LIKE '%decis%cojug%arquivamento%nulidade%'  THEN 0.5 /* Decisão COJUG- Arquivamento por Nulidade*/
			  WHEN LOWER(nm_documento) LIKE '%parecer%' THEN 0.3 /* Parecer */
			  WHEN LOWER(nm_documento) LIKE '%despacho%' THEN 0.3 /* Despachos */
			  WHEN LOWER(nm_documento) LIKE '%of%cio%' THEN 0.3 /* Ofício */
			  WHEN LOWER(nm_documento) LIKE '%comunicado%' THEN 0.3 /* Comunicado de Multa */
			  WHEN LOWER(nm_documento) LIKE '%decis%' THEN 1 /* Decisão */
			  WHEN LOWER(nm_documento) LIKE '%nota%cnica%' THEN 1 /* Nota Técnica */
			  WHEN LOWER(nm_documento) LIKE '%memorando%dital%' THEN 1 /* Memorando Encaminha Edital  Publicação em DOU */
				WHEN LOWER(nm_documento) LIKE '%novo%' THEN 1 /*Novos documentos*/
			  ELSE 0.0
				  /*Sem pontuação:
				  "Memorando Encaminha Edital  Publicação em DOU"
				  "Despacho Especial 1"
				  "Edital GTAA - Anexo ao Memorando GTAA - Edital de Intimação"*/
		     END)::double precision AS peso
	      FROM elemento_pn_documento),
   situacao AS (SELECT id_demanda, situacao
   		 FROM 	(SELECT id_demanda, nm_valor AS situacao, RANK() OVER(PARTITION BY id_demanda ORDER BY id_demanda_dado DESC)
			 FROM area_sfi INNER JOIN demanda_dado USING (id_demanda) INNER JOIN dado_valor USING (id_dado_valor)
			 WHERE id_dado = 3770) AS SUB
		 WHERE rank = 1),

    pluna AS (SELECT id_demanda, CASE WHEN nm_valor ILIKE '%pluna%' THEN 0.1 ELSE 1 END AS sn_pluna
	     FROM (SELECT id_demanda, id_demanda_dado, nm_valor, RANK() OVER(PARTITION BY id_demanda ORDER BY id_demanda_dado DESC)
		   FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
		   WHERE id_dado = 3767) AS sub
	     WHERE rank = 1),

   CapLegal AS (SELECT id_demanda, nm_valor
				FROM demanda INNER JOIN demanda_dado USING (id_demanda)
			    INNER JOIN dado_valor USING (id_dado_valor)
			    INNER JOIN versao_processo USING (id_versao_processo)
				WHERE id_dado = 3776 AND id_processo = 3551
				AND id_demanda_dado IN (SELECT MAX(id_demanda_dado)
				FROM demanda_dado INNER JOIN dado_valor USING (id_dado_valor)
				WHERE id_dado = 3776 GROUP BY id_demanda))

SELECT 	id_demanda as "Nº Demanda", nup AS "Nº Processo", nr_ano_nup AS "Ano NUP", nm_documento as "Documento", trienal as "Data da Prescrição Trienal",
		quinquenal as "Data da Prescrição Quinquenal", nm_valor as "Capitulação Legal",	pont_ideal AS "Peso", EXTRACT(YEAR FROM cadastro_doc) AS nr_ano,
		area as "Área", login, rev AS "Revisor", cadastro_doc::DATE as cadastro, situacao AS "Situação do processo" ,nm_status_demanda AS "Status da demanda"

FROM (
	SELECT id_demanda, nm_login_cadastro AS login, nm_documento, area, nup, nr_ano_nup, nm_status_demanda, trienal, quinquenal, CapLegal.nm_valor,

	CASE WHEN nm_documento ilike '%Decisão%GTAA%Concessão%50%' THEN peso * sn_pluna * sn_forca_tarefa
		     WHEN nm_documento ilike 'Parecer%GTAA%Alteração%Competência' THEN peso * sn_pluna * sn_forca_tarefa
		     ELSE peso * peso_area * sn_pluna * sn_forca_tarefa
		END AS pont_ideal, dt_cadastro AS cadastro_doc, cadastro_demanda::DATE, sn_45_em_revisao, sn_54_revisado, CASE WHEN count_54 IS NULL THEN 0 ELSE count_54 END as COUNT_54, situacao,
		CASE WHEN atual.nm_login_executor = '' THEN 'Demanda Finalizada' ELSE atual.nm_login_executor END AS exec_atual, rev, 1 as ordem



	FROM elemento_pn_documento

			LEFT JOIN sei.padrao_sei USING (id_elemento_pn_documento)
			INNER JOIN sei.demanda_sei USING (id_padrao_sei)
			INNER JOIN cte USING (id_elemento_pn_documento)
			INNER JOIN area_sfi USING (id_demanda)
			INNER JOIN status_demanda USING (id_status_demanda)
			LEFT JOIN trienal USING (id_demanda)
			LEFT JOIN quinquenal USING (id_demanda)
			LEFT JOIN rev USING (id_demanda)
			LEFT JOIN sn_45 USING (id_demanda)
			LEFT JOIN sn_54 USING (id_demanda)
			LEFT JOIN situacao USING (id_demanda)
			LEFT JOIN demanda_etapa AS atual USING (id_demanda)
      LEFT JOIN pluna USING (id_demanda)
      LEFT JOIN count_54 USING (id_demanda)
      LEFT JOIN nmr_nup USING (id_demanda)
		  LEFT JOIN CapLegal USING (id_demanda) /*Inserir CAPLEGAL*/


	WHERE  	id_elemento_pn IN (SELECT id_elemento_pn FROM elemento_pn INNER JOIN versao_processo USING (id_versao_processo)
				   WHERE id_processo = 3551 AND (LOWER(nm_elemento_pn) LIKE '%analisar%consolidar%resultado%atualizar%situa%processo%' OR
								 LOWER(nm_elemento_pn) LIKE '%verificar%pend%documento%conclu%demanda%')
							    AND sn_minuta = 'FALSE')
		AND sei.demanda_sei.nm_login_cadastro IN ('allan.michel','aldrin.sampaio','carolina.carneiro','celso.leao','delvecclio.trivelato','ligia.deus',
							  															'marc.zw', 'bruno.maranhao', 'francis.costa','marcos.cardoso','naara.souza','alfredo.paula',
							  															'glend.dias', 'juliao.silva', 'eugenio.prado', 'cintia.schunk', 'jorge.henrique','liana.nascentes',
																							'maicon.ardirson', 'danielle.eller','marcelo.vicente', 'manoel.souza', 'lucas.cardoso', 'geovana.bernardes',
																							'paula.matheus', 'camilla.dornelas', 'ghianlluca.santos')
		AND NOT LOWER(nm_documento) LIKE '%comunicado%'
		AND atual.sn_atual = TRUE


) AS SUB

ORDER BY cadastro_doc DESC
