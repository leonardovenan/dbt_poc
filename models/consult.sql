SELECT t1.*
FROM dbcargaexterna.tbcambioprocess as t1
INNER JOIN (
    SELECT mes, MAX(dt_referencia_criacao) AS max_dt_referencia_criacao
    FROM dbcargaexterna.tbcambioprocess
    GROUP BY mes
) t2 ON t1.mes = t2.mes AND t1.dt_referencia_criacao = t2.max_dt_referencia_criacao
