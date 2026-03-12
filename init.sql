-- =============================================================
-- init.sql — tabelas para usar com o modular-ingestion-framework
-- =============================================================

-- Municípios (usado nos exemplos IBGE)
CREATE TABLE IF NOT EXISTS municipios (
    id            INTEGER PRIMARY KEY,
    nome          VARCHAR(120) NOT NULL,
    uf            CHAR(2)      NOT NULL,
    populacao     INTEGER,
    area_km2      NUMERIC(10, 2),
    densidade_hab_km2 NUMERIC(10, 2),
    regiao        VARCHAR(30),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

-- Selic (usado no exemplo BCB Selic)
CREATE TABLE IF NOT EXISTS selic_diaria (
    data              DATE        PRIMARY KEY,
    taxa_ao_dia       NUMERIC(10, 6) NOT NULL,
    taxa_anualizada_pct NUMERIC(8, 4),
    fonte             VARCHAR(50) DEFAULT 'BCB SGS 11',
    atualizado_em     TIMESTAMP DEFAULT NOW()
);

-- IPCA (usado no exemplo BCB IPCA)
CREATE TABLE IF NOT EXISTS ipca_mensal (
    periodo           CHAR(7)     PRIMARY KEY,  -- formato: 2025-01
    variacao_pct      NUMERIC(8, 4) NOT NULL,
    fonte             VARCHAR(50) DEFAULT 'BCB SGS 433',
    atualizado_em     TIMESTAMP DEFAULT NOW()
);

-- Pipeline audit log (opcional — registra execuções)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               SERIAL PRIMARY KEY,
    pipeline         VARCHAR(100) NOT NULL,
    success          BOOLEAN NOT NULL,
    records_fetched  INTEGER DEFAULT 0,
    records_loaded   INTEGER DEFAULT 0,
    duration_seconds NUMERIC(10, 3),
    started_at       TIMESTAMP,
    created_at       TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- Dados iniciais para testar sem precisar chamar API
-- =============================================================

INSERT INTO municipios (id, nome, uf, populacao, area_km2, densidade_hab_km2, regiao) VALUES
    (2207702, 'Parnaíba',            'PI', 153820, 435.6,  353.27, 'Norte'),
    (2211001, 'Teresina',            'PI', 868075, 1391.9, 623.68, 'Centro-Norte'),
    (2203909, 'Floriano',            'PI',  60252, 3408.5,  17.68, 'Sul'),
    (2208007, 'Picos',               'PI',  78222, 1944.2,  40.23, 'Centro'),
    (2209104, 'São Raimundo Nonato', 'PI',  34908, 2499.8,  13.96, 'Sul')
ON CONFLICT (id) DO UPDATE SET
    nome      = EXCLUDED.nome,
    populacao = EXCLUDED.populacao,
    atualizado_em = NOW();

-- =============================================================
-- Views úteis para checar os dados
-- =============================================================

CREATE OR REPLACE VIEW v_municipios_resumo AS
SELECT
    uf,
    COUNT(*)            AS total_municipios,
    SUM(populacao)      AS populacao_total,
    ROUND(AVG(densidade_hab_km2)::numeric, 2) AS densidade_media
FROM municipios
GROUP BY uf
ORDER BY populacao_total DESC;

CREATE OR REPLACE VIEW v_pipeline_runs_resumo AS
SELECT
    pipeline,
    COUNT(*)                              AS total_execucoes,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) AS sucesso,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS falha,
    ROUND(AVG(duration_seconds)::numeric, 3) AS duracao_media_s,
    SUM(records_loaded)                   AS total_registros_carregados
FROM pipeline_runs
GROUP BY pipeline
ORDER BY total_execucoes DESC;

-- Confirma criação
DO $$
BEGIN
    RAISE NOTICE '✓ Tabelas criadas: municipios, selic_diaria, ipca_mensal, pipeline_runs';
    RAISE NOTICE '✓ Views criadas: v_municipios_resumo, v_pipeline_runs_resumo';
    RAISE NOTICE '✓ 5 municípios do PI inseridos como dados iniciais';
END $$;