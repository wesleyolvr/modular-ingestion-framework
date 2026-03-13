import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from transformers.field_mapper import FieldMapper
from transformers.enrich_transformer import EnrichTransformer
from transformers.filter_transformer import FilterTransformer
from transformers.transform_pipeline import TransformPipeline
from transformers.ibge_transformer import IBGETransformer
from transformers.selic_transformer import SelicTransformer


# ── FieldMapper ───────────────────────────────────────────────────────────

class TestFieldMapper:
    def test_simple_mapping(self):
        mapper = FieldMapper(mapping={"identifier": "id", "name": "nome"})
        result = mapper.transform({"id": 1, "nome": "Parnaíba", "extra": "ignored"})
        assert result == {"identifier": 1, "name": "Parnaíba"}

    def test_nested_field(self):
        mapper = FieldMapper(mapping={"uf": "microrregiao.mesorregiao.UF.sigla"})
        data = {"microrregiao": {"mesorregiao": {"UF": {"sigla": "PI"}}}}
        result = mapper.transform(data)
        assert result == {"uf": "PI"}

    def test_missing_field_excluded(self):
        mapper = FieldMapper(mapping={"id": "id", "email": "email"})
        result = mapper.transform({"id": 1})
        assert result == {"id": 1}
        assert "email" not in result

    def test_missing_nested_field_excluded(self):
        mapper = FieldMapper(mapping={"uf": "a.b.c"})
        result = mapper.transform({"a": {"b": {}}})
        assert result == {}

    def test_list_of_records(self):
        mapper = FieldMapper(mapping={"id": "id", "name": "nome"})
        data = [{"id": 1, "nome": "A"}, {"id": 2, "nome": "B"}]
        result = mapper.transform(data)
        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "A"}

    def test_invalid_mapping_type_raises(self):
        with pytest.raises(TypeError, match="mapping deve ser dict"):
            FieldMapper(mapping="not_a_dict")

    def test_callable_interface(self):
        mapper = FieldMapper(mapping={"id": "id"})
        result = mapper({"id": 42})
        assert result == {"id": 42}

    def test_deep_nested_value(self):
        mapper = FieldMapper(mapping={"value": "a.b.c.d.e"})
        data = {"a": {"b": {"c": {"d": {"e": "deep"}}}}}
        result = mapper.transform(data)
        assert result == {"value": "deep"}


# ── EnrichTransformer ─────────────────────────────────────────────────────

class TestEnrichTransformer:
    def test_adds_fixed_fields(self):
        transformer = EnrichTransformer(fields={"fonte": "API", "versao": "1.0"})
        result = transformer.transform({"id": 1})
        assert result["fonte"] == "API"
        assert result["versao"] == "1.0"
        assert result["id"] == 1

    def test_adds_timestamp(self):
        transformer = EnrichTransformer(add_timestamp=True)
        result = transformer.transform({"id": 1})
        assert "timestamp" in result

    def test_list_of_records(self):
        transformer = EnrichTransformer(fields={"fonte": "API"})
        result = transformer.transform([{"id": 1}, {"id": 2}])
        assert len(result) == 2
        assert all(r["fonte"] == "API" for r in result)

    def test_overwrites_existing_field(self):
        transformer = EnrichTransformer(fields={"nome": "Novo Nome"})
        result = transformer.transform({"id": 1, "nome": "Antigo"})
        assert result["nome"] == "Novo Nome"

    def test_empty_fields_no_change(self):
        transformer = EnrichTransformer()
        result = transformer.transform({"id": 1})
        assert result == {"id": 1}

    def test_non_dict_passthrough(self):
        transformer = EnrichTransformer(fields={"fonte": "API"})
        assert transformer.transform("string") == "string"


# ── FilterTransformer ─────────────────────────────────────────────────────

class TestFilterTransformer:
    def test_filters_records(self):
        transformer = FilterTransformer(condition=lambda r: r.get("ativo") is True)
        data = [
            {"id": 1, "ativo": True},
            {"id": 2, "ativo": False},
            {"id": 3, "ativo": True},
        ]
        result = transformer.transform(data)
        assert len(result) == 2
        assert all(r["ativo"] is True for r in result)

    def test_empty_list_returns_empty(self):
        transformer = FilterTransformer(condition=lambda r: True)
        assert transformer.transform([]) == []

    def test_all_filtered_out(self):
        transformer = FilterTransformer(condition=lambda r: False)
        result = transformer.transform([{"id": 1}, {"id": 2}])
        assert result == []

    def test_single_dict_matching(self):
        transformer = FilterTransformer(condition=lambda r: r.get("id") > 0)
        result = transformer.transform({"id": 5})
        assert result == {"id": 5}

    def test_single_dict_not_matching(self):
        transformer = FilterTransformer(condition=lambda r: r.get("id") > 10)
        result = transformer.transform({"id": 5})
        assert result == {}

    def test_invalid_condition_raises(self):
        with pytest.raises(TypeError, match="condition deve ser callable"):
            FilterTransformer(condition="not_callable")

    def test_filter_by_numeric_condition(self):
        transformer = FilterTransformer(condition=lambda r: r.get("preco", 0) > 100)
        data = [{"id": 1, "preco": 50}, {"id": 2, "preco": 200}]
        result = transformer.transform(data)
        assert len(result) == 1
        assert result[0]["id"] == 2


# ── TransformPipeline ─────────────────────────────────────────────────────

class TestTransformPipeline:
    def test_applies_sequentially(self):
        pipeline = TransformPipeline([
            FilterTransformer(condition=lambda r: r.get("ativo")),
            EnrichTransformer(fields={"processado": True}),
        ])
        data = [
            {"id": 1, "ativo": True},
            {"id": 2, "ativo": False},
        ]
        result = pipeline.transform(data)
        assert len(result) == 1
        assert result[0]["processado"] is True

    def test_mixed_transformer_and_callable(self):
        pipeline = TransformPipeline([
            lambda data: [r for r in data if r["id"] > 1],
            EnrichTransformer(fields={"via": "pipeline"}),
        ])
        data = [{"id": 1}, {"id": 2}, {"id": 3}]
        result = pipeline.transform(data)
        assert len(result) == 2
        assert all(r["via"] == "pipeline" for r in result)

    def test_single_transformer(self):
        pipeline = TransformPipeline([
            FieldMapper(mapping={"name": "nome"}),
        ])
        result = pipeline.transform([{"nome": "Test"}])
        assert result == [{"name": "Test"}]

    def test_empty_pipeline_returns_original(self):
        pipeline = TransformPipeline([])
        data = [{"id": 1}]
        assert pipeline.transform(data) == data

    def test_invalid_transformer_type_raises(self):
        pipeline = TransformPipeline(["not_a_transformer"])
        with pytest.raises(TypeError, match="Transformer deve ser"):
            pipeline.transform([{"id": 1}])

    def test_callable_interface(self):
        pipeline = TransformPipeline([
            EnrichTransformer(fields={"ok": True}),
        ])
        result = pipeline([{"id": 1}])
        assert result[0]["ok"] is True


# ── IBGETransformer ───────────────────────────────────────────────────────

class TestIBGETransformer:
    def _ibge_record(self, id=1, nome="Parnaíba", uf_sigla="PI", regiao_nome="Nordeste"):
        return {
            "id": id,
            "nome": nome,
            "microrregiao": {
                "mesorregiao": {
                    "UF": {
                        "sigla": uf_sigla,
                        "regiao": {"nome": regiao_nome},
                    }
                }
            },
        }

    def test_extracts_fields(self):
        transformer = IBGETransformer()
        result = transformer.transform([self._ibge_record()])
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["nome"] == "Parnaíba"
        assert result[0]["uf"] == "PI"
        assert result[0]["regiao"] == "Nordeste"

    def test_multiple_records(self):
        transformer = IBGETransformer()
        data = [
            self._ibge_record(id=1, nome="Parnaíba"),
            self._ibge_record(id=2, nome="Teresina"),
        ]
        result = transformer.transform(data)
        assert len(result) == 2

    def test_missing_nested_fields_return_none(self):
        transformer = IBGETransformer()
        result = transformer.transform([{"id": 1, "nome": "X"}])
        assert result[0]["uf"] is None
        assert result[0]["regiao"] is None

    def test_non_list_passthrough(self):
        transformer = IBGETransformer()
        assert transformer.transform("string") == "string"


# ── SelicTransformer ──────────────────────────────────────────────────────

class TestSelicTransformer:
    def test_converts_date_format(self):
        transformer = SelicTransformer()
        result = transformer.transform([{"data": "12/03/2025", "valor": "0.04532"}])
        assert result[0]["data"] == "2025-03-12"

    def test_calculates_annualized_rate(self):
        transformer = SelicTransformer()
        result = transformer.transform([{"data": "01/01/2025", "valor": "0.04532"}])
        assert result[0]["taxa_ao_dia"] == 0.04532
        assert isinstance(result[0]["taxa_anualizada_pct"], float)
        assert result[0]["taxa_anualizada_pct"] > 0

    def test_default_fonte(self):
        transformer = SelicTransformer()
        result = transformer.transform([{"data": "01/01/2025", "valor": "0.04532"}])
        assert result[0]["fonte"] == "BCB SGS 11"

    def test_custom_fonte(self):
        transformer = SelicTransformer(fonte="Minha Fonte")
        result = transformer.transform([{"data": "01/01/2025", "valor": "0.04532"}])
        assert result[0]["fonte"] == "Minha Fonte"

    def test_invalid_date_skipped(self):
        transformer = SelicTransformer()
        result = transformer.transform([
            {"data": "data-invalida", "valor": "0.04532"},
            {"data": "01/01/2025", "valor": "0.04532"},
        ])
        assert len(result) == 1

    def test_invalid_value_skipped(self):
        transformer = SelicTransformer()
        result = transformer.transform([{"data": "01/01/2025", "valor": "abc"}])
        assert result == []

    def test_non_list_passthrough(self):
        transformer = SelicTransformer()
        assert transformer.transform("string") == "string"
