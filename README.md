# basededadosdeusinas

## bdt tb_empreendimento
### tabela bdt_tb_empreendimento

- id_empreendimento
- nom_completo
- nom_reduzido
- id_tipo_empreend
- id_municipio_1
- Nome com ID (concatenação de id_empreendimento, nom_completo e nom_reduzido)

> A tabela `bdt_tb_empreendimento` contém informações detalhadas sobre os empreendimentos, incluindo o nome completo, nome reduzido e identificadores de tipo de empreendimento.

## Fonte de Dados
- **Banco de dados SQL Inbound**: prd-inst-bi2.ons.org.br,2058
- **Tabela principal**: bdt.tb_empreendimento

### Relacionamentos:
1. **bdt.tb_empreendcontrato** (relacionado por `id_empreendimento`)
   - Colunas expandidas: `id_contrato`
   
2. **bdt.tb_contrato** (relacionado por `id_contrato`)
   - Colunas expandidas: `id_tipo_contrato`
   
3. **bdt.tb_usianeelvisaoons** (relacionado por `cod_chavevisaousions`)
   - Colunas expandidas: `id_usianeel`
   
4. **bdt.tb_usianeel** (relacionado por `id_usianeel`)
   - Colunas expandidas: `id_usianeel` (foi mantida após o relacionamento)

5. **bdt.tb_usianeelvig** (relacionado por `id_usianeel`)
   - Colunas expandidas: `cod_cegcompleto`

## Modelagem de dados
### Power Query

```powerquery
let
    Fonte = Sql.Databases("prd-inst-bi2.ons.org.br,2058"),
    INBOUND = Fonte{[Name="INBOUND"]}[Data],
    bdt_tb_empreendimento = INBOUND{[Schema="bdt",Item="tb_empreendimento"]}[Data],
    #"Linhas Filtradas" = Table.SelectRows(bdt_tb_empreendimento, each ([id_tipo_empreend] <> 6 and [id_tipo_empreend] <> 7 and [id_tipo_empreend] <> 8 and [id_tipo_empreend] <> 9 and [id_tipo_empreend] <> 13 and [id_tipo_empreend] <> 15)),
    #"Colunas Removidas" = Table.RemoveColumns(#"Linhas Filtradas",{"id_empreendimentodistribuicao", "flg_conspotlivre", "flg_necessitagarantiafinanceira", "hor_pontahornormal", "hor_pontahorverao", "id_tpindiceeconomico", "din_inclusao", "din_alteracao", "flg_empreendatendedecreto", "flg_licitadaren782", "flg_licitado", "flg_database", "flg_consorcio", "nom_consorcio", "obs_empreendimento"}),
    #"Tipo Alterado" = Table.TransformColumnTypes(#"Colunas Removidas",{{"id_empreendimento", type text}}),
    #"Tipo Alterado com Localidade" = Table.TransformColumnTypes(#"Tipo Alterado", {{"nom_completo", type text}}, "la-001"),
    #"Tipo Alterado com Localidade1" = Table.TransformColumnTypes(#"Tipo Alterado com Localidade", {{"nom_reduzido", type text}}, "la-001"),
    #"Personalização Adicionada" = Table.AddColumn(#"Tipo Alterado com Localidade1", "Nome com ID", each Text.From([id_empreendimento]) & " :: " & Text.From([nom_completo]) & " :: " & Text.From([nom_reduzido])),
    #"Tipo Alterado1" = Table.TransformColumnTypes(#"Personalização Adicionada",{{"Nome com ID", type text}, {"id_empreendimento", Int64.Type}}),
    #"Consultas Mescladas" = Table.NestedJoin(#"Tipo Alterado1", {"id_empreendimento"}, #"bdt tb_empreendcontrato", {"id_empreendimento"}, "bdt tb_empreendcontrato", JoinKind.LeftOuter),
    #"bdt tb_empreendcontrato Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas", "bdt tb_empreendcontrato", {"id_contrato"}, {"bdt tb_empreendcontrato.id_contrato"}),
    #"Consultas Mescladas1" = Table.NestedJoin(#"bdt tb_empreendcontrato Expandido", {"bdt tb_empreendcontrato.id_contrato"}, #"bdt tb_contrato", {"id_contrato"}, "bdt tb_contrato", JoinKind.LeftOuter),
    #"bdt tb_contrato Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas1", "bdt tb_contrato", {"id_tipo_contrato"}, {"bdt tb_contrato.id_tipo_contrato"}),
    #"Linhas Filtradas1" = Table.SelectRows(#"bdt tb_contrato Expandido", each [bdt tb_contrato.id_tipo_contrato] <> null and [bdt tb_contrato.id_tipo_contrato] <> ""),
    #"Duplicatas Removidas" = Table.Distinct(#"Linhas Filtradas1", {"id_empreendimento"}),
    #"Colunas Removidas1" = Table.RemoveColumns(#"Duplicatas Removidas",{"bdt tb_contrato.id_tipo_contrato", "bdt tb_empreendcontrato.id_contrato"}),
    #"Consultas Mescladas2" = Table.NestedJoin(#"Colunas Removidas1", {"id_empreendimento"}, #"bdt tb_usianeelvisaoons", {"cod_chavevisaousions"}, "bdt tb_usianeelvisaoons", JoinKind.LeftOuter),
    #"bdt tb_usianeelvisaoons Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas2", "bdt tb_usianeelvisaoons", {"id_usianeel"}, {"bdt tb_usianeelvisaoons.id_usianeel"}),
    #"Consultas Mescladas3" = Table.NestedJoin(#"bdt tb_usianeelvisaoons Expandido", {"bdt tb_usianeelvisaoons.id_usianeel"}, #"bdt tb_usianeel", {"id_usianeel"}, "bdt tb_usianeel", JoinKind.LeftOuter),
    #"bdt tb_usianeel Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas3", "bdt tb_usianeel", {"id_usianeel"}, {"bdt tb_usianeel.id_usianeel"}),
    #"Consultas Mescladas4" = Table.NestedJoin(#"bdt tb_usianeel Expandido", {"bdt tb_usianeel.id_usianeel"}, #"bdt tb_usianeelvig", {"id_usianeel"}, "bdt tb_usianeelvig", JoinKind.LeftOuter),
    #"bdt tb_usianeelvig Expandido" = Table.ExpandTableColumn(#"Consultas Mescladas4", "bdt tb_usianeelvig", {"cod_cegcompleto"}, {"cod_cegcompleto"}),
    #"Colunas Removidas2" = Table.RemoveColumns(#"bdt tb_usianeelvig Expandido",{"bdt tb_usianeelvisaoons.id_usianeel", "bdt tb_usianeel.id_usianeel"}),
    #"Duplicatas Removidas1" = Table.Distinct(#"Colunas Removidas2", {"id_empreendimento"}),
    #"Colunas Removidas3" = Table.RemoveColumns(#"Duplicatas Removidas1",{"id_pais"}),
    #"Removido Vazio" = Table.SelectRows(#"Colunas Removidas3", each ([id_municipio_1] <> null))
in
    #"Removido Vazio"
```
