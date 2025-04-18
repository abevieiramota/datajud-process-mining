import requests
import json
import pprint

URL = "https://api-publica.datajud.cnj.jus.br/api_publica_tjce/_search"
HEADERS = {
  'Authorization': 'APIKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==',
  'Content-Type': 'application/json'
}
SIZE = 10000


def request_data(search_after, size=SIZE):

    payload = json.dumps(
        {
            "size": size,
            "query": {
                "bool": {
                    "must": [
                        {"match": {"classe.codigo": 1116}}
                    ]
                }
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc"
                    }
                }
            ],
            "search_after": [ search_after ]
        }
    )

    response = requests.request("POST", URL, headers=HEADERS, data=payload)

    return json.loads(response.text)["hits"]["hits"]


def extract_data(processo_json):

    j = processo_json["_source"]
    processo = dict()

    processo["id"] = j["id"]
    processo["nr_processo"] = j["numeroProcesso"]
    processo["classe"] = "{} - {}".format(j["classe"]["codigo"], j["classe"]["nome"])
    processo["sistema"] = j["sistema"]["nome"]
    processo["grau"] = j["grau"]
    processo["dt_ajuizamento"] = j["dataAjuizamento"]
    processo["orgao_julgador"] = "{} - {}".format(j["orgaoJulgador"]["codigo"], j["orgaoJulgador"]["nome"])
    processo["assuntos"] = ["{} - {}".format(a["codigo"], a["nome"]) for a in j["assuntos"]] if "assuntos" in j else []
    processo["movimentacoes"] = [
        {
            "dt_movimentacao": m["dataHora"],
            "bsq_movimentacao": m["codigo"],
            "dsc_movimentacao": m["nome"],
            "complementos": [
                {
                    "cod_tipo": c["codigo"],
                    "dsc_tipo": c["descricao"],
                    "cod_valor": c["valor"],
                    "dsc_valor": c["nome"]
                } for c in m.get("complementosTabelados", [])
            ]
        } for m in j["movimentos"]
    ]

    return processo