import csv 
from collections import defaultdict
import pickle
from datajud_api import request_data, extract_data
import pprint


def make_rules():
    situacoes = dict()

    with open("parametrizacao_cnj/param_situacao.csv", encoding="utf-8") as csvfile:
        sitreader = csv.DictReader(csvfile, delimiter=';')
        for row in sitreader:
            situacoes[int(row["bsq_situacao"])] = row["dsc_situacao"]

    with open("parametrizacao_cnj/param_situacao.pkl", "wb") as f:
        pickle.dump(situacoes, f)

    sit_ini_por_mov = defaultdict(list)

    with open("parametrizacao_cnj/param_situacao_ini_mov.csv", encoding="utf-8") as csvfile:
        simreader = csv.DictReader(csvfile, delimiter=";")
        for row in simreader:
            if "sem-complemento" in row["bsq_regra"]:
                sit_ini_por_mov[int(row["bsq_movimentacao_por"])].append(int(row["bsq_situacao_iniciada"]))

    with open("parametrizacao_cnj/param_situacao_ini_mov.pkl", "wb") as f:
        pickle.dump(sit_ini_por_mov, f)


def load_rules():

    with open("parametrizacao_cnj/param_situacao.pkl", "rb") as f:
        situacoes = pickle.load(f)

    with open("parametrizacao_cnj/param_situacao_ini_mov.pkl", "rb") as f:
        sit_ini_por_mov = pickle.load(f)

    return situacoes, sit_ini_por_mov


s, spm = load_rules()

j = request_data(0)
situacoes = []
movimentacoes = []

for p_j in j:

    p = extract_data(p_j)

    for m in p["movimentacoes"]:

        movimentacoes.append({
            "id_processo": p["id"],
            "dt_movimentacao": m["dt_movimentacao"],
            "dsc_movimentacao": "{} - {}".format(m["bsq_movimentacao"], m["dsc_movimentacao"])
        })

        if m["bsq_movimentacao"] in spm:

            for bsq_sit in spm[m["bsq_movimentacao"]]:

                situacao = {
                    "id_processo": p["id"],
                    "data": m["dt_movimentacao"],
                    "situação": s[bsq_sit],
                    "movimentação": "{} - {}".format(m["bsq_movimentacao"], m["dsc_movimentacao"])
                }

                situacoes.append(situacao)

with open("base_movimentacoes.csv", "w", encoding="utf-8", newline="") as csvfile:
    fieldnames = ["id_processo", "dt_movimentacao", "dsc_movimentacao"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(movimentacoes)


with open("base_situacoes.csv", "w", encoding="utf-8", newline="") as csvfile:
    fieldnames = ["id_processo", "data", "situação", "movimentação"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(situacoes)