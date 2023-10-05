import pandas as pd
import numpy as np
import random

emme_base="Palt_Koll_2017"
border_links = pd.read_csv("indata/Palt_koll_2017/emme_links_border.csv", sep="\t").to_dict("records")
zones_outside = pd.read_csv("indata/Palt_koll_2017/zones_outside_Le.csv", sep="\t")["ID"].tolist()
nodes_in_Le = pd.read_csv("indata/Palt_koll_2017/nodes_Le.csv", sep="\t")["i"].tolist()
connectors_in_Le =pd.read_csv("indata/Palt_koll_2017/Connectors_Le.csv", sep="\t").to_dict("records")
zones_in_Le =pd.read_csv("indata/Palt_koll_2017/Zones_Le.csv", sep="\t")["i"].tolist()

links = []
connectors = []

used_links = []

def getgate_border(link):
    i = link["i"]
    j = link["j"]
    id = "%s-%s"%(i,j)

    gate = random.choice(zones_outside)
    zones_outside.remove(gate)

    if id not in used_links:

        if j in nodes_in_Le:
            links.append({"i": "%s" % i,"j": "%s" % j, "@gate": gate})
            links.append({"i": "%s" % j, "j": "%s" % i, "@gate": -gate})
            used_links.append(id)
            used_links.append("%s-%s" % (j, i))


        else:
            # links.append({"id": link_id, "@gate": gate})
            links.append({"i": "%s" % i, "j": "%s" % j, "@gate": -gate})
            links.append({"i": "%s" % j, "j": "%s" % i, "@gate": gate})
            used_links.append(id)
            used_links.append("%s-%s" % (j, i))


def getgate_inside(link):
    i = link["i"]
    j = link["j"]
    id = "%s-%s"%(i, j)


    if j in zones_in_Le:
        connectors.append({"i": "%s" % i,"j": "%s" % j, "@gate": -j})
        connectors.append({"i": "%s" % j, "j": "%s" % i, "@gate": j})
        # used_links.append(id)
        # used_links.append("%s-%s" % (j, i))


    # else:
    #     # links.append({"id": link_id, "@gate": gate})
    #     links.append({"i": "%s" % i, "j": "%s" % j, "@gate": -gate})
    #     links.append({"i": "%s" % j, "j": "%s" % i, "@gate": gate})
    #     used_links.append(id)
    #     used_links.append("%s-%s" % (j, i))


[getgate_border(link) for link in border_links]
[getgate_inside (link) for link in connectors_in_Le]

df_border_links = pd.DataFrame(links)
df_border_links= df_border_links[["i", "j", "@gate"]] # change order of columns

if df_border_links["@gate"].sum() != 0:
    print("warning border links dont sum up")
else:

    df_border_links.to_csv("links_gates_%s.csv" % emme_base, index=False, sep=";")

df_connectors_in_Le = pd.DataFrame(connectors)
df_connectors_in_Le= df_connectors_in_Le[["i", "j", "@gate"]]

if df_connectors_in_Le["@gate"].sum() != 0:
    print("warning connectors dont sum up")
else:

    df_connectors_in_Le.to_csv("connectors_%s.csv" % emme_base, index=False, sep=";")


pd.concat([df_border_links, df_connectors_in_Le]).to_csv("to_emme_%s.csv" % emme_base, index=False, sep=";")