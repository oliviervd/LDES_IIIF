import subprocess
import pandas as pd
import json
from urllib.request import urlopen
from io import BytesIO

from PIL.Image import Image
import requests

## 1. fetch json from LDES
import requests as requests
from PIL.Image import Image

fetch_from = "2022-03-00T00:00:00.309Z"
context = "src/utils/context.jsonld"
filepath = "src/data/dmg_obj.json"

endpoint = f"actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context " + context + " --fromTime 2022-03-00T00:00:00.309Z --emitMemberOnce false --disablePolling true https://apidg.gent.be/opendata/adlib2eventstream/v1/dmg/objecten"

def fetch_json(filepath):
    with open(filepath, "w") as f:
        subprocess.run(endpoint, shell=True, stdout=f, text=True)

## fetch_json(filepath)

## 2.parse result into dataframe

def generate_dataframe(filepath):
    with open(filepath) as p:
        res = p.read()
        result = res.splitlines()
        print("done with parsing data from DMG")
        return result

df_dmg = pd.DataFrame(generate_dataframe(filepath))
columns = ["URI", "timestamp", "title", "IIIF_manifest", "IIIF_image", "HEX_values"]

##define actions

def fetch_title(df, range, _json):
    try:
        title = _json["http://www.cidoc-crm.org/cidoc-crm/P102_has_title"]
        df.at[range, "title"] = title["@value"]
    except Exception:
        pass

def fetch_IIIF(df, range, _json):
    try:
        IIIF_manifest = _json["Entiteit.isHetOnderwerpVan"][0]["@id"]
        df.at[range, "IIIF_manifest"] = IIIF_manifest
    except Exception:
        pass

def fetch_image(df, range, _json):
    try:
        url = _json["Entiteit.isHetOnderwerpVan"][0]["@id"]
        response = urlopen(url)
        data_json = json.loads(response.read())
        _im = data_json["sequences"][0]['canvases'][0]["images"][0]["resource"]["@id"]
        df.at[range, "IIIF_image"] = _im
    except Exception:
        pass

def fetch_timestamp(df, range, _json):
    try:
        time_stamp = _json["@id"]
        time_stamp = time_stamp.split("/")[-1]
        df.at[range, "timestamp"] = time_stamp
    except Exception:
        pass

for i in range(0, len(columns)):
    df_dmg.insert(i, columns[i], "")

for i in range(0, 10): ##change to len(df_dmg to fetch all)
    x = df_dmg.loc[i]
    j = json.loads(x[0])

    uri = j["http://purl.org/dc/terms/isVersionOf"]["@id"]
    df_dmg.at[i, "URI"] = uri

    fetch_title(df_dmg, i, j)
    fetch_IIIF(df_dmg, i, j)
    fetch_image(df_dmg, i, j)
    fetch_timestamp(df_dmg, i, j)

    print("image "+str(i)+ " done")

df_dmg.to_csv("df_dmg.csv")