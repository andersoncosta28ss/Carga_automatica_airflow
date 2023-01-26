url_base_base = "http://host.docker.internal:3005/"
idCarga = 1
state = "Tete"
url = f"{url_base_base}updateStateOfCharge/?id_charge={idCarga}&state={state}"
print(url)