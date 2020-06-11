import json

def load_memory():
    json_file = "/home/manuel_pereira/Dropbox/Documentos/#os_meus_dados/Google/Histórico de localizações/Histórico de localizações.json"

    print(f'Loading...')
    with open(json_file) as f:
        json_data = json.load(f)
    print(f'Done')
    return json_data
