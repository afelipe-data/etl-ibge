def extrai_api():
        data_path = '/tmp/dimensao_mesorregioes_mg.csv'
        url = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/MG/mesorregioes'
        response = requests.get(url)
        response_json = json.loads(response.text)
        df = pd.DataFrame(response_json)[['id','nome']]
        df.to_csv(data_path, index=False, encoding='utf-8', sep=';')
        return data_path
