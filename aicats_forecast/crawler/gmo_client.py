import requests


class GMOClient:

    def ticker(self, symbol: str = 'BTC'):
        url = f'https://api.coin.z.com/public/v1/ticker?symbol={symbol}'
        res = requests.get(url)
        return res.json()['data'][0]


class GMOWSClient:
    pass
