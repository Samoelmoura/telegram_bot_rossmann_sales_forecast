# imports
import pandas as pd
import requests
from flask import Flask, request, Response
import os


PATH_REPO = '' # cloud
TOKEN = '6193653024:AAG-DiO6Z8-ACEapNavjWcNy_CfzEEd1gso'

# # setWebhook
# https://api.telegram.org/bot6193653024:AAG-DiO6Z8-ACEapNavjWcNy_CfzEEd1gso/setWebhook?url=https://rossmann-telegram-samoel.herokuapp.com/


def send_message(chat_id, text):
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}'

    r = requests.post(url, json={'text':text})
    print(f'Status code {r.status_code}')

    return None


def load_dataset(store_id):
    # loading datasets
    store = pd.read_csv(PATH_REPO + r'data/raw/store.csv', low_memory=False)
    test = pd.read_csv(PATH_REPO + r'data/raw/test.csv', low_memory=False)

    # mergings
    test_raw = pd.merge(test, store, on='Store', how='left')

    # copying
    test = test_raw.copy()

    # inputing store
    data = test[test['Store'] == store_id]

    # testing if data is empty
    if not data.empty:

        # json format
        data = data.to_json(orient='records')

    else:
        data = 'error'

    return data


def predict(data):
    url = 'https://rossmann-predict-samoel.herokuapp.com//predict'
    headers = {'Content-type':'application/json'}
    data = data

    df10_json = requests.post(url=url, headers=headers, data=data)
    print(f'Status request code: {df10_json.status_code}')

    df = pd.DataFrame(df10_json.json(), columns=df10_json.json()[0].keys())

    return df


def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']
    store_id = store_id.replace('/', '')
    
    try:
        store_id = int(store_id)
    except ValueError:
        store_id = 'error'

    return chat_id, store_id

# api initialize
app = Flask(__name__)
@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        message = request.get_json()
        chat_id, store_id = parse_message(message)

        if store_id != 'error':

            # loading dataset
            data = load_dataset(store_id)

            if data != 'error':
                # predict
                df = predict(data)

                df = df.groupby('Store').agg({'predictions':'sum'}).reset_index()

                loja = df['Store'].values[0]
                venda = df['predictions'].values[0]
                
                msg = f'A loja número {loja} venderá R${venda:.2f} nas próximas 6 semanas.'

                send_message(chat_id, msg)
                return Response('ok', status=200)
            
            else:
                send_message(chat_id, 'Não é uma loja válida.')
                return Response('ok', status=200)

        else: 
            send_message(chat_id, 'Somente números.')
            return Response('ok', status=200)
        
    else:
        return '<h1> Rossmann Telegram Bot </h1>'

if __name__ == '__main__':
    PORT = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=PORT)