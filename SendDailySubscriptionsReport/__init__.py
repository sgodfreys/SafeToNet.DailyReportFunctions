from pymongo import MongoClient
from bson.json_util import dumps
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from . import GetAzureUserData
import datetime
import logging

import azure.functions as func

def flatten_json(nested_json, exclude=['']):
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

def main(mytimer: func.TimerRequest) -> None:
    logging.info('About to run the daily subs report function!')

    client = MongoClient()
    client = MongoClient('mongodb://stn-euw-cosmos-be-prod:LPpx7e9k8OMlzUW7a0Xj7EjwQPm4rgz0m5c8TRgrgp8szm5yDKmEyHSTXkLUE5Z8jv2OzZhG8fH7bnLAq9Zo7Q==@stn-euw-cosmos-be-prod.documents.azure.com:10255/?ssl=true&replicaSet=globaldb')

    db = client['subscriptions']
    getData = db.get_collection('subscription')
    cursor = getData.find({})
    df = pd.DataFrame([flatten_json(x) for x in cursor])

    df.drop(['_id', 'account_id', 'google_receipt', 'apple_receipt', 'plan_id',  'plan_teaser_title', 'plan_teaser_subtitle', 'plan_teaser_content',  'plan_description', 'plan_version', 'plan_period', 'plan_price',  'plan_currency', 'plan_autorenew', 'plan_features_0_name',  'plan_features_0_description', 'plan_features_0_id', 'plan_features_1_name',  'plan_features_1_description', 'plan_features_1_id', 'plan_features_2_name',  'plan_features_2_description', 'plan_features_2_id',  'plan_google_product_id', 'plan_apple_product_id', 'apple_subscriptions',  'google_subscription', 'plan_features_3_name',  'plan_features_3_description', 'plan_features_3_id', 'plan_features_4_name',  'plan_features_4_description', 'plan_features_4_id', 'plan_features_5_name',  'plan_features_5_description', 'plan_features_5_id'], axis=1, inplace=True)
    print(df.columns.values)

    df = df[df["account_email"].str.contains('safetonet|protonmail|stnqa')==False]
    df.sort_values("account_email", inplace = True)
    df.drop_duplicates(subset ="account_email", keep = False, inplace = True)

    result = df.groupby(['start','plan_name']).mean()
    print(result)
    counts = df.groupby(['start','plan_name']).size()
    print(counts)
    result['count'] = counts
    print(result)

    df = pd.DataFrame(counts)

    df.to_html("sub_result.html")

    email = "<html> <head> </head> <body> <h1>Daily Database Subscriptions Report </h1> <hr> {df} <hr> <h1>Daily Azure Subscriptions Report </h1> <hr> {azuredf} </body> </html>"

    email = email.format(df=df.to_html(), azuredf=GetAzureUserData.GetAzure())
    print(email)

    message = Mail(
        from_email='sgodfrey@safetonet.com',
        to_emails=['sgodfrey@safetonet.com', 'hmohr@safetonet.com', 'jpursey@safetonet.com', 'ibrown@safetonet.com'],
        #to_emails=['sgodfrey@safetonet.com'],
        subject='Automated daily subscription report',
        html_content=email)
    try:
        sg = SendGridAPIClient('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')
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e.message)


    logging.info('Completed the daily subs report function!')
