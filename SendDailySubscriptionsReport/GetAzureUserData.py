
def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
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

def GetAzure():
    import requests
    import pandas as pd
    
    urlAuth = "https://login.microsoftonline.com/stnprodb2c.onmicrosoft.com/oauth2/token"
    urlGraph = "https://graph.windows.net/stnprodb2c.onmicrosoft.com/users"

    payload = "resource=https%3A%2F%2Fgraph.windows.net&client_id=8afde06f-71e4-4b4e-8844-9d0a419c2626&client_secret=OrxIaSe2C3r3t4e?aA/wRxMiVAoIqH::&grant_type=client_credentials"
    headers = {
        'Content-Type': "application/x-www-form-urlencoded",
        'User-Agent': "PostmanRuntime/7.19.0",
        'Accept': "*/*",
        'Host': "login.microsoftonline.com",
        'Accept-Encoding': "gzip, deflate",
        'Content-Length': "164",
        'Cookie': "esctx=AQABAAAAAAAP0wLlqdLVToOpA4kwzSnx6FWtReqpH_uGysGgQVSopRZH4NX5FPmcCb2oSADhCAfkb6QdwziHeSADOltd0zYL9Ngouk_KPQ739PvdwvPWVhPHrFYXowy3F0G3g15KqywOXBkiszUNdI9WviwfzgI0GpQtECZSfex9Jorb4VZEHdJ7Nu8KWbuUDNcC_E2dCjUgAA; MSCC=1567068096; fpc=AluQHeTOPE9MhOIQc80JXS-E_YkIAQAAAOBoYNUOAAAA; x-ms-gateway-slice=prod; stsservicecookie=ests",
        'Connection': "keep-alive"
        }

    response = requests.request("POST", urlAuth, data=payload, headers=headers)
    jsonData = response.json()
    print(jsonData)
    access_token = jsonData["access_token"]
    print('this is the token: {}'.format(access_token))

    print('--------------------------------------------------------------------------------------------')
    print('--------------------------------------------------------------------------------------------')

    querystring = {"api-version":"1.6"}
    headers = {
        'Authorization': "Bearer " + access_token,
        'User-Agent': "PostmanRuntime/7.19.0",
        'Accept': "*/*",
        'Host': "graph.windows.net",
        'Accept-Encoding': "gzip, deflate",
        'Connection': "keep-alive"
        }

    responseGraph = requests.request("GET", urlGraph, headers=headers, params=querystring)
    graph_json = responseGraph.json()
    print('Azure Prod Users {} '.format(graph_json))

    b2cvalues = graph_json["value"]

    # pd_json = pd.read_json(graph_json)
    # print(pd_json.head())

    # df = json_normalize(graph_json)
    # print(df)
    # df.to_csv('results.csv')

    df = pd.DataFrame([flatten_json(x) for x in b2cvalues])

    #df = pd.io.json.json_normalize(b2cvalues)
    #df = pd.io.json.json_normalize(b2cvalues, record_path='signInNames', meta=['type', 'value'])
    #df = json_normalize(b2cvalues, record_path='signInNames', meta=['type', 'value'], record_prefix='signInNames_')
    #df.columns = df.columns.map(lambda x: x.split(".")[-1])

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #     print(df)

    df.drop(['odata.type' ,'objectType' ,'objectId' ,'deletionTimestamp' ,'accountEnabled',  'ageGroup' ,'city' ,'companyName' ,'consentProvidedForMinor' ,'country',  'creationType' ,'department' ,'dirSyncEnabled',  'displayName' ,'employeeId' ,'facsimileTelephoneNumber' ,'givenName' , 'immutableId' ,'isCompromised' ,'jobTitle' ,'lastDirSyncTime',  'legalAgeGroupClassification' ,'mail' ,'mailNickname' ,'mobile',  'onPremisesDistinguishedName' ,'onPremisesSecurityIdentifier',  'passwordPolicies' ,'passwordProfile' ,'physicalDeliveryOfficeName',  'postalCode' ,'preferredLanguage' ,'refreshTokensValidFromDateTime',  'showInAddressList' ,'signInNames_0_type' ,  'sipProxyAddress' ,'state' ,'streetAddress' ,'surname' ,'telephoneNumber',  'thumbnailPhoto@odata.mediaEditLink' ,'usageLocation' ,'userPrincipalName',  'userState' ,'userStateChangedOn' ,'userType',  'extension_492028cb23044e06a66775c81fe5d411_features',  'extension_492028cb23044e06a66775c81fe5d411_plan_id',  'extension_492028cb23044e06a66775c81fe5d411_subscription_id',  'extension_492028cb23044e06a66775c81fe5d411_account_id',  'extension_492028cb23044e06a66775c81fe5d411_subscription_status',  'otherMails_0' ,'proxyAddresses_0'], axis=1, inplace=True)
    print(df.columns.values)
    df = df[df["signInNames_0_value"].str.contains('safetonet|protonmail|stnqa')==False]
    df.sort_values(["createdDateTime", "extension_492028cb23044e06a66775c81fe5d411_subscription_start", "signInNames_0_value"], inplace = True)
    df.drop_duplicates(subset ="signInNames_0_value", keep = False, inplace = True)
    df['createdDateTime'] = df['createdDateTime'].apply(lambda x: x[:10])
    df=df.fillna("NODATA")

    df.to_html('azure-report.html')
    df.to_csv('azure-report.csv')

    counts = df.groupby(['createdDateTime','extension_492028cb23044e06a66775c81fe5d411_plan_name']).size()
    print(counts)

    df = pd.DataFrame(counts)


    df.to_html("azure_result.html")
    print('+++++++++++++++++++++++++++++++++++++++++++++++++')
    return df.to_html()
