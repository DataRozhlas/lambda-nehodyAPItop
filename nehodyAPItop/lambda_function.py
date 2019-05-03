import json
from boto3.dynamodb.conditions import Key, Attr
import base64
import boto3
import statistics


s3 = boto3.client('s3')
dyn = boto3.resource('dynamodb', region_name='eu-central-1')
table = dyn.Table('nehody')


def lambda_handler(event, context):
    e = eval(base64.b64decode(event['q']))
    resp = table.query(
        KeyConditionExpression=Key('primkey').eq(1) & Key('tstamp').between(e['from'], e['to'])
    )
    
    if len(resp['Items']) == 0: # pokud to nevrati nic
        return {
            'bounds': {
                'from': e['from'],
                'to': e['to']
            }
        }
    
    tmp = {}
    tmp['čr'] = {
        'PN': [],
        'M': [],
        'LR': [],
        'PVA': []
    }
    
    dates = []
    dates_cr = []
    for i in resp['Items']:
        dates.append(i['tstamp'])
        dates_cr.append(i['tstamp'])
        cr_tmp = {
            'PN': 0,
            'M': 0,
            'LR': 0,
            'PVA': 0
        }
        
        for kraj in i['data']:
            if kraj not in tmp:
                tmp[kraj] = {}
                tmp[kraj].update({
                    'PN': [],
                    'M': [],
                    'LR': [],
                    'PVA': []
                })
            tmp[kraj]['PN'].append(i['data'][kraj]['PN'])
            tmp[kraj]['M'].append(i['data'][kraj]['M'])
            tmp[kraj]['LR'].append(i['data'][kraj]['LR'])
            tmp[kraj]['PVA'].append(i['data'][kraj]['PVA'])
            
            cr_tmp['PN'] += i['data'][kraj]['PN']
            cr_tmp['M'] += i['data'][kraj]['M']
            cr_tmp['LR'] += i['data'][kraj]['LR']
            cr_tmp['PVA'] += i['data'][kraj]['PVA']
            
        tmp['čr']['PN'].append(cr_tmp['PN'])
        tmp['čr']['M'].append(cr_tmp['M'])
        tmp['čr']['LR'].append(cr_tmp['LR'])
        tmp['čr']['PVA'].append(cr_tmp['PVA'])
        
    out = {}
    for kraj in tmp:
        if kraj == 'čr':
            continue
        out[kraj] = {
            'PN': {
                'min': min(tmp[kraj]['PN']), 
                'min_day': dates[tmp[kraj]['PN'].index(min(tmp[kraj]['PN']))], 
                'max': max(tmp[kraj]['PN']), 
                'max_day': dates[tmp[kraj]['PN'].index(max(tmp[kraj]['PN']))], 
                'mean': statistics.mean(tmp[kraj]['PN']), 
                'median': statistics.median(tmp[kraj]['PN'])
            },
            'M': {
                'min': min(tmp[kraj]['M']), 
                'min_day': dates[tmp[kraj]['M'].index(min(tmp[kraj]['M']))], 
                'max': max(tmp[kraj]['M']), 
                'max_day': dates[tmp[kraj]['M'].index(max(tmp[kraj]['M']))],
                'mean': statistics.mean(tmp[kraj]['M']), 
                'median': statistics.median(tmp[kraj]['M'])
            },
            'LR': {
                'min': min(tmp[kraj]['LR']), 
                'min_day': dates[tmp[kraj]['LR'].index(min(tmp[kraj]['LR']))], 
                'max': max(tmp[kraj]['LR']), 
                'max_day': dates[tmp[kraj]['LR'].index(max(tmp[kraj]['LR']))],
                'mean': statistics.mean(tmp[kraj]['LR']), 
                'median': statistics.median(tmp[kraj]['LR'])
            },
            'PVA': {
                'min': min(tmp[kraj]['PVA']), 
                'min_day': dates[tmp[kraj]['PVA'].index(min(tmp[kraj]['PVA']))],
                'max': max(tmp[kraj]['PVA']), 
                'max_day': dates[tmp[kraj]['PVA'].index(max(tmp[kraj]['PVA']))],
                'mean': statistics.mean(tmp[kraj]['PVA']), 
                'median': statistics.median(tmp[kraj]['PVA'])
            }
        }
        
    #CR
    out['ČR'] = {
        'PN': {
            'min': min(tmp['čr']['PN']), 
            'min_day': dates_cr[tmp['čr']['PN'].index(min(tmp['čr']['PN']))], 
            'max': max(tmp['čr']['PN']), 
            'max_day': dates_cr[tmp['čr']['PN'].index(max(tmp['čr']['PN']))], 
            'mean': statistics.mean(tmp['čr']['PN']), 
            'median': statistics.median(tmp['čr']['PN'])
        },
        'M': {
            'min': min(tmp['čr']['M']), 
            'min_day': dates_cr[tmp['čr']['M'].index(min(tmp['čr']['M']))], 
            'max': max(tmp['čr']['M']), 
            'max_day': dates_cr[tmp['čr']['M'].index(max(tmp['čr']['M']))],
            'mean': statistics.mean(tmp['čr']['M']), 
            'median': statistics.median(tmp['čr']['M'])
        },
        'LR': {
            'min': min(tmp['čr']['LR']), 
            'min_day': dates_cr[tmp['čr']['LR'].index(min(tmp['čr']['LR']))], 
            'max': max(tmp['čr']['LR']), 
            'max_day': dates_cr[tmp['čr']['LR'].index(max(tmp['čr']['LR']))],
            'mean': statistics.mean(tmp['čr']['LR']), 
            'median': statistics.median(tmp['čr']['LR'])
        },
        'PVA': {
            'min': min(tmp[kraj]['PVA']),
            'min_day': dates_cr[tmp['čr']['PVA'].index(min(tmp['čr']['PVA']))],
            'max': max(tmp['čr']['PVA']), 
            'max_day': dates_cr[tmp['čr']['PVA'].index(max(tmp['čr']['PVA']))],
            'mean': statistics.mean(tmp['čr']['PVA']), 
            'median': statistics.median(tmp['čr']['PVA'])
        }
    }
    
    out.update({
        'bounds': {
            'from': min(dates),
            'to': max(dates)
        }
    })
    return out