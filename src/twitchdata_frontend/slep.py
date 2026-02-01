import asyncio
import redis.asyncio as redis
from nicegui import background_tasks,app,ui

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Annotated

from fastapi import Query

from twitchdata_redis import RedisCM

import logging
import os

from time import perf_counter
DT_TIMEZONE = 'America/Chicago'

def pretty_time_delta(seconds):
    if not seconds:
        return f"0 seconds"
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    measures = (
        (days, "d"),
        (hours, "h"),
        (minutes, "m"),
        (seconds, "s"),
    )
    return "".join([f"{count}{noun}" for (count, noun) in measures if count])

def get_date_str(d):
    return datetime.fromtimestamp(d/1000,timezone.utc).astimezone(ZoneInfo(DT_TIMEZONE)).strftime("%m/%d %H:%M:%S")

async def addNew(r,key,grid_live,cached):
  try:  
    b = await r.getStreamData(key)       
    b['start datetime']=get_date_str(b['start time'])      
    b['stream length seconds']=int(datetime.now(timezone.utc).timestamp()-(b['start time']/1000))                    
    if not b['id'] in cached:
        cached[b['id']]={'name':b['name'],'start_str':b['start datetime'],'start':b['start time']}
        await grid_live.run_grid_method("applyTransactionAsync",{ 'add': [b] })
        print("added",b['name'])
        
  except TimeoutError:
    print('addNew timeour') 
  except asyncio.CancelledError:
    print('addNew cancelled')      
  
async def updateData(r,key,sid,grid_live,grid_history,cached):
  try:
    b = await r.getStreamData(key,cached[sid]['name'])                
    b['start datetime']=cached[b['id']]['start_str']              
    if 'end time' in b:
        b['stream length'] = pretty_time_delta(b['stream length seconds'])
        b['end time'] = get_date_str(b['end time'])
        b['viewers'] = ''
        cached.pop(b['id'])
        #print("moo",b)    
        await grid_live.run_grid_method("applyTransactionAsync",{ 'remove': [{'id':f"{b['id']}"}] })
        #print("movee",b['name'],b["total messages"])    
        grid_history.options['rowData'].insert(0, b)
        print("moved",b['name'],b["total messages"])        
    else:  
        if 'seconds' in cached[b['id']]:
            b['stream length']=cached[b['id']]['seconds']
        if 'secs' in cached[b['id']]:    
            b['stream length seconds']=cached[b['id']]['secs']         
        grid_live.run_grid_method("applyTransaction",{ 'update': [b] })
        #print(b['name'],b["total messages"])  
  except asyncio.CancelledError:
    print('updateData cancelled')
  except TimeoutError:
    print('updateData timeour')
    
async def message_reader(l,r,g_live,g_history,cached,tt):
    
    async with redis.Redis(connection_pool=pool).pubsub() as pubsub:
        try:
          print("2-Elapsed time after pubsub[",await pubsub.psubscribe(*[f"__keyspace@0__:stream:*:{i}:data" for i in (l if l else ['*'])]),']:',perf_counter() - tt)
          async with asyncio.TaskGroup() as tg:  
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if message is not None:
                    if message['data']=='hset':
                        sid=message['channel'].split(':')[2]
                        if sid in cached:
                            tg.create_task(updateData(r,message['channel'].split(':',1)[1],sid,g_live,g_history,cached))
                        else:
                            tg.create_task(addNew(r,message['channel'].split(':',1)[1],g_live,cached))
                    else:    
                        print(f"(Reader) Message Received: {message}") 
                    
        except asyncio.CancelledError:
    #playsound('90143__pengo_au__steam_burst.wav',False)   
            print("ws cancelled...")        

async def reader(channel: redis.client.PubSub,r,grid,d,g2):
    try:
      while True:

        message = await channel.get_message(ignore_subscribe_messages=True, timeout=None)
        if message is not None:
            #print(f"(Reader) Message Received: {message}")
            if message['data']=='hset':
                if message['channel'].split(':')[2] in d:
                    b = await r.getStreamData(message['channel'].split(':',1)[1],d[message['channel'].split(':')[2]]['name'])                
                    b['start datetime']=d[b['id']]['start_str']              
                    if 'end time' in b:
                        b['stream length'] = pretty_time_delta(b['stream length seconds'])
                        b['end time'] = get_date_str(b['end time'])
                        b['viewers'] = ''
                        d.pop(b['id'])
                        grid.run_grid_method("applyTransaction",{ 'remove': [b] })
                        g2.options['rowData'].insert(0, b)                      
                    else:  
                        if 'seconds' in d[b['id']]:
                            b['stream length']=d[b['id']]['seconds']
                            b['stream length seconds']=d[b['id']]['secs']
                        grid.run_grid_method("applyTransactionAsync",{ 'update': [b] })
                else:
                    #print(f"new: {message['channel'].split(':',1)[1].rsplit(':',1)[0]}:name")
                    b = await r.getStreamData(message['channel'].split(':',1)[1])       
                    b['start datetime']=get_date_str(b['start time'])      
                    b['stream length seconds']=int(datetime.now(timezone.utc).timestamp()-(b['start time']/1000))                    
                    if not b['id'] in d:
                        d[b['id']]={'name':b['name'],'start_str':b['start datetime'],'start':b['start time']}
                        grid.run_grid_method("applyTransactionAsync",{ 'add': [b] })

            else:    
                print(f"(Reader) Message Received: {message}")    
    except asyncio.CancelledError:
    #playsound('90143__pengo_au__steam_burst.wav',False)   
      print("ws cancelled...")    
async def on_timer(d,g):
    #print(datetime.utcnow().timestamp()-v['start']/1000)
    #for k,v in d.items():
    i = 0
    for k in list(d):
        i += 1
        if k in d:
            v = d[k]
        #print(datetime.utcnow().timestamp(),v['start']/1000)
            f = datetime.now(timezone.utc).timestamp()*1000-(v['start'])
            v['secs']=int(f/1000)
            v['seconds']=pretty_time_delta(v['secs'])
            #tt = perf_counter() 
            rr = await g.run_row_method(k, 'setDataValue', 'stream length', v['seconds'])
            #print(rr,perf_counter()-tt,k,i)
            if f>7200000 and f<7201400:
            #print("fix",v)
                print(await g.run_row_method(k, 'setDataValue', 'stream length seconds', v['secs']),await g.run_grid_method("refreshCells"),"fix",v['secs'])
 

async def subscribe(ps,channel,l):
    if l:
        for s in l:
            print(channel.format(s))
            print(await ps.psubscribe(channel.format(s)))
    else:
        print(await ps.psubscribe(channel.format('*')))
      
@ui.page('/')
async def page(l: Annotated[list[str] | None, Query()] = None):
    def stateUpdated(event):
        print('hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')
        app.storage.user['state'] = event.state
    print(l)
    ui.add_css('''
    .is_starting {
        background-color: #cc333344;
    }
''')
    ui.add_css('''
    body .myshit {
        --ag-spacing: 2px;
        --ag-foreground-color: grey;
        --ag-background-color: rgb(255, 249, 241);*/
        --header-background-color: rgb(228, 237, 250);
    }
    body .ag-cell-data-changed{
#       --ag-value-change-value-highlight-background-color: #cc222244;
    }

    .ag-cell-none2h{
        color: darkgrey
    }

 
''')
    '''
        .ag-header .ag-pinned-left-header,
    .ag-body-viewport .ag-pinned-left-cols-container {
        position: relative;
        z-index: 1;
        -webkit-box-shadow: 9px 1px 15px -4px rgba(0, 0, 0, 0.1);
        box-shadow: 9px 1px 15px -4px rgba(0, 0, 0, 0.1);
    }
    .ag-body-viewport .ag-pinned-left-cols-container {
        position: relative;
        z-index: 1;
        -webkit-box-shadow: 9px 1px 15px -4px rgba(0, 0, 0, 0.1);
        box-shadow: 9px 1px 15px -4px rgba(0, 0, 0, 0.1);
    }
    .ag-body-viewport .ag-pinned-left-cols-container {
        position: relative;
        z-index: 1;
        -webkit-box-shadow: 9px 1px 15px -4px rgba(0, 0, 0, 0.1);
        box-shadow: 9px 1px 15px -4px rgba(0, 0, 0, 0.1);
    }
    '''
    #ui.dark_mode().enable()    
    with ui.column().classes('w-full h-[calc(100vh-2rem)]'):
    #with ui.card().classes('w-full h-[calc(100vh-2rem)]'):    
        with ui.splitter(horizontal=True).classes('w-full h-full').props('before-class=overflow-hidden after-class=overflow-hidden') as splitter:   
          with splitter.before:      
            grid = ui.aggrid({         
                'defaultColDef': {
                    'width': 64,
                    'suppressMovable': True,
             #       'wrapHeaderText': True, 
                    'autoHeaderHeight': True,
                    'resizable': False,
                    'suppressSizeToFit': True,
                    'enableCellChangeFlash': True,
                    'sortingOrder': ["desc", "asc"]
                },
                'columnDefs': [              
                    {'field': 'name', 'width': 110,'headerTooltip': 'Checkboxes indicate selection','pinned':'left','cellStyle': {'font-weight': 'bold'},'sortingOrder': ["desc", "asc"]},
                    {'field': 'unique chatters','headerTooltip': 'Unique Chatters', 'type': 'numericColumn'},
                    {'headerName': 'Uq Ch Non Shared','headerTooltip': 'Unique Chatters Non Shared', 'field': 'unique chatters non shared', 'type': 'numericColumn'},
                    {'field': 'unique chatters subbed','headerTooltip': 'Unique Chatters Subbed', 'type': 'numericColumn'},
                    {'headerName': 'Uq Ch Subbed %','headerTooltip': 'Unique Chatters Subbed %', 'field': 'unique chatters subbed pct', ':valueFormatter': '(p) => p.value?p.value/10+"%":""', 'type': 'numericColumn'},
                    {'field': 'avg viewers','headerTooltip': 'Average Viewers',  'type': 'numericColumn', 'width': 70},
                    {'field': 'total messages','headerTooltip': 'Total Messages', 'type': 'numericColumn', 'width': 74},
                    {'headerName': 'From Shared','headerTooltip': 'Messages From Shared Chat', 'field': 'messages from shared chat',  'type': 'numericColumn'},
                    {'headerName': 'Own Messages %','headerTooltip': 'Own Messages % From Shared Chat', 'field': 'own messaged percent from all', ':valueFormatter': '(p) => p.value?p.value/10+"%":""', 'type': 'numericColumn'},
                    {'field': 'unique subs','headerTooltip': 'Subs', 'type': 'numericColumn', 'width': 55},
                    {'field': 'subs non gifted','headerTooltip': 'Subs Non Gifted', 'type': 'numericColumn', 'width': 55},
                    {'field': 'subs gifted','headerTooltip': 'Subs Gifted', 'type': 'numericColumn', 'width': 55},
                    {'field': 'bits','headerTooltip': 'Bits', 'type': 'numericColumn', 'width': 55},
                    {'field': 'viewers','headerTooltip': 'Viewers', 'type': 'numericColumn', 'width': 90, 'cellStyle': { 'color': '#FF7F50' },'cellRenderer': "agAnimateShowChangeCellRenderer"},
                    {'headerName': 'Mes Last Min','headerTooltip': 'Messages Last Minute', 'field': 'messages lastmin', 'type': 'numericColumn', 'width': 55},
                    {'headerName': 'Avg Per Min(20 min)','headerTooltip': 'Average Messages Per Minute Last 20 Minutes', 'field': 'average messages per min last 20 min', 'type': 'numericColumn'},
                    {'headerName': 'Uq Ch Avg 10min','headerTooltip': 'Unique Chatters Last 10 Minutes', 'field': 'uninque chatters average over 10 min',  'type': 'numericColumn'},
                    {'field': 'unique chatters first 2h','headerTooltip': 'Unique Chatters First 2 Hours', 'type': 'numericColumn',':cellClassRules':{'ag-cell-none2h': "data['stream length seconds'] < 7200"}},
                    {'field': 'total messages 2h','headerTooltip': 'Total Messages First 2 Hours', 'type': 'numericColumn', 'width': 72,':cellClassRules':{'ag-cell-none2h': "data['stream length seconds'] < 7200"}},
                    {'headerName': 'Start','headerTooltip': 'Start','field': 'start datetime', 'width': 120, 'type': 'rightAligned'},
                    {'headerName': 'Status','headerTooltip': 'Status', 'field': 'end time', 'width': 120, 'type': 'rightAligned'},
                    {'headerName': 'Stream Length','headerTooltip': 'Stream Length', 'field': 'stream length','enableCellChangeFlash':False, 'width': 100, 'cellStyle': { 'text-align': 'center' }},
                    {'headerName': 'Stream Link','headerTooltip': 'Checkboxes indicate selection', 'field': 'link',':cellRenderer': 'params => `<a href="/streams/${params.data.id}/${params.data.login}">Stream-></a>`', 'resizable': True,"suppressSizeToFit": False, 'type': 'rightAligned'},
                ],
                'rowSelection': {
                    'mode': 'singleRow',
                    'checkboxes': False,
                    'enableClickSelection': True,
                },
                'suppressCellFocus':True,
                ':onRowClicked':'(params)=>{console.log(params.data[\'stream length seconds\']);console.log(params);}',
                ':onRowDoubleClicked':'(params)=>{window.location.href = `/streams/${params.data.id}/${params.data.login}`}',
      #          'suppressCellFocus ':True,
     #           'autoSizeStrategy': {'type': 'fitCellContents',},
                'rowData': [],
     #           'loading': True,
        #        'enableRowPinning' : True,
                ':getRowId': '(params) => params.data.id',
         #       ':isRowPinned': '(rowNode) => {return rowNode.data["total messages"] > 10000 ? "top" : null;}',
         #       'theme': 'quartz.withParts({pinnedRowBorder: { width: 2 },})',
                'theme': "balham",
         #       'domLayout':'autoHeight',
                ':onGridReady': '(params) => {window.ggrid =params.api;}',
                'rowClassRules': {"is_starting": "data[ 'stream status' ] == 1",},
                'cellFlashDuration':100,
               
            }).on('onStateUpdated', lambda event: stateUpdated(event)).classes('w-full h-full')#.style(add='spacing: 2; foregroundColor: "rgb(14, 68, 145)"; backgroundColor: "rgb(241, 247, 255)"; headerBackgroundColor: "rgb(228, 237, 250)"; rowHoverColor: "rgb(216, 226, 255)"') 

          with splitter.after:       
                grid2 = ui.aggrid({
                'defaultColDef': {
                    'width': 64,
                    'suppressMovable': True,
                    #'wrapHeaderText': True, 
                    #'autoHeaderHeight': True,
                    'resizable': False,
                    'suppressSizeToFit': True,
                    #'enableCellChangeFlash': True,
                },
                'columnDefs': [
                    {'headerName': "History",'headerStyle': { 'color': "white", 'backgroundColor': "cadetblue" },'pinned':'left','wrapHeaderText': True,'resizable': True,'suppressSizeToFit': False, 'autoHeaderHeight': True,'children':grid.options['columnDefs']}
                ],
                'pagination':True,
                'paginationAutoPageSize':True,
                'rowSelection': {
                    'mode': 'singleRow',
                    'checkboxes': False,
                    'enableClickSelection': True,
                },
                'headerHeight':0,
                ':alignedGrids' : '()=>[window.ggrid]',
                'rowData': [],
                'loading': True,
                'suppressCellFocus':True,
                ':onRowDoubleClicked':'(params)=>{ window.location.href = `/streams/${params.data.id}/${params.data.login}`}',
  #              'enableRowPinning' : True,
  #              ':getRowId': '(params) => params.data.id',
  #              ':isRowPinned': '(rowNode) => {return rowNode.data["total messages"] > 10000 ? "top" : null;}',
   #             'defaultColDef': {'enableCellChangeFlash': 'true'},
         #       'theme': 'quartz.withParts({pinnedRowBorder: { width: 2 },})',
                'theme': "quartz",
   #          'getRowClass': "params => { if (params.node.rowIndex % 2 === 0) { return 'my-shaded-effect'; } }"
   #             'domLayout':'autoHeight'
            }).classes('w-full h-full myshit')
        with ui.row().classes('w-full'):
            ui.label('state area')    
        
    await ui.context.client.connected()
    tt = perf_counter() 
    t1 = tt
    data_dict = dict()
    async with RedisCM(connection_pool=pool) as r:
      history_data = []
      try:         
        #ws_task = asyncio.create_task(reader(pubsub,r,grid,data_dict,grid2))      
        ws_task = asyncio.create_task(message_reader(l,r,grid,grid2,data_dict,tt))      
        
        #async for s,key in r.getKeysBySet2(l):#'stream:*:{}:data',l if l else ['*']):
        async for s,key in r.getLive(l):#'stream:*:{}:data',l if l else ['*']):
            s['start datetime']=get_date_str(s['start time'])
            
            #print(key)
            if not 'end time' in s:
                if not s['id'] in data_dict:
                    
                    s['stream length seconds']=int(datetime.now(timezone.utc).timestamp()-(s['start time']/1000)) 
                    data_dict[s['id']]={'name':s['name'],'start_str':s['start datetime'],'start':s['start time'],'secs':s['stream length seconds']}        
                    #print('one',s['name'])    
                    grid.run_grid_method("applyTransactionAsync",{ 'add': [s] })   
                else:
                    print('is in',data_dict[s['id']])    
                    
            else:        
                print("wtff!!!!!!!!!!!!!!!!",f"no {s['id']}:{s['login']}")
                if 'stream length seconds' in s:
                    s['stream length'] = pretty_time_delta(s['stream length seconds'])
                s['end time']=get_date_str(s['end time'])
                s['viewers'] = ''
                i = 0
                
                while len(history_data)>i and history_data[i]['end time']>s['end time']:
                    i += 1
                if len(history_data)<=i or history_data[i]['id']!=s['id']:
                    history_data.insert(i,s)    
        print("1-Elapsed time before history:", perf_counter() - tt)     
        tt = perf_counter()  
        async for k in r.getHistory(l,50):
            for s in k:
                if 'stream length seconds' in s:
                    s['stream length'] = pretty_time_delta(s['stream length seconds'])
                if not 'end time' in s:
                    print(f"no {s['id']}:{s['login']}")    
                else:
                    s['end time']=get_date_str(s['end time'])
                s['start datetime']=get_date_str(s['start time'])    
                s['viewers'] = ''    
            if k:
                grid2.options['rowData'].extend(k)    
               
        print("1-Elapsed time before refresh:", perf_counter() - tt)
        if 'state' in app.storage.user:
            print(app.storage.user['state'])
            grid.run_grid_method('applyState',app.storage.user['state'])
        tt = perf_counter()       
        #grid.run_grid_method("setGridOption","loading",False)
  #      print(splitter)
  #      print(grid)
        grid2.options['loading']=False
      #  grid2.options['alignedGrids']=['window.ggrid']
        grid2.options['rowData'].extend(history_data)
        grid2.options['loading']=False
        timer = ui.timer(1.3, lambda: on_timer(data_dict,grid))
        print("1-Elapsed time total:", perf_counter() - t1)   
            
        await asyncio.wait([asyncio.create_task(ui.context.client.disconnected()),ws_task],return_when=asyncio.FIRST_COMPLETED)
      except asyncio.CancelledError:
        print("Cancelled") 
      
      if not ws_task.done():
        ws_task.cancel()
      await ws_task 
       
      timer.cancel()    
    #await asyncio.sleep(1)
   
    print('disconnected')
 
async def readStreams(chart,STREAMKEYS,streamid,login): 
    streams = {i:'0' for i in STREAMKEYS}
    async with RedisCM(connection_pool=pool) as r:
        #name = await r.get(f'stream:{streamid}:{login}:name')
        sd = await r.getStreamData(f'stream:{streamid}:{login}:data')
        name = sd['name']
        chart.cliq = sd['stream strted']
        if 'vod id' in sd:
            chart.vod = sd['vod id']
        if not name:        

            return True
        chart.options['title']['text']=name
        while True:
            datas = await r.getDataStream(streams)
            for streamkey,data in datas:
                for rid,values in data:
                    match STREAMKEYS.index(streamkey):
                        case 0:
                            if rid>streams[streamkey]:
                                streams[streamkey] = rid
                            if 'val-2' in values:
                                chart.options['series'][2]['data'].append([int(rid[:-2])*60*1000,int(values['val-2'])])
                            if 'val-3' in values:
                                chart.options['series'][1]['data'].append([int(rid[:-2])*60*1000,int(values['val-3'])])    
                            if 'val-1' in values and (not chart.options['series'][0]['data'] or int(values['val-1']) !=chart.options['series'][0]['data'][-1][1]):
                                chart.options['series'][0]['data'].append([int(rid[:-2])*60*1000,int(values['val-1'])])    
                        case _:
                            print("error") 
 
@ui.page('/streams/{streamid}/{login}')
async def pageStream(streamid: str,login: str):  

  #  ui.add_css('@import url("https://code.highcharts.com/dashboards/css/dashboards.css")')
  
    #ui.add_css('@import url("https://code.highcharts.com/css/highcharts.css")')
    #ui.add_css('@import url("https://code.highcharts.com/css/themes/sand-signika.css")')
    #ui.add_css('@import url("https://code.highcharts.com/css/themes/dark-unica.css")')
    #ui.add_head_html('<script src="https://code.highcharts.com/themes/adaptive.js"></script>')
    
  #  ui.add_css(
    '''
    body {
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Roboto,
        Helvetica,
        Arial,
        "Apple Color Emoji",
        "Segoe UI Emoji",
        "Segoe UI Symbol",
        sans-serif;
}

.highcharts-figure {
    max-width: 800px;
    min-width: 360px;
    margin: 0 auto;
}

.highcharts-background {
    transition: all 250ms;
}

.highcharts-description {
    margin: 10px;
}

.controls {
    margin: 10px;
}

/* Extend the Adaptive theme */
:root,
.highcharts-light {
    --highcharts-color-0: black;
}

@media (prefers-color-scheme: dark) {
    :root {
        --highcharts-color-0: white;
    }
}

.highcharts-dark {
    --highcharts-color-0: white;
}
 
'''
    ui.add_body_html('''
        <script>
        function loadTheme() {
            const darkModeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
            const linkElement = document.createElement('link');
            linkElement.rel = 'stylesheet';
            linkElement.href = darkModeMediaQuery.matches ? 'https://code.highcharts.com/css/themes/dark-unica.css' : 'https://code.highcharts.com/css/themes/sand-signika.css';
            document.head.appendChild(linkElement);
            const ee = document.getElementById("c1");
            console.log(darkModeMediaQuery.matches)
            ee.classList.toggle("highcharts-container", !darkModeMediaQuery.matches)
        }

        document.addEventListener("DOMContentLoaded", function() {
            loadTheme(); // Load theme based on initial preference
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', loadTheme); // Listen for changes
        });
    </script>'''
        )
   
#)
    chart = ui.highchart({
    'chart': {
        'zooming': {
            'type': 'xy'
        },
        'styledMode': True

    },
    'title': {'text':''},
    'xAxis': [{
        'type': 'datetime',
        'dateTimeLabelFormats': {
            'month': '%e. %b',
            'year': '%b'
        },
        'title': {
            'text': 'Date'
        }
    }],
    'yAxis': [{ 
        'labels': {
            'format': '{value}'
        },
        'title': {
            'text': 'Viewers'
        },

        'lineColor': 'Highcharts.getOptions().colors[0]',
        'lineWidth': 2
    }, {
        'title': {
            'text': 'Messages, Chatters'
        },
        'labels': {
            'format': '{value}'
        },
        'maxPadding': 0.9,
        'lineColor': 'Highcharts.getOptions().colors[2]',
        'lineWidth': 2,
        'opposite': True,
        
    }],
    'tooltip': {
        'shared': True
    },
 #   'colors': ['#FF5733', '#33FF57', '#3357FF', '#F1C40F', '#8E44AD'],
    'legend': {
        'align': 'left',
        'verticalAlign': 'bottom'
        
    },
    'time': {
        'timezone': 'America/Chicago'
    },
    'plotOptions': {
        'showCheckbox':True,
        'column': {
            'pointPadding': 0.08,
            'groupPadding': 0,
        },

        'spline': {
            'dataLabels': {
                'enabled': True
            },
#            'enableMouseTracking': false
        }
    },
    'series': [{
        'id': 'main-series',
        'name': 'Viewers',
        'type': 'spline',
        'data':[],
        'tooltip': {
       #     'valueSuffix': '°C'
        }
    }, {
        'name': 'Unique Chatters Last 10 Min',
        'type': 'areaspline',
        'yAxis': 1,
        'data':[],
        'tooltip': {
       #     'valueSuffix': '°C'
        }
    },{
        'name': 'Messages Last Minute',
        'type': 'column',
        'yAxis': 1,
        'data': [],
        'tooltip': {
  #          'valueSuffix': ' mm'
        }

    } ]
    },extras=['exporting'],on_point_click=lambda e: ui.navigate.to(f'https://www.twitch.tv/videos/{e.sender.vod}?t={int((e.point_x-e.sender.cliq)/1000)}s',True) if hasattr(e.sender,'vod') else False).classes('w-full h-full')

    #ui.button('Update', on_click=update)
    STREAMKEYS = [f'stream:{streamid}:{login}:logs']
    streams = {i:'0' for i in STREAMKEYS}
    await ui.context.client.connected()
    
    
    read_task = asyncio.create_task(readStreams(chart,STREAMKEYS,streamid,login))
    ui.button('Cacncel', on_click=lambda : read_task.cancel())
         
    
                        
    #print('belllllllllllllllllllllllllllllllllllllll')
    #print(await anext(r.scan_iter(f'stream:{streamid}:*:data')))
    await asyncio.wait([asyncio.create_task(ui.context.client.disconnected()),read_task],return_when=asyncio.FIRST_COMPLETED)
    print('closing...........')
    if not read_task.done():
        read_task.cancel()
        print('cancelling')
    res = await read_task
    if res:
        ui.navigate.history.replace('/streams/{}'.format(streamid))
        ui.navigate.to('/streams/{}'.format(streamid))
    print('shiii')
    #await r.aclose()
    
@ui.page('/streams/{streamid}')
async def pageStream(streamid: str):    
    await ui.context.client.connected()     
    r = redis.Redis(connection_pool=pool,decode_responses=True)
    new_url = '/streams/{}/{}'.format(streamid,(await anext(r.scan_iter(f'stream:{streamid}:*:data'))).split(':')[2])
    ui.navigate.history.replace(new_url)
    ui.navigate.to(new_url)
    #ui.navigate.history.replace(new_url)
    #print(await anext(r.scan_iter(f'stream:{streamid}:*:data')))
    await ui.context.client.disconnected()
    await r.aclose()
    
@app.on_page_exception
def timeout_error_page(exception: Exception) -> None:
    if not isinstance(exception, StopAsyncIteration):
        raise exception
    with ui.column().classes('absolute-center items-center gap-8'):
        ui.icon('sym_o_timer', size='xl')
        ui.label(f'{exception}').classes('text-2xl')
        ui.code(traceback.format_exc(chain=False))    


pool = None

async def createPool():
    #loop = asyncio.get_running_loop()
    #loop.set_debug(True)
    #loop.slow_callback_duration = 0.05

    global pool 
    if os.name == 'posix':
        pool = redis.ConnectionPool(connection_class=redis.UnixDomainSocketConnection, path='/run/valkey/valkey.sock',decode_responses=True,health_check_interval = 5,max_connections=55)
    else:    
        pool = redis.ConnectionPool(decode_responses=True,health_check_interval = 5,max_connections=55)
#    r = redis.Redis(connection_pool=pool)
#    print(f"Ping successful: {await r.ping()}")
#    print(await r.hgetall("commands:shutdown"))
'''
    async with r.pubsub() as pubsub:
        await pubsub.subscribe("__keyspace@0__:commands:shutdown")
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
        if message is None:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
        if message is not None:
            
                if message['data']=='hset':
                
                    app.shutdown()
    await r.aclose()
    print('closing')
    #print(pool)
'''
async def closePool():
    #print(pool)
    await pool.aclose()       
def main():    
#logging.basicConfig(level=logging.DEBUG)    
    app.on_startup(createPool)
    app.on_shutdown(closePool)
#ui.run(show=False,uvicorn_logging_level='debug')
    ui.run(dark=None,show=False,storage_secret='delete this later',reload=False)

if __name__ == "__main__":
    main()
