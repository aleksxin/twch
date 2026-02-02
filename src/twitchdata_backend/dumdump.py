import aiohttp
import asyncio
import websockets
import sys
import json
import logging
#import hashlib
import functools
import distinctipy
from playsound import playsound
import statistics
#import csv
#import aiofiles
#from aiocsv import AsyncWriter

from rich import print
from rich.markup import escape
#from rich.live import Live
#from rich.table import Table
from rich.logging import RichHandler
from rich.console import Console

from collections import defaultdict
from collections import Counter
from collections import deque

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os

import time
#import keyboard 
from enum import Enum
import numpy as np
from twitchdata_redis import RedisCM

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
    
mcolors = distinctipy.get_colors(52)    
@functools.cache   
def get_color(st):
    #print(st)
    return '#%02x%02x%02x' % tuple([int(255*x) for x in mcolors.pop(0)])
@functools.cache
def complement(h):
    #print(h)
    return '#%02x%02x%02x' % tuple([int(255*x) for x in distinctipy.get_text_color([1.0*x/255 for x in tuple(int(h.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))])])
    
CLIENTID = 'hain44pio7jpdyo2yab09ols71hmkq'
TOKENID = '8z8rdb7pm6b8c9ocv9omhivr1p3i1d'
URL = 'https://api.twitch.tv/helix/'
WS_TIMEOUT = 30
WS_URL = 'wss://eventsub.wss.twitch.tv/ws?keepalive_timeout_seconds='+str(WS_TIMEOUT)
USER_ID = '1214278273'
SUB_LIST = ["channel.chat.message","channel.chat.notification"]
DT_TIMEZONE = 'America/Chicago'
BOT_NAMES = ['HAchuBOT','BeptoBot']
TIME_CONST_MIN = 120
MIN_AVG = 20
N_MINUTES = 5
N_MULTI = 2
EVENT_LIMIT = 4
   
class StreamerData:
    def __init__(self, stream_id, name, login, viewers = 0, streamstarted = None, title =''):
        if os.path.exists("c:\\data\\chatty_logs\\clien\\419023__jacco18__acess-denied-buzz.mp3"):
            playsound('c:\\data\\chatty_logs\\clien\\419023__jacco18__acess-denied-buzz.mp3',False)
        self._id = stream_id
        self._name = name
        self._login = login
        self._viewers = viewers    
        self._uqc = Counter()      
        self._uqs = Counter()
        self._uqbb = Counter()
        self._type1 = Counter()
        self._uqc_t = Counter()
        self._uq_lastn=None #deque(maxlen = N_MINUTES+1)
        self._minavg = None
        self._messages_t = 0
        self._lastmin = -1
        self._messages = 0
        self._bits = 0
        self._shared = 0
        self._gifted = 0
        self._subs = 0          
        self._subbed = 0
        self._started = None
        self._ended = None   
        self._prevmin = None     
        self._streamstarted = streamstarted
        self._viewersy = np.array([])
        self._viewersx = np.array([])
        self._avgviewers = None
        
        self._savechat = None
        self._chatlog = None
        self._debug = None
        
        self._title = title
        self._vod = None
      
    async def update(self, name, viewers = 0, streamstarted = None, title = ''):
        self._name = name
        #if self._started:
        #    xtime = (round((datetime.now(timezone.utc)-self._started).total_seconds()))
        if viewers and (viewers != self._viewers or len(self._viewersx) == 0 or round((datetime.now(timezone.utc)-self._started).total_seconds()) - self._viewersx[len(self._viewersx)-1] > 300):
            self._viewersy = np.append(self._viewersy,viewers)
            self._viewersx = np.append(self._viewersx, round((datetime.now(timezone.utc)-self._started).total_seconds()))
            self._avgviewers = round(np.trapezoid(self._viewersy, self._viewersx, axis=0)/(self._viewersx[len(self._viewersx)-1]-self._viewersx[0])) if len(self._viewersx)>1 else 0
            #print(self._viewersx,self._viewersy,(self._viewersx[len(self._viewersx)-1]-self._viewersx[0]))
        if viewers:
            self._viewers = viewers
            
        if streamstarted:
            self._streamstarted = streamstarted
            
        if title:
            self._title = title    
                
         
    def end(self,endts): 
        if os.path.exists("c:\\data\\chatty_logs\\clien\\159399__noirenex__power-down.wav"):
            playsound('c:\\data\\chatty_logs\\clien\\159399__noirenex__power-down.wav',False)
        self._ended = endts

        return self.get_row_final()
    
    @property
    def id(self):
        return self._id
        
    @property
    def name(self):
        return self._name[:12]
        
    @property
    def login(self):
        return self._login    
        
    @property
    def vod(self):
        return self._vod
        
    @vod.setter
    def vod(self, value):
        self._vod = int(value)

    @property
    def title(self):
        return self._title    
        
    @property
    def color(self):
        return get_color(self._name)
        
    @property
    def streamstarted(self):     
        return self._streamstarted
        
    @property
    def uniquechatters(self):
        return len(self._uqc) if self._uqc else ''
    
    @property
    def uniquechatters_nonshared(self):
        return len(self._uqs) if self._uqs and self._shared else ''    
    
    @property
    def uniquechatters_subbed(self):
        return len(self._uqbb) if self._uqbb else ''
             
    @property
    def uniquechatters_subbed_aspct(self):
        return (len(self._uqbb)/len(self._uqc))*100 if self._uqc else ''
    
    @property
    def type1(self):
        return len(self._type1) if self._type1 else ''
        
    @property
    def totalmessages(self):
        return self._messages     
    
    @property
    def messages_fromshared(self):
        return self._shared if self._shared>0 else ''
    
    @property
    def ownmessages_fromsharedaspct(self):
        return ((self._messages-self._shared)/self._messages)*100 if self._messages and self._shared else ''
    
    @property
    def subs(self):
        return self._subs
    
    @property
    def subs_nongifted(self):
        return self._subs-self._gifted
    
    @property
    def subs_gifted(self):
        return self._gifted
    
    @property
    def bits(self):
        return self._bits
    
    @property
    def viewers(self):
        return self._viewers
    
    @property
    def messages_lastmin(self):
        return self._minavg[-1] if self._minavg else ""
    
    @property
    def messages_permin_avg(self):
        return round(statistics.mean(self._minavg)) if self._minavg else ''
    
    @property
    def uniquechatters_lastnmin(self):
        return len(self._uq_lastn[0]) if self._uq_lastn else ''
    
    @property
    def uniquechatters_forperiod(self):
        return len(self._uqc_t) if self._uqc_t else ''
    
    @property
    def totalmessages_forperiod(self):
        return self._messages_t
    
    @property
    def stream_starttime(self):
        return self._started.astimezone(ZoneInfo(DT_TIMEZONE)).strftime("%m-%d %H:%M:%S") if self._started else ''
    
    @property
    def stream_length_sec(self):
        #if self.is_live():
            return (datetime.now(timezone.utc)-self._started).total_seconds() if self._started else ''
        #else:
        #    return (self._ended-self._started).total_seconds()
    
    @property
    def stream_length_sec_final(self):
            return (self._ended-self._started).total_seconds() if self._ended and self._started else ''
    
    @property    
    def stream_endtime(self):
        return self._ended.astimezone(ZoneInfo(DT_TIMEZONE)).strftime("%m-%d %H:%M:%S") if self._ended else '' 

    @property    
    def average_viewers(self):
        return self._avgviewers if self._avgviewers else ''    
        
    def __getitem__(self, key):
        match key:
            case 1 : return self.name.upper()
            case 2 : return self.uniquechatters if self.uniquechatters else 0
            case 3 : return self.uniquechatters_nonshared if self.uniquechatters_nonshared else self[2]
            case 4 : return self.uniquechatters_subbed_aspct if self.uniquechatters_subbed_aspct else 0
            case 5 : return self.totalmessages if self.totalmessages else 0
            case 6 : return self.subs if self.subs else 0
            case 7 : return self.viewers if self.viewers else 0
            case 8 : return self.messages_permin_avg if self.messages_permin_avg else 0
            case 9 : return self.uniquechatters_lastnmin if self.uniquechatters_lastnmin else 0
            case _ : return print('key')
    
    @functools.cache    
    def col_names(sort,reverse):
        if reverse:
            c = '\u2193'
        else:
            c = '\u2191'
        r = ["name","uq ch.","uq s","subbd","sub.%","type 1","total","shrd","n-sh%","subs","non-g","giftd","bits","vwrs.","last m","avg"+pretty_time_delta(timedelta(minutes = MIN_AVG).total_seconds()),"uq "+pretty_time_delta(timedelta(minutes = N_MINUTES*N_MULTI).total_seconds()),"uq "+pretty_time_delta(timedelta(minutes = TIME_CONST_MIN).total_seconds()),"t. "+pretty_time_delta(timedelta(minutes = TIME_CONST_MIN).total_seconds()),"started","status/ended","time"]
        match sort:
            case 1 : r[0] += c
            case 2 : r[1] += c
            case 3 : r[2] += c
            case 4 : r[4] += c
            case 5 : r[6] += c
            case 6 : r[9] += c
            case 7 : r[13] += c
            case 8 : r[15] += c
            case 9 : r[16] += c
            case _ : pass
        return r
    
    def get_row(self):
            return ["["+complement(self.color)+' on '+self.color+']'+self.name+"[/]",self.uniquechatters,self.uniquechatters_nonshared,self.uniquechatters_subbed,str(round(self.uniquechatters_subbed_aspct,1))+"%" if self.uniquechatters_subbed_aspct else '',self.average_viewers,self.totalmessages,self.messages_fromshared,str(round(self.ownmessages_fromsharedaspct,1))+"%" if self.ownmessages_fromsharedaspct else '',self.subs,self.subs_nongifted,self.subs_gifted,self.bits,"[red]"+str(self.viewers)+"[/]" if self.viewers>0 else self.viewers,self.messages_lastmin,round(self.messages_permin_avg) if self.messages_permin_avg else '',self.uniquechatters_lastnmin,self.uniquechatters_forperiod,self.totalmessages_forperiod,self.stream_starttime,"[bold green]LIVE[/]" if not self.is_starting() else "[black on red]Starting[/]",pretty_time_delta(int(self.stream_length_sec))if self.stream_length_sec else '']

    def get_row_final(self):
            return [self.name,self.uniquechatters,self.uniquechatters_nonshared,self.uniquechatters_subbed,str(round(self.uniquechatters_subbed_aspct,1))+"%" if self.uniquechatters_subbed_aspct else '',self.average_viewers,self.totalmessages,self.messages_fromshared,str(round(self.ownmessages_fromsharedaspct,1))+"%" if self.ownmessages_fromsharedaspct else '',self.subs,self.subs_nongifted,self.subs_gifted,self.bits,'',self.messages_lastmin,round(self.messages_permin_avg) if self.messages_permin_avg else '',self.uniquechatters_lastnmin,self.uniquechatters_forperiod,self.totalmessages_forperiod,self.stream_starttime,self.stream_endtime,pretty_time_delta(int(self.stream_length_sec_final))if self.stream_length_sec_final else '']

    
    def parse(self,message_data):     
        raise NotImplementedError("Subclasses must implement this method")
    
    def is_new_min(self,minute):
        if minute != self._lastmin:
            if self._prevmin and minute == self._prevmin:
                print(minute,'->','Yes',self._name)
                return False
            if self._lastmin != -1:
                if self._minavg is None:
                    self._minavg = deque(maxlen = MIN_AVG)
                else:
                    self._minavg.append(self._messages_1m)       
            if self._uq_lastn is None:
                self._uq_lastn = deque(maxlen = N_MINUTES+1)
                self._uq_lastn.append(Counter())
            else:
                if minute % N_MULTI == 0:
                    self._uq_lastn.append(Counter())

            self._prevmin = self._lastmin    
            self._lastmin = minute    
            self._messages_1m = 0
            return True
        else:
            return False
        
    def new_message(self,timest,chatter_name,shared,bits,subbed = False,type1 = False):
        if self._uqc == None:
            print(self)
            
        if not self._started:
            print("new started ("+self._name+") "+timest.astimezone(ZoneInfo(DT_TIMEZONE)).strftime("%H:%M:%S"))
            self._started = timest
            
        #time = datetime.fromisoformat(timest)
        
        #self._last = datetime.fromisoformat(timest)
        #print(chatter_name)
        self._uqc[chatter_name] += 1
        self._messages += 1
        self._bits += bits
        if shared:
            self._shared += 1
        else:
            self._uqs[chatter_name] += 1
             
        if subbed:
            self._uqbb[chatter_name] += 1
               
        if type1:
            self._type1[chatter_name] += 1
        
        if not timest:
            print("Timest is None !!!!!!")
        
        if int((timest - self._started).total_seconds() // 60) < TIME_CONST_MIN:
            self._uqc_t[chatter_name] += 1
            self._messages_t += 1    
        
        r = None

        if self.is_new_min(timest.minute):  
            r = timest
        
        if not shared:   
            self._messages_1m += 1
            for i in range(max(0,len(self._uq_lastn)-N_MINUTES),len(self._uq_lastn)):
                self._uq_lastn[i][chatter_name] += 1
            
        #print(self._messages_1m,escape(str(self._minavg)),self._messages,len(self._uqc))
        if r:
            return r
            
    def new_sub(self,timest,chatter_name,gifted):
        #self._last = datetime.fromisoformat(timest)  
        self._subs += 1
        if gifted:
            self._gifted += 1
        
    
    def is_starting(self):
        return self._viewers == 0
        
    def get_csv_data(self,minute):           
            self.is_new_min(minute)
            return self._viewers,self._minavg[-1] if self._minavg else "",len(self._uq_lastn[0]) if self._uq_lastn else ''
            
    def __repr__(self):
        return f"{self._name} - {self._viewers},{self.uniquechatters},{self.totalmessages}, stream running time {pretty_time_delta((datetime.now(timezone.utc)-self._streamstarted).total_seconds())}"             
            
class StreamerDataWS(StreamerData):
    def __init__(self, stream_id, name, login, ws_session,viewers = 0, streamstarted = None, title = ''):    
        super().__init__(stream_id,name,login,viewers,streamstarted,title)
        self._ws_session=ws_session
        self._sessions = None
        
        
    async def end(self,httpsession,endts,r):
        results = "ended"
        if self._sessions:         
            coros = [stop_subscription(httpsession,self._sessions[i]) for i in self._sessions]+[r.setEndHashData(self._ended,self._started,self)]
            results = await asyncio.gather(*coros)
            self._ended = datetime.fromtimestamp(results[2]/1000,timezone.utc)
        else:
            results=await r.setEndHashData(self._ended,self._started,self)        
            if not self._ended:
                self._ended = datetime.fromtimestamp(results/1000,timezone.utc)
            
        #await r.hset('stream:'+self.id+':'+self.login+':data',items=['end time',int(self._ended.timestamp()*1000),'stream length seconds',int(self.stream_length_sec_final)])  
        return results,super().end(self._ended)        
        
    async def connect_todata(self,httpsession,my_id,session_id,r):             
        if self.is_connected():
            print("There are sessions!!!",self._name)
            return
            
        if session_id:
            #if self._ws_session==session_id:
            #    print('same sessoin')
            self._ws_session=session_id   
            
        ret = []    
        if not self._sessions:
            self._sessions = dict()  
            self.frick = False            
        for i in SUB_LIST:
            if i in self._sessions and self._sessions[i]:
                print(self._name,'has',i,'!')
            else:
                status,sub_id,createdat = await start_subscription(httpsession,i,my_id,session_id,USER_ID)
                if status == 202:
                    self._sessions[i] = sub_id
                    if i == "channel.chat.message":
                        if not self._started:                           
                            self._started = datetime.fromisoformat(createdat)
                            #await r.set('stream:'+self.id+':'+self.login+':name',self.name) 
                            #await r.hset('stream:'+self.id+':'+self.login+':data',items=['start time',int(self._started.timestamp()*1000)])  
                            await r.setStartHashData(self._started,self)
                                                           
                        if self._ended:
                            ret.append('Disconnected for '+str(round((datetime.fromisoformat(createdat)-self._ended).total_seconds(),1))+' seconds')
                            self._ended = None
                        
                    ret.append(self._sessions[i])
                else:
                    ret.append([status,sub_id])

        return ret
           
    def disconnect_data(self,end_time):
        if os.path.exists("c:\\data\\chatty_logs\\clien\\51702__bristolstories__ping.mp3"):
            playsound('c:\\data\\chatty_logs\\clien\\51702__bristolstories__ping.mp3',False)
        self._ws_session = None
        self._sessions = None
        self._ended = end_time
        
        return str(end_time)
        
    async def parse(self,data_type,data,dt,r = None):
        CHAT = 4
        CHAT_LOG = 2
        DEBUG_DATA = 1
        result = None
        params = 0
        match data_type:
            case 'channel.chat.message':
                #print(data)
                if self._savechat and r:
                    await r.saveChat(dt,data,self)
                if self._chatlog and r:
                    await r.saveChatLog(dt,data,self)    
                if not data['chatter_user_name'] in BOT_NAMES and not any(x['set_id'] == 'bot-badge' for x in data['badges']):
                    result = self.new_message(dt,data['chatter_user_name'],data['source_broadcaster_user_name'] is not None, data['cheer']['bits'] if data['cheer'] and not data['source_broadcaster_user_name'] else 0, any(x['set_id'] == 'subscriber' for x in data['badges']))
                    if r:
                        params = await r.setHashData(self._started,self)
                if not self.frick or False or data['message_type']!='text' and data['message_type']!='channel_points_highlighted':
                    await asyncio.create_task(print_chat(dt,data,self.is_starting()), name = "Print chat")
                    self.frick = True
                       
            case 'channel.chat.notification':
                #print('[black on white]'+data+'[/]')
                if self._savechat and r:
                    await r.saveChatNoti(dt,data,self)
                if self._chatlog and r:
                    await r.saveChatLogNoti(dt,data,self) 
                if data['notice_type'] in ('sub','resub','sub_gift'):
                    self.new_sub(dt,data['chatter_user_name'],data['notice_type'] == 'sub_gift')
                    if r:
                        params = await r.setHashData(self._started,self)
                if False:    
                    await asyncio.create_task(print_noti(dt,data), name = "Print noti")    
       
        self._savechat = params & CHAT
        self._chatlog = params & CHAT_LOG
        self._debug = params & DEBUG_DATA        
        return result 
        
    def is_connected(self):
        return bool(self._sessions)and len(self._sessions) == len(SUB_LIST)
                
    def get_row(self):
 
            return ["["+complement(self.color)+' on '+self.color+']'+self.name+"[/]",self.uniquechatters,self.uniquechatters_nonshared,self.uniquechatters_subbed,str(round(self.uniquechatters_subbed_aspct,1))+"%" if self.uniquechatters_subbed_aspct else '',self.average_viewers,self.totalmessages,self.messages_fromshared,str(round(self.ownmessages_fromsharedaspct,1))+"%" if self.ownmessages_fromsharedaspct else '',self.subs,self.subs_nongifted,self.subs_gifted,self.bits,"[red]"+str(self.viewers)+"[/]" if self.viewers>0 else self.viewers,self.messages_lastmin,round(self.messages_permin_avg) if self.messages_permin_avg else '',self.uniquechatters_lastnmin,self.uniquechatters_forperiod,self.totalmessages_forperiod,self.stream_starttime,"[red]NO DATA[/]" if not self.is_connected() else "[bold green]LIVE[/]" if not self.is_starting() else "[black on red]Starting[/]",pretty_time_delta(int(self.stream_length_sec))if self.stream_length_sec else '']    
    
    def __repr__(self):
        return super().__repr__()
        
  
async def sort_dict(d,sortparam,logindict):
    if sortparam['value'] == 0:
        return  sorted(d.items(), key=lambda item: list(logindict.values()).index(item[0]), reverse=sortparam['reverse'])
    else:    
        return  sorted(d.items(), key=lambda item: item[1][sortparam['value']], reverse=sortparam['reverse'])

    '''
def generate_table(sortedd,history_sesh,labal_row) -> Table:
    """Make a new table."""
    table = Table()
    for i in labal_row:
        table.add_column(i)
    for x in sortedd:
        table.add_row(*(str(i) for i in x[1].get_row()))
    if history_sesh:
        for x in reversed(history_sesh):
            table.add_row(*["[bright_black]"+str(i)+"[/]" for i in x])

    return table
'''
async def get_api(httpsession, endpoint, paramskey, queryparams):
    #print(f"Starting task: {url}")
    if queryparams != '':
        params = {paramskey:queryparams}
    #print(params)
    #headers = {"Authorization": "Bearer "+TOKENID,"Client-Id":CLIENTID}    
    async with httpsession.get(URL+endpoint,params=params,raise_for_status=True) as response:
        #print(response.headers)
            return response.status, await response.json()

        
async def get_streams(d,httpsession,logins,ws_session = None,r = None):
    status,jsond = await get_api(httpsession,"streams","user_login",logins)
    temp = list(d)
    res = []
    if status == 200:
        #print(jsond)
        for i in jsond["data"]:
            #print(i['id'])
            if i['user_id'] in d:
                await asyncio.create_task(d[i['user_id']].update(i['user_name'],i['viewer_count'],datetime.fromisoformat(i["started_at"]),i['title']))
                if ws_session and not d[i['user_id']].is_connected():
                    print("rare is not supposed to happen!")            
                    print('New ConnectData for '+i['user_login']+':',escape(str(await asyncio.create_task(d[i['user_id']].connect_todata(httpsession,i['user_id'],ws_session,r), name='ConnectData for '+i['user_login']))))
                temp.remove(i['user_id'])
            else:
                d[i['user_id']] = StreamerDataWS(i['id'],i['user_name'],i['user_login'],ws_session,i['viewer_count'],datetime.fromisoformat(i["started_at"]),i['title'])
                if ws_session:
                    print('ConnectData for '+i['user_login']+':',escape(str(await asyncio.create_task(d[i['user_id']].connect_todata(httpsession,i['user_id'],ws_session,r), name='ConnectData for '+i['user_login']))),'started',pretty_time_delta((datetime.now(timezone.utc)-datetime.fromisoformat(i['started_at'])).total_seconds()),'ago')
  #          d[i["user_id"]]["name"] = i["user_name"]    
  #          d[i["user_id"]]["viewers"] = i["viewer_count"]
        #print(temp)
        if temp:
            async for x in (list((d[k].name,)+await asyncio.create_task(d.pop(k).end(httpsession,datetime.now(timezone.utc),r))) for k in temp):
                res += [x[2]]
                print("Stop Data for",x[0],":",escape(str(x[1])))

    else:
        print(status," - ",jsond)        
    #if res:
        #print(res)    
    return res

async def getThemVods(httpsession,d):
    res = []
    #print(d)
    async for response,man in ((await get_api(httpsession,"videos","user_id",i),d[i]) for i in d if not d[i].vod):
        if response[0]==200:
            #print(response)
            vid = next((data['id'] for data in response[1]['data'] if data['stream_id']==man.id),None)
            if vid:
                man.vod=vid
                res += [f'{man.id}:{vid}']
            else:
                res += [f'not found for {man}']
        else:
            res +=[f'error {man} - {status}']
                
    return res
async def start_subscription(httpsession,subtype,broadcaster_id,session_id,user_id = '',version = '1'):
    if not session_id:
        return
    data = {
        "type": subtype,
        "version": version,
        "condition": {
            "broadcaster_user_id": broadcaster_id
        },
        "transport": {
            "method": "websocket",
            "session_id": session_id
        }
    }
    if user_id != '':
        data['condition']['user_id'] = user_id
    #print(data)
    headers = httpsession.headers
    headers["Content-Type"] = "application/json" 
    d = None
    i = 0
    while True:    
        async with httpsession.post(URL+"eventsub/subscriptions",headers=headers,json=data) as response:
        #print(response)
            if response.status == 202:             
                jsn = await response.json()
                #print(jsn)
                return response.status, jsn['data'][0]['id'], jsn['data'][0]['created_at']
            elif response.status not in {429}:
                return response.status, response, None
            print(broadcaster_id,subtype,response.status,response.text if response.status != 429 else '','..........')
            mil = int(response.headers['Ratelimit-Reset']) if response.headers['Ratelimit-Reset'] else time.time()+1
            if not d:
                d = 1
            else:
                d = d * max((5 - i),1)
     
            await asyncio.sleep((datetime.fromtimestamp(mil,timezone.utc)-datetime.now(timezone.utc)).total_seconds()+d)
            i += 1
            print(broadcaster_id,'retrying.......',i)
            
 
            
async def stop_subscription(httpsession,sub_id):
    params = {"id" : sub_id}
    async with httpsession.delete(URL+"eventsub/subscriptions",params=params) as response:
        #print(response)
        return response.status
   
async def print_chat(timest,message,is_starting = False):
    s = '[bright_black]-'+timest.astimezone(ZoneInfo(DT_TIMEZONE)).strftime("%H:%M:%S")+'[/]'    
    s += message['message_type'][0]+' '
    s += '['+complement(get_color(message['broadcaster_user_name']))+' on '+get_color(message['broadcaster_user_name'])+']('+message['broadcaster_user_name']+')[/]'   
    if message['color'] and message['color']!='':
        s += '['+message['color']+']'+message['chatter_user_name']+':[/]'
    else:
        s += message['chatter_user_name']+':'
                            
    if message['source_broadcaster_user_name']:
        s += '['+complement(get_color(message['source_broadcaster_user_name']))+' on '+get_color(message['source_broadcaster_user_name'])+'](from ' + message['source_broadcaster_user_name']+')[/]'
    
    if is_starting:
        s += "[on red] "+message['message']['text']+"[/]"    
    else:
        s += ' '+message['message']['text']    
                        
    if message['cheer']:
        s += '\t\t\t\t[black on purple]'+str(message['cheer']['bits'])+'[/]'
    if message['message_type']!='text':
        s+= '\t\t\t\t\t\t[black on green]'+message['message_type']+'[/]'
                           #s += '&'.join([x['set_id'] for x in parsed['payload']['event']['badges']])
    print(s)
    
    
async def print_noti(timest,message):
    s = '[bright_black]-'+timest.astimezone(ZoneInfo(DT_TIMEZONE)).strftime("%H:%M:%S",)+'[/]'     
    s += '[black on red]'+message['notice_type']+'[/]'+'['+complement(get_color(message['broadcaster_user_name']))+' on '+get_color(message['broadcaster_user_name'])+']('+message['broadcaster_user_name']+')[/]'+'[white on bright_black]'+message['system_message']+'[/]'
    print(s)
    
async def start_streamsubs(background_tasks,d,loginsids,limit,httpsession,ws_session,wait_time =1):   
    for i in list(loginsids.keys())[:limit]:
        await asyncio.sleep(wait_time)
        if loginsids[i] in d:
            background_tasks.add(asyncio.create_task(start_subscription(httpsession,"stream.offline",loginsids[i],ws_session[0]), name='StartSubscription stream.offline for '+i))
        else:
            background_tasks.add(asyncio.create_task(start_subscription(httpsession,"stream.online",loginsids[i],ws_session[0]), name='StartSubscription stream.online for '+i))
            
async def block_checking(websocket):
    start_time = time.time()
    while True:
        print(f"elapsed time {time.time() - start_time}", flush=True)
        #if websocket.state == DISCONNECTED:
        #    break
        await asyncio.sleep(0.5)                    

async def ws(url,d,httpsession,r,logins,background_tasks,loginsids,ws_session,connectedevent,spe1,createsubs = True):
  #spe = ['['+complement('#%02x%02x%02x' % tuple([int(255*x) for x in mcolors[i]]))+' on #%02x%02x%02x]' % tuple([int(255*x) for x in mcolors[i]])+s+'[/]' for i,s in enumerate("STARTING WEBSOCKETS LOOP")]
  #spe1 = ''.join(('['+complement('#%02x%02x%02x' % tuple([int(255*x) for x in i]))+' on #%02x%02x%02x]' % tuple([int(255*x) for x in i])+'{}[/]' for i in mcolors))
  try:        
    async for websocket in websockets.connect(url):      
      try:  
       #print(websocket)
       #asyncio.create_task(block_checking(websocket))   
       #print(*spe,sep = '')

       if os.path.exists("c:\\data\\chatty_logs\\clien\\778926__looplicator__120-bpm-industrial-drum-loop-9503-wav.wav"):
            playsound('c:\\data\\chatty_logs\\clien\\778926__looplicator__120-bpm-industrial-drum-loop-9503-wav.wav',False)
       
       #ws_session[0] = '';     
       mtime = None  
       n = 0
       async with asyncio.timeout(WS_TIMEOUT+1) as cm:          
        #temp_set = set()
        async for message in websocket:
            cm.reschedule(asyncio.get_running_loop().time() + WS_TIMEOUT+1)
            parsed = json.loads(message)

            mtime = datetime.fromisoformat(parsed['metadata']['message_timestamp'])
            match parsed['metadata']['message_type']:          
                case "session_welcome":
                    ws_session[0] = parsed['payload']['session']['id']
                    #print(spe1)
                    print(spe1.format(*(s for s in (ws_session[0]+' starting').ljust(52,'.'))))#*['['+complement('#%02x%02x%02x' % tuple([int(255*x) for x in mcolors[i]]))+' on #%02x%02x%02x]' % tuple([int(255*x) for x in mcolors[i]])+s+'[/]' for i,s in enumerate(ws_session[0])],sep = '')
                    background_tasks.update((asyncio.create_task(d[loginsids[i]].connect_todata(httpsession,loginsids[i],ws_session[0],r), name='StartData for '+i) for i in loginsids if loginsids[i] in d))
                    if createsubs:
                        background_tasks.add(asyncio.create_task(start_streamsubs(background_tasks,d,loginsids,EVENT_LIMIT,httpsession,ws_session), name = 'Starting stream online/offline subscriptions'))          
                    connectedevent.set()
                case "session_keepalive":
                    #print('*',end='')    
                    pass
                    
                case "notification":
                    match parsed['payload']['subscription']['type']:           
                        
                  #  background_tasks.add(f)
                  #  f.set_result(parsed["payload"]["session"]["reconnect_url"])
                        case 'channel.chat.message'|'channel.chat.notification':
                            if parsed['payload']['event']['broadcaster_user_id'] in d:
                                background_tasks.add(asyncio.create_task(d[parsed['payload']['event']['broadcaster_user_id']].parse(parsed['payload']['subscription']['type'],parsed['payload']['event'],mtime,r), name = 'message at '+parsed['payload']['event']['broadcaster_user_name']))
                            else:
                                print("Key doesnt exist:",end=" ")
                                background_tasks.add(asyncio.create_task(print_chat(mtime,parsed['payload']['event']), name = "Print chat"))

                        case 'stream.online':
                            i = parsed['payload']['event']                               
                            if not i['broadcaster_user_id'] in d:
                                d[i['broadcaster_user_id']] = StreamerDataWS(i['id'],i['broadcaster_user_name'],i["broadcaster_user_login"],ws_session[0])
                                background_tasks.add(asyncio.create_task(d[i['broadcaster_user_id']].connect_todata(httpsession,i['broadcaster_user_id'],ws_session[0],r), name='StartData for '+i['broadcaster_user_name']))    
                            background_tasks.add(asyncio.create_task(stop_subscription(httpsession,parsed['payload']['subscription']['id']), name='Stop '+parsed['payload']['subscription']['type']+' ('+parsed['payload']['subscription']['id']+') for '+i['broadcaster_user_name']))                           
                            background_tasks.add(asyncio.create_task(start_subscription(httpsession,"stream.offline",i['broadcaster_user_id'],ws_session[0]), name='StartSubscription stream.offline for '+i['broadcaster_user_name']))          

                        case 'stream.offline':            
                            i = parsed['payload']['event']
                            #print(mtime,str(d))
                            if i['broadcaster_user_id'] in d:
                                print("offline ")
                                background_tasks.add(asyncio.create_task(d.pop(i['broadcaster_user_id']).end(httpsession,mtime,r), name='StopData for '+i['broadcaster_user_name']))   
                            else:
                                print("Cannot stop key not in dict",i['broadcaster_user_login'],i['broadcaster_user_id'])
                            background_tasks.add(asyncio.create_task(stop_subscription(httpsession,parsed['payload']['subscription']['id']), name='Stop '+parsed['payload']['subscription']['type']+' ('+parsed['payload']['subscription']['id']+') for '+i['broadcaster_user_name']))
                            background_tasks.add(asyncio.create_task(start_subscription(httpsession,"stream.online",i['broadcaster_user_id'],ws_session[0]), name='StartSubscription stream.online for '+i['broadcaster_user_name']))
                               
                case "session_reconnect":    
                    print('reconnect received')
                    f = asyncio.get_running_loop().create_future()                  
                    f.set_result(parsed["payload"]["session"]["reconnect_url"])
                    background_tasks.add(f)
                case _:
                    print("------------------ERRROR--------------")
                    #print(e)
                    print(json.dumps(parsed, indent=4))
        print("TIMEOUT!!!!")        
      except (websockets.exceptions.ConnectionClosed,asyncio.TimeoutError) as e:
        print('WebSocket Error', str(e),'reconnecnting...')
        url = WS_URL
        ws_session[0]=""
        connectedevent.clear()
        await r.setDisconnected(d)
        for x in d:
            print(d[x].name,'disconnected at',d[x].disconnect_data(mtime))
        if not createsubs:
            print('cant reconnect we break!')
            break
        else:    
            await asyncio.sleep(1)       
            continue
  except asyncio.CancelledError:

    if os.path.exists("c:\\data\\chatty_logs\\clien\\90143__pengo_au__steam_burst.wav"):
        playsound('c:\\data\\chatty_logs\\clien\\90143__pengo_au__steam_burst.wav',False)   
    await r.setDisconnected(d)
    print("ws cancelled...")    
    return 'closed'   

async def run(logins):
    spe1 = ''.join(('['+complement('#%02x%02x%02x' % tuple([int(255*x) for x in i]))+' on #%02x%02x%02x]' % tuple([int(255*x) for x in i])+'{}[/]' for i in mcolors))        
    d = dict()
    #asyncio.get_event_loop().slow_callback_duration = 0.2
    ws_session=['']
    history_row=[]
    ws_task = None
    
    sorting = {"value" : 0, "reverse" : False}
    def setSorting(mode,revers=True):
        if os.path.exists("c:\\data\\chatty_logs\\clien\\70106__justinbw__function-beep.wav"):
            playsound('70106__justinbw__function-beep.wav',False)
        sorting['value'] = mode
        sorting['reverse'] = revers
        
    loginsdict = {i:None for i in logins}
    background_tasks = set()
    t_c = None
    csv_time = None
    new_csv_time = csv_time
    headers = {"Authorization": "Bearer "+TOKENID,"Client-Id":CLIENTID}    
    #fname = "csv\\bitch_"+datetime.utcnow().astimezone(ZoneInfo(DT_TIMEZONE)).strftime("%y%m%d%H%M%S")+".csv"
    ws_connected = asyncio.Event()
    #red = redis.Redis()
    #print(f"Ping successful: {await red.ping()}")
    console = Console()
    '''   
    keyboard.on_press_key("0", lambda _: setSorting(0,revers=False))
    keyboard.on_press_key("7", lambda _: setSorting(7)) 
    keyboard.on_press_key("2", lambda _: setSorting(2)) 
    keyboard.on_press_key("1", lambda _: setSorting(1,revers=False))
    keyboard.on_press_key("3", lambda _: setSorting(3)) 
    keyboard.on_press_key("4", lambda _: setSorting(4)) 
    keyboard.on_press_key("5", lambda _: setSorting(5)) 
    keyboard.on_press_key("6", lambda _: setSorting(6))     
    keyboard.on_press_key("8", lambda _: setSorting(8)) 
    keyboard.on_press_key("9", lambda _: setSorting(9))     
    '''
    async with RedisCM() as red:
      async with aiohttp.ClientSession(headers=headers) as httpsession:
        await red.updateLogins(logins,True)  
        #mentions = await red.getMentions()
        #print('mentions=',mentions)
        await get_streams(d,httpsession,logins)     
        t = time.time()
        
        status,jsond = await get_api(httpsession,"users","login",logins)
        
        for i in jsond['data']:
            loginsdict[i['login']] = i['id']
        
        #async with aiofiles.open(fname, 'w', newline='') as csvfile:
        #    writer = AsyncWriter(csvfile)
        #    await writer.writerow([""]+[item for t in [(i+"-viewers",i+"-lastmin_chats",i+"-uq_chatters_over_5_min") for i in loginsdict] for item in t])
        try:
            while not ws_task or ws_task.get_name() == 'THEWEBSOCKET_RECONNECTED':
                ws_task = asyncio.create_task(ws(WS_URL,d,httpsession,red,logins,background_tasks,loginsdict,ws_session,ws_connected,spe1),name = "THEWEBSOCKET")    
        #await ws_task
                background_tasks.add(ws_task)
                await ws_connected.wait()
        #sort_task1 = asyncio.create_task(sort_dict(d,EventEnum.Sorting,loginsdict,EventEnum.Reverse))
        
        #with Live(generate_table(await sort_dict(d,sorting,loginsdict),[],StreamerData.col_names(sorting['value'],sorting['reverse'])), refresh_per_second=0.5) as live:
               
                while not ws_task.done():
                    backs = background_tasks.copy()
                    #print(background_tasks)
                    background_tasks.clear()         
                    
                    done,backs = await asyncio.wait(backs,timeout = 2,return_when=asyncio.FIRST_EXCEPTION) #await asyncio.sleep(2)        
                    background_tasks.update(backs)
                    if len(backs) > 1:
                        console.log(escape(str(len(backs)))," pending...",'['+', '.join('"[yellow]'+t.get_name()+'[/yellow]"' if isinstance(t,asyncio.Task) else 'fufutre' for t in backs)+']',style='red')#escape(str(backs)))         
                          
                    for task in done:
                        r = task.result()
                        if isinstance(r,datetime):                          
                            if not new_csv_time or new_csv_time.minute != r.minute and r > new_csv_time:
                                new_csv_time = r
                                lb_t = time.time() 
                                #live.console.log(new_csv_time.astimezone(tz.gettz(DT_TIMEZONE)).strftime("%m-%d %H:%M:%S"))    
                            #print(*(d[k].get_csv_data(r.minute) for k in d)
                            #csv_time = r  
                        elif r and isinstance(task,asyncio.Task):
                            if isinstance(r,asyncio.Task):
                                background_tasks.add(r)    
                            if task.get_name().startswith("StopData"):
                                #print("Yes")
                                #history_row.append(r[1])
                                r = r[0]
                                
                            console.log(escape(task.get_name()+": "+str(r)))    
                        elif r:
                            if isinstance(r,str) and r.startswith("wss"):
                                old_ws = ws_task
                                ws_connected.clear()
                                ws_task = asyncio.create_task(ws(r,d,httpsession,red,logins,background_tasks,loginsdict,ws_session,ws_connected,spe1,False),name = "THEWEBSOCKET_RECONNECTED") 
                                background_tasks.add(ws_task)
                                await ws_connected.wait()
                                old_ws.cancel()
                                
                            console.log(escape(str(r)))    
                    if t_c and time.time() - t_c > 60 and new_csv_time == csv_time:
                        new_csv_time = new_csv_time + timedelta(minutes = 1)
                        t_c = time.time()    
                    #print(str(d))                             
                    if new_csv_time != csv_time and time.time() - lb_t > 3:
                        if csv_time:
             #             async with aiofiles.open(fname, 'a', newline='') as f:
             #               writer_obj = AsyncWriter(f)        
                            await red.addTimeValues(csv_time,new_csv_time.minute,d)
                            #await writer_obj.writerow([csv_time.astimezone(tz.gettz(DT_TIMEZONE)).strftime("%m-%d %H:%M")]+[item for t in[d[loginsdict[k]].get_csv_data(new_csv_time.minute) if loginsdict[k] in d else ("","","") for k in loginsdict]for item in t])
                          #live.console.log(new_csv_time.astimezone(tz.gettz(DT_TIMEZONE)).strftime("%m-%d %H:%M:%S") ,'->',csv_time.astimezone(tz.gettz(DT_TIMEZONE)).strftime("%m-%d %H:%M:%S"))    
                            
                            
                        #print(*[item for t in[d[k].get_csv_data(new_csv_time.minute) for k in d]for item in t])
                        
                        csv_time = new_csv_time      
                        t_c = time.time()
                    #new_csv_time = csv_time
                    #console.log('tesr')      
                    #print(ws_session[0])
                    if time.time() - t >= 25:
                        try:
                            if await red.updateLogins(logins):
                                status,jsond = await get_api(httpsession,"users","login",logins)       
                                for i in jsond['data']:
                                    loginsdict[i['login']] = i['id']     
                                #console.log(loginsdict)
                            if ws_connected.is_set():
                                
                                await asyncio.create_task(get_streams(d,httpsession,logins,ws_session[0],red))
                                background_tasks.add(asyncio.create_task(getThemVods(httpsession,d),name = "creating vods"))
                                #history_row.extend((i for i in await asyncio.create_task(get_streams(d,httpsession,logins,ws_session[0],red))))
                                
                        except aiohttp.ClientConnectorError as e:
                            if os.path.exists("c:\\data\\chatty_logs\\clien\\70106__justinbw__function-beep.wav"):
                                playsound('c:\\data\\chatty_logs\\clien\\70106__justinbw__function-beep.wav',False)
                            console.log('Connection Error', str(e))        
                        t = time.time()

                    #live.update(generate_table(await sort_dict(d,sorting,loginsdict), history_row,StreamerData.col_names(sorting['value'],sorting['reverse'])))

                   
                await ws_task    
        except asyncio.CancelledError:
            #    print("run cancelled...")
                if not ws_task.done():
                    ws_task.cancel()  
                await ws_task
              #  await red.aclose()
                await asyncio.sleep(1)
            #    if background_tasks:        
            #        await asyncio.wait(background_tasks, timeout=1.0)
            #    del background_tasks    
    #ws_task.cancel()   
    #atexit.register(terminate_processes)    
    print("Good bye!")  
    
    
    
def main():
    #fh = logging.FileHandler('spam.log')
    #fh.setLevel(logging.DEBUG)
    #logging.getLogger("websockets").setLevel(logging.DEBUG)
    #logging.getLogger("websockets").addHandler(fh)
    #logging.getLogger("websockets").propagate = False
    #print(logging.getLogger("websockets").handlers)
    FORMAT = "%(message)s"
    logging.basicConfig(level=logging.WARNING, format=FORMAT, datefmt="[%X]", handlers=[RichHandler()])
    asyncio.run(run(sys.argv[1:]),debug = True)
    
if __name__ == '__main__':
    main()    
