import redis.asyncio as redis
from itertools import pairwise
import os

class RedisCM:
    def __init__(self, connection_pool = None):
        self._cm = connection_pool
        self._pipe = None
        
    @property
    def pipe(self):
        if not self._pipe:
            self._pipe = self._redis.pipeline()
        return self._pipe    
    
    async def __aenter__(self):
        if os.name == 'posix' and self._cm is None:
            self._redis = redis.from_url("unix://localhost/run/valkey/valkey.sock?decode_responses=True")
        else: 
            self._redis= redis.Redis(connection_pool=self._cm,decode_responses=True)
        return self 

    async def __aexit__(self, exc_type, exc, tb):
        #print('removing redis')
        await self._redis.aclose()
     
    async def pubsub(self):
        return self._redis.pubsub()       
   
    async def getResult():
       if self._pipe:
            r =  await self._pipe.execute()
            self._pipe=None
            return r
        
    async def setHashData(self,started,obb,pipe = None):
        if not pipe:
            pipe = self.pipe;
        key = 'stream:'+obb.id+':'+obb.login
        rs = ['start time',int(started.timestamp()*1000),'unique chatters',obb.uniquechatters,'total messages',obb.totalmessages,'unique subs',obb.subs,'subs non gifted',obb.subs_nongifted,'subs gifted',obb.subs_gifted,'bits',obb.bits,'uninque chatters average over 10 min',obb.uniquechatters_lastnmin,'unique chatters first 2h',obb.uniquechatters_forperiod,'total messages 2h',obb.totalmessages_forperiod]
        if obb.uniquechatters_nonshared:
            rs += ['unique chatters non shared',obb.uniquechatters_nonshared]
        if obb.uniquechatters_subbed:
            rs += ['unique chatters subbed',obb.uniquechatters_subbed]
        if obb.uniquechatters_subbed_aspct:
            rs += ['unique chatters subbed pct',round(obb.uniquechatters_subbed_aspct*10)]
        if obb.ownmessages_fromsharedaspct:
            rs += ['own messaged percent from all',round(obb.ownmessages_fromsharedaspct*10)]    
        if obb.average_viewers:
            rs += ['avg viewers',obb.average_viewers]
        if obb.messages_fromshared:
            rs += ['messages from shared chat',obb.messages_fromshared]
        if obb.viewers:
            rs += ['viewers',obb.viewers]
            rs += ['stream status',2]
        else:
            rs += ['stream status',1]
        if obb.messages_lastmin:
            rs += ['messages lastmin',obb.messages_lastmin]  
        if obb.messages_permin_avg:
            rs += ['average messages per min last 20 min',obb.messages_permin_avg] 
        if obb.streamstarted:
            rs += ['stream strted',int(obb.streamstarted.timestamp()*1000)]      
            
        if obb.vod:
            rs += ['vod id',int(obb.vod)]         
            
 #       if obb.uniquechatters_subbed:
 #            rs += ['unique chatters subbed',obb.uniquechatters_subbed] 
 #       print(rs)
        async with self._redis.pipeline(transaction=False) as pipe:
            a,b,c,d,_ = await pipe.exists(f'{key}:chat').exists(f'{key}:chat-log').exists(f'{key}:debug-data').exists(f'{key}:title').hset(f'{key}:data',items=rs).execute()#['start time',int(self._started.timestamp()*1000),'unique chatters',self.uniquechatters,'unique chatters non shared',self.uniquechatters_nonshared,'unique chatters subbed',self.uniquechatters_subbed,'avg viewers',self.average_viewers,'total messages',self.totalmessages,'messages from shared chat',self.messages_fromshared,'unique subs',self.subs,'subs non gifted',self.subs_nongifted,'subs gifted',self.subs_gifted,'bits',        self.bits,'viewers',self.viewers,'messages lastmin',self.messages_lastmin,'average messages per min last 20 min',self.messages_permin_avg,'uninque chatters average over 10 min',self.uniquechatters_lastnmin,'unique chatters first 2h',self.uniquechatters_forperiod,'total messages 2h',self.totalmessages_forperiod] if self.uniquechatters_nonshared else ['start time',int(self._started.timestamp()*1000),'unique chatters',self.uniquechatters,'unique chatters subbed',self.uniquechatters_subbed,'avg viewers',self.average_viewers,'total messages',self.totalmessages,'unique subs',self.subs,'subs non gifted',self.subs_nongifted,'subs gifted',self.subs_gifted,'bits',self.bits,'viewers',self.viewers,'messages lastmin',self.messages_lastmin,'average messages per min last 20 min',self.messages_permin_avg,'uninque chatters average over 10 min',self.uniquechatters_lastnmin,'unique chatters first 2h',self.uniquechatters_forperiod,'total messages 2h',self.totalmessages_forperiod])
            if not d and obb.title:
                print('settting title to',obb.title,await self._redis.set(f'{key}:title',obb.title))
        return a*4+b*2+c    
        
    async def setEndHashData(self,ended,started,obb):
        
        async with self._redis.pipeline(transaction=True) as pipe:
        #await self._redis.hset('stream:'+obb.id+':'+obb.login+':data',items=['end time',int(ended.timestamp()*1000),'stream length seconds',int(obb.stream_length_sec_final),'stream status',0])
            ok1,ok2,r = await pipe.delete(f'live:{obb.login}').srem('live:__all__',f'stream:{obb.id}:{obb.login}').xadd('history:__all__',{'id':obb.id,'login':obb.login}).execute()
            
            if not ended:
                ended=r[:-2]
            else:
                ended = ended.timestamp()*1000
                print("there's ended")    
        await self._redis.hset(f'stream:{obb.id}:{obb.login}:data',items=['end time',int(ended),'stream length seconds',int(int(ended)/1000)-int(started.timestamp()),'stream status',0])
        if not ok1 or not ok2:
            print(f'didnt delete {ok1} {ok2}: stream:{obb.id}:{obb.login}')
        return int(r[:-2])
            
    async def setStartHashData(self,started,obb):     
        async with self._redis.pipeline(transaction=True) as pipe:
            #ok1,ok2,ok3,ok4,ok5 = await (pipe.set('stream:'+obb.id+':'+obb.login+':name',obb.name).hset('stream:'+obb.id+':'+obb.login+':data',items=['start time',int(started.timestamp()*1000),'stream status',1]).sadd('login:__all__','stream:'+obb.id+':'+obb.login).sadd('login:'+obb.login,'stream:'+obb.id+':'+obb.login).hdel('stream:'+obb.id+':'+obb.login+':data','end time','stream length seconds').execute())
            await pipe.set(f'stream:{obb.id}:{obb.login}:name',obb.name).hset(f'stream:{obb.id}:{obb.login}:data',items=['start time',int(started.timestamp()*1000),'stream status',1]).sadd('live:__all__',f'stream:{obb.id}:{obb.login}').set(f'live:{obb.login}',obb.id).hdel(f'stream:{obb.id}:{obb.login}:data','end time','stream length seconds').execute()
            #print(ok1,ok2,ok3,ok4)
    
    async def saveChat(self,dt,message,obb):
        key = 'stream:'+obb.id+':'+obb.login+':chat'
        rs = ['time',int(dt.timestamp()*1000), 'stream time', int(dt.timestamp()-obb.streamstarted.timestamp()),'message type',message['message_type'],'chatter_user_name',message['chatter_user_name'],'text',message['message']['text']]
        
        if message['source_broadcaster_user_name']:
            rs += ['source_broadcaster_user_name',message['source_broadcaster_user_name']]             
        if message['color']:
            rs += ['color',message['color']]              
        if message['cheer']:
            rs += ['cheer',message['cheer']]    

        await self._redis.hset(key,items=rs)    
        
    async def saveChatLog(self,dt,data,obb):
        pass
        
    async def saveChatNoti(self,dt,data,obb):
        pass
        
    async def saveChatLogNoti(self,dt,data,obb):
        pass 
    
    async def updateLogins(self,logins,initial = False):
        if initial:
            self._logins = logins[:]   
 
        l = await self._redis.smembers('logins') | set(self._logins)
        b = False
        s = set(logins)-l
        
        if s:
            b = True
            print("removing ",s)
            for item in s:
                logins.remove(item)
   
        s = l - set(logins)
        if s:
            b=True
            print("adding ",s)
            logins.extend([i for i in s])
            
        return b
        
    async def setDisconnected(self,obbs):
        async with self._redis.pipeline(transaction=False) as pipe:
            for obb in obbs.values():
                pipe.hset('stream:'+obb.id+':'+obb.login+':data',items=['stream status',3])
            print('disconnected: ',await pipe.execute())   

    async def addTimeValues(self,timeh,minute,obbs):
        #async with self._redis.pipeline(transaction=True) as pipe:    
        #ts = self._redis.ts()   
        #print(await ts.madd([('stream:'+obb.id+':'+obb.login+':val-'+str(i+1),int(time.timestamp()*1000),val) for obb in obbs.values() for i,val in enumerate(obb.get_csv_data(minute)) if val]))        
              
        async with self._redis.pipeline(transaction=False) as pipe:
            for obb in obbs.values():
                v = {'val-'+str(i+1):val for i,val in enumerate(obb.get_csv_data(minute)) if val}
                if v:
                    pipe.xadd('stream:'+obb.id+':'+obb.login+':logs',v,id=int(timeh.timestamp()/60))
                else:
                    print('empty vals','stream:'+obb.id+':'+obb.login)
               
            await pipe.execute()  
    
    async def getStreamData(self,key, name = None):
        if not name:
            async with self._redis.pipeline(transaction=False) as pipe:
                ss, name = await (pipe.hgetall(key).get(key.rsplit(':',1)[0]+':name').execute())    
        else:
            ss = await self._redis.hgetall(key)
        s = {k:int(v) for k,v in ss.items()}
        s['id'] = key.split(':')[1]
        s['name'] = name
        s['login'] = key.split(':')[2]
        return s    

    async def getBunchStreamData(self,keys):
        async with self._redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key).get(key.rsplit(':',1)[0]+':name')
                
            ls = await pipe.execute()
            #print([(ls[i], ls[i + 1]) for i in range(0, len(ls) - 1, 2)])
        for key,ss in zip(keys,[(ls[i], ls[i + 1]) for i in range(0, len(ls) - 1, 2)]):    
            #print(ss)
            #print(name)
            s = {k:int(v) for k,v in ss[0].items()}
            s['id'] = key.split(':')[1]
            s['name'] = ss[1]
            s['login'] = key.split(':')[2]
            yield s
            
    async def getKeysIter(self,key,l):
        for s in l:
            async for j in self._redis.scan_iter(key.format(s)):   
            #    for j in res:
                    m = await self.getStreamData(j)
                    yield m,j
               

    async def getKeys(self,key,l):
        for s in l:
            i = 0
            r = -1
            while r!=0:
                r,res = await self._redis.scan(i,key.format(s))    
                async for m in self.getBunchStreamData(res):
                    yield m,'32'
                i = r
    
    async def getMemberIter(self,key,l):
        for s in l:
            i = None
            while i!=0:
                i,res = await self._redis.sscan(key.format(s),i if i else 0)    
                yield res
    async def getKeysBySet(self,l):
        for s in l:
            async for res in self._redis.sscan_iter(f'login:{s}'):  
                #print(res)
                m = await self.getStreamData(res+':data')
                yield m,res            
    
    async def getKeysBySet2(self,l):
        if l:
            await self._redis.sadd('logins',*l)
        async for keys in self.getMemberIter('login:{}',l if l else ['__all__']):
            #print(keys)
            async with self._redis.pipeline(transaction=False) as pipe:
                for key in keys:
                    pipe.hgetall(f'{key}:data').get(f'{key}:name')    
                data_names = await pipe.execute()
                #print(data_names)
            for key,ss in zip(keys,range(0, len(data_names) - 1, 2)):    
                #print(data_names[ss])
                s = {k:int(v) for k,v in data_names[ss].items()}
                s['id'] = key.split(':')[1]
                s['name'] = data_names[ss+1]
                s['login'] = key.split(':')[2]
                yield s,key    

    async def getLive(self,l):
        if l:
            async with self._redis.pipeline(transaction=False) as pipe:
                keys = [f'stream:{sid}:{login}' for login,sid in zip(l,(await pipe.mget(*[f'live:{i}' for i in l]).sadd('logins',*l).execute())[0]) if sid]
        else:         
            keys = await self._redis.smembers('live:__all__')
        async with self._redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(f'{key}:data').get(f'{key}:name')    
            data_names = await pipe.execute()
        for key,ss in zip(keys,range(0, len(data_names) - 1, 2)):    
                #print(key,ss)
                #print(data_names[ss])
                s = {k:int(v) for k,v in data_names[ss].items()}
                s['id'] = key.split(':')[1]
                s['name'] = data_names[ss+1]
                s['login'] = key.split(':')[2]
                yield s,key                   
        
    async def getHistory(self,l,count,start = None):
        if start:
            start=f'({start}-0'
        keys = 1    
        while count!=0 and keys:
            
            keys = [(f"stream:{i[0]}:{i[1]}",i[2]) for _,i in zip(range(count-1),((s[1]['id'],s[1]['login'],s[0]) for s in await self._redis.xrevrange("history:__all__",f'({start}' if start else "+","-",count if not l else 50) if not l or s[1]['login'] in l))]
            if keys:
                    start=keys[-1][1]
            #print(keys)
            async with self._redis.pipeline(transaction=False) as pipe:    
                for key in keys:
                    #print(f'{key[0]}:data')
                    pipe.hgetall(f'{key[0]}:data').get(f'{key[0]}:name')    
                data_names = await pipe.execute()     
            #print(data_names)        
            yield [{k:int(v) for k,v in data_names[ss].items()}|{'id':key[0].split(':')[1],'name':data_names[ss+1],'login':key[0].split(':')[2]}for key,ss in zip(keys,range(0, len(data_names) - 1, 2))]
            count -= len(keys)
       
    def ps(self):
        return self._redis.pubsub()    
        
    async def getDataStream(self,streams):
        return await self._redis.xread(streams=streams,block=0)
        
    async def get(self,key):
        return await self._redis.get(key)    
        
    async def getMentionGroups(self,group):
        names = await self._redis.smembers(f'mentions:{group}')
        if names:
            #print([await self.getMentionGroups(i) if i!=group else {i} for i in names])
            return set().union(*[await self.getMentionGroups(i) if i!=group else {i} for i in names])
        else:
            return {group}
    
    async def getMentions(self, nid = 0):
        res=[]
        for sid,data in next((datas for key,datas in await self._redis.xread({'mentions':nid}))):
            #print(data)
            s={'pattern':data['pattern']}
            if 'allowed' in data:
                s['allowed'] = await self.getMentionGroups(data['allowed'])
            if 'not allowed' in data:
                s['not_allowed'] = await self.getMentionGroups(data['not allowed'])
            res += [s]    
        return res    
