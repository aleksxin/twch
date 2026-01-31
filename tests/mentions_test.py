from twitchdata_redis import RedisCM
import pytest
#import time
import re

@pytest.mark.asyncio
async def test_bonnie():
  #  t = time.perf_counter() 
    async with RedisCM() as r:
        ment = await r.getMentions()
        print(ment)
        assert re.search(ment[0]['pattern'],'fds BONNIE',re.IGNORECASE) is not None
        
@pytest.mark.asyncio
async def test_bonnie2():
  #  t = time.perf_counter() 
    async with RedisCM() as r:
        ment = await r.getMentions()
        print(ment)
        assert re.search(ment[0]['pattern'],'fds bonito',re.IGNORECASE) is not None        
  #  print("Elapsed time:", time.perf_counter() - t)       
#asyncio.run(main(sys.argv[1:]))        