import time
from crawler import crawler as crawler_single
from crawler_mthread import crawler as crawler_mthread
import operator

class Timer:
    def __enter__(self):
        self.start = time.time()
        return self
    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        
if __name__ == "__main__":

    bot_single = crawler_single(None,"urls.txt")
    with Timer() as t:
        bot_single.crawl(depth=1)
    print 'Single threaded took %.06f sec.' % t.interval
    
    bot_multi = crawler_mthread(None,"urls.txt")
    with Timer() as t:
        bot_multi.crawl(depth=1)
    print 'Multi threaded took %.06f sec.' % t.interval
