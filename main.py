import asyncio
import sys
import time
import threading

#from scraper.immoscraper import ImmoCrawler
from importlib.machinery import SourceFileLoader
immoscraper = SourceFileLoader("immoscraper", "/home/patchwork/Documents/projects_becode/immo_eliza_goats/scraper/immoscraper.py").load_module()


#def spinner():
#    while True:
#        for cursor in "|/-\\":
#            sys.stdout.write(cursor)
#            sys.stdout.flush()
#            time.sleep(0.1)
#            sys.stdout.write("\b")


async def main():
    #print("\nImmoCrawler is running...", end="", flush=True)
    #threading.Thread(
    #    target=spinner, daemon=True
    #).start()  # Start spinner in a separate thread
    crawler = immoscraper.ImmoCrawler()
    await crawler.get_properties()
    crawler.to_csv("/home/patchwork/Documents/projects_becode/immo_eliza_goats/data/raw/data")

def async_run_main():
    asyncio.run(main())


#if __name__ == "__main__":
#
#    asyncio.run(main_test())
#
