import asyncio
import sys
import time
import threading
from importlib.machinery import SourceFileLoader

async def main(workdir, **kwargs):
    immoscraper = SourceFileLoader("immoscraper", f"{workdir}/src/immoscraper.py").load_module()
    crawler = immoscraper.ImmoCrawler()
    await crawler.get_properties()
    crawler.to_csv(f"{workdir}/data/raw/data")

def async_run_main(workdir, **kwargs):
    asyncio.run(main(workdir))


if __name__ == "__main__":
    workdir = "/home/patchwork/Documents/projects_becode/immo_eliza_goats"
    async_run_main(workdir)
