import asyncio
import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import xlsxwriter


fake_u = UserAgent().chrome
okveds_osn = {'вид деятельности': [],
              'оквэд': [],
              'название оквэда': []
              }
okveds_dop = {'вид деятельности': [],
              'оквэд': [],
              'название оквэда': []
              }


async def add_okveds(session, link):
    for l in link:
        name = l.text.lstrip().replace('\n', '')
        resp = await session.get(l.attrs['href'], headers={'User-Agent': fake_u})
        soup = BeautifulSoup(await resp.text(), "html.parser")
        table = soup.find('table')
        tbody = table.find_all('tbody')

        flag_okved = None
        for i in tbody:
            if flag_okved == 'osn' and i.text.replace('\n', '').replace(' ', '')[:2] == 'ОК':
                okveds_osn['вид деятельности'].append(name)
                okveds_osn['оквэд'].append(i.text.lstrip().replace('\n', '').split()[1])
                okveds_osn['название оквэда'].append(' '.join(i.text.lstrip().replace('\n', '').split()[2:]))
            elif flag_okved == 'dop' and i.text.replace('\n', '').replace(' ', '')[:2] == 'ОК':
                okveds_dop['вид деятельности'].append(name)
                okveds_dop['оквэд'].append(i.text.lstrip().replace('\n', '').split()[1])
                okveds_dop['название оквэда'].append(' '.join(i.text.lstrip().replace('\n', '').split()[2:]))
            if i.text.lstrip().replace('\n', '').replace(' ', '') == 'Кодосновноговидадеятельности':
                flag_okved = 'osn'
            elif 'ОКВЭД' not in i.text.replace('\n', '').replace(' ', ''):
                flag_okved = 'dop'


async def gather_data():
    async with aiohttp.ClientSession() as session:
        links = []
        for page in range(1, 14):
            resp = await session.get(f"https://okvedkod.ru/kit?page={page}", headers={'User-Agent': fake_u})
            soup = BeautifulSoup(await resp.text(), "html.parser")
            links.append(soup.find(class_="tile _mt1em").find_all('a'))

        tasks = []
        for link in links:
            task = asyncio.create_task(add_okveds(session, link))
            tasks.append(task)

        await asyncio.gather(*tasks)


def main():
    asyncio.run(gather_data())

    osn_ok = pd.DataFrame(okveds_osn)
    dop_ok = pd.DataFrame(okveds_dop)
    writer = pd.ExcelWriter('okveds.xlsx', engine='xlsxwriter')
    osn_ok.to_excel(writer, sheet_name='Основные', index=False)
    dop_ok.to_excel(writer, sheet_name='Дополнительные', index=False)
    writer._save()


if __name__ == '__main__':
    main()
