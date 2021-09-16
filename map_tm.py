from lxml import html
import pandas as pd
from fake_headers import Headers
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore',InsecureRequestWarning)
import requests
from bs4 import BeautifulSoup as bs
import re
import time
from tqdm import tqdm
import decimal
from heapq import nlargest
from multiprocessing.dummy import Pool as ThreadPool
import time

start_time = time.time()
header = Headers(headers=True).generate()
f1 = 'D:\\transfermarkt\\refs.txt'
pool = ThreadPool(8)

players_df = pd.DataFrame(columns=['name', 'age', 'cost', 'country', 'year', 'pos'])
cost_df = pd.DataFrame(columns=['country', 'conf', 'year', 'counter', 'top11', 'top11_mean', 'top', 'top_perc', 'rank2021'])

def read_file(path):
    with open(path) as input_file:
        i = input_file.read()
        links = i.split(',')
        links_updated = []
        for link in links:
            link = link.replace('[', '').replace(']', '').replace('\'', '').replace(' ', '')
            links_updated.append(link)
    return links_updated


def parse_tm(link):
    global players_df, cost_df
    r = requests.get(url=link, headers=header)
    root = html.fromstring(r.text)
    soup = bs(r.text, 'lxml')
    country = soup.find("div", class_=['dataName']).text.strip()
    conf = root.xpath('//*[@id="verein_head"]/div/div/div[2]/div/div[2]/p[1]/span[2]/text()')
    conf = conf[0].strip()
    rank2021 = root.xpath('//*[@id="verein_head"]/div/div/div[2]/div/div[2]/p[2]/span[2]/a/text()')
    rank2021 = rank2021[0]
    rank2021 = rank2021.split('.')
    rank2021 = rank2021[0].strip()
    for i in range(2010, 2020):
        url = link + f'?saison_id={i}'
        r = requests.get(url=url, headers=header)
        soup = bs(r.text, 'lxml')
        root = html.fromstring(r.text)
        info = soup.find_all("tr", class_=['odd', 'even'])
        year = i + 1
        counter = 0
        full_cost = 0
        full_cost_list = []
        top_perc = 0.0
        for i in range(len(info)):
            # age = info[i].find('td', class_='zentriert').text
            #     name_pos = info[i].find('td', class_='posrela').text.split('.')
            #     name = name_pos[0][:-1]
            #     reg = re.compile('[^а-яА-Я ]')
            name = info[i].find('span', class_='hide-for-small').text
            pos = info[i].find('td', class_='posrela').text.split(' ')
            pos = pos[-1]
            reg = re.compile('[^а-яА-Я ]')
            pos = reg.sub('', pos)
            i = i + 1
            age = root.xpath(f'//*[@id="yw1"]/table/tbody/tr[{i}]/td[3]/text()')
            age = age[0]
            cost = root.xpath(f'//*[@id="yw1"]/table/tbody/tr[{i}]/td[5]/text()')
            cost = cost[0]
            cost = cost.replace(' млн € ', '').replace(",", ".")
            # reg = re.compile('[^а-яА-Я ]')
            # cost = reg.sub('', cost)
            if len(cost) < 3:
                cost = 0.0
            elif ('тыс' in cost):
                cost = cost.replace(' тыс €', '')
                cost = float(cost)
                cost = cost / 1000
            cost = float(cost)
            if cost > 0:
                full_cost_list.append(cost)
            # print(name, pos, age, cost, country, year)
            players_df = players_df.append(pd.DataFrame(data={'name': name, 'age': age, 'cost': cost,
                                                              'country': country, 'year': year, 'pos': pos},
                                                        index=[0]))
            if cost > 0:
                counter += 1
                full_cost += cost

        if counter > 0:
            full_cost_mean = round((full_cost / counter), 1)
        else:
            full_cost_mean = 0.0
            full_cost_list.append(0)

        if len(full_cost_list) > 10:
            top11 = round(sum(nlargest(11, full_cost_list)), 2)
            top11_mean = round((top11 / 11), 2)
            top = sum(nlargest(1, full_cost_list))
            top_perc = round((top / (sum(nlargest(11, full_cost_list))) * 100), 2)
        else:
            top11 = 0.0
            top11_mean = 0.0

        if len(full_cost_list) > 10:
            top = sum(nlargest(1, full_cost_list))
            top_perc = round((top / (sum(nlargest(11, full_cost_list))) * 100), 2)
        else:
            top11 = 0.0
            top11_mean = 0.0
            top = (nlargest(1, full_cost_list))
            top_perc = 0.0

        cost_df = cost_df.append(pd.DataFrame(data={'country': country, 'conf': conf, 'year': year, 'counter': counter,
                                                    'full_cost_mean': full_cost_mean, 'top11': top11,
                                                    'top11_mean': top11_mean, 'top': top, 'top_perc': top_perc,
                                                    'rank2021': rank2021}, index=[0]))


def parse_tm_2021(link):
    global players_df, cost_df
    r = requests.get(url=link, headers=header)
    soup = bs(r.text, 'lxml')
    country = soup.find("div", class_=['dataName']).text.strip()
    root = html.fromstring(r.text)
    info = soup.find_all("tr", class_=['odd', 'even'])
    conf = root.xpath('//*[@id="verein_head"]/div/div/div[2]/div/div[2]/p[1]/span[2]/text()')
    conf = conf[0]
    rank2021 = root.xpath('//*[@id="verein_head"]/div/div/div[2]/div/div[2]/p[2]/span[2]/a/text()')
    rank2021 = rank2021[0]
    rank2021 = rank2021.split('.')
    rank2021 = rank2021[0].strip()
    year = 2021
    counter = 0
    full_cost = 0
    full_cost_list = []
    for i in range(len(info)):
        name = info[i].find('div', class_='di nowrap').text
        pos = info[i].find('table', class_='inline-table').text
        reg = re.compile('[^а-яА-Я ]')
        pos = reg.sub('', pos)
        pos = pos.split(' ')
        pos = pos[-1]
        i = i + 1
        age = root.xpath(f'//*[@id="yw1"]/table/tbody/tr[{i}]/td[4]/text()')
        age = age[0]
        cost = root.xpath(f'//*[@id="yw1"]/table/tbody/tr[{i}]/td[6]/text()')
        cost = cost[0]
        cost = cost.replace(' млн € ', '').replace(",", ".")
        # reg = re.compile('[^а-яА-Я ]')
        # cost = reg.sub('', cost)
        if len(cost) < 3:
            cost = 0.0
        elif ('тыс' in cost):
            cost = cost.replace(' тыс €', '')
            cost = float(cost)
            cost = cost / 1000
        cost = float(cost)
        full_cost_list.append(cost)
        players_df = players_df.append(pd.DataFrame(data={'name': name, 'age': age, 'cost': cost,
                                                          'country': country, 'year': year, 'pos': pos},
                                                    index=[0]))
        if cost > 0:
            counter += 1
            full_cost += cost

    if counter > 0:
        full_cost_mean = round((full_cost / counter), 1)
    else:
        full_cost_mean = 0.0
        full_cost_list.append(0)

    if counter > 10:
        top11 = round(sum(nlargest(11, full_cost_list)), 2)
        top11_mean = round((top11 / 11), 2)
        top = sum(nlargest(1, full_cost_list))
        top_perc = round((top / (sum(nlargest(11, full_cost_list))) * 100), 2)
    else:
        top11 = 0.0
        top11_mean = 0.0
        top = (nlargest(1, full_cost_list))
        top_perc = 0.0

    cost_df = cost_df.append(pd.DataFrame(data={'country': country, 'conf': conf, 'year': year, 'counter': counter,
                                                    'full_cost_mean': full_cost_mean, 'top11': top11,
                                                    'top11_mean': top11_mean, 'top': top, 'top_perc': top_perc,
                                                    'rank2021': rank2021}, index=[0]))



link = read_file(f1)
link = link[200:207]
pool.map(parse_tm, link)
pool.map(parse_tm_2021, link)
pool.close()
pool.join()
print(cost_df)
elapsed_time_secs = time.time() - start_time
players_df.reset_index(inplace=True, drop=True)
players_df.to_csv('D:\\transfermarkt\\players1.txt', mode='a', header=False, index=False)
cost_df.reset_index(inplace=True, drop=True)
cost_df.to_csv('D:\\transfermarkt\\teams_cost1.txt', mode='a', header=False, index=False)
print(elapsed_time_secs)
