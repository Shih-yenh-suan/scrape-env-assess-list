import os
import pandas as pd
import random
import re
import requests
import time
from lxml import etree


URL = "http://114.251.10.92:8080/XYPT/unit/list"

UA = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded"
}

DATA = {
    "pageNo": 1,
    "pageSize": 20,
    "noCreditState": 0
}


def retry_on_failure(func):
    '''当爬取失败时，暂停重试'''
    try:
        result = func()
        return result
    except Exception as e:
        print(f'错误: {e}, 暂停 3 秒')
        time.sleep(3)
        return retry_on_failure(func)


def getListUrl(page):
    DATA["pageNo"] = page
    response = retry_on_failure(lambda: requests.post(
        url=URL, data=DATA, headers=UA)).text
    parsedHtmls = etree.HTML(response)

    # 获取需要的内容
    listName = parsedHtmls.xpath(
        '//table[@id = "contentTable"]/tbody/tr/td/a/text()')
    listName = [re.sub(r'(\s)', '', u) for u in listName]
    listName = listName[::4]

    listUrl = parsedHtmls.xpath(
        '//table[@id = "contentTable"]/tbody/tr/td/a/@href')
    listUrl = listUrl[::4]
    listUrl = [re.search(r"'(\d+)'", u).group(1) for u in listUrl]

    listLocAndCode = parsedHtmls.xpath(
        '//table[@id = "contentTable"]/tbody/tr/td/text()')
    listLocAndCode = [re.sub(r'(\s)', '', u) for u in listLocAndCode]

    listCode = listLocAndCode[3::12]
    listLoc = listLocAndCode[4::12]
    listStatus = listLocAndCode[9::12]
    listInfo = [[a, b, c, d, e]
                for (a, b, c, d, e) in zip(listName, listUrl, listCode, listLoc, listStatus)]

    return listInfo


def getUnitInfo(unitInfo):
    unitUrl = f"http://114.251.10.92:8080/XYPT/score/detailInfo?objectType=1&objectId={unitInfo[1]}"

    responseUnit = retry_on_failure(lambda: requests.post(
        url=unitUrl, headers=UA)).text
    parsedHtmlsUnit = etree.HTML(responseUnit)

    unitScoreTable = parsedHtmlsUnit.xpath(
        '//table[@id="headSorce"]//text()')
    unitScoreTable = [re.sub(r'(\s)', '', u) for u in unitScoreTable]

    # ['第1记分周期5', '第2记分周期5', '第3记分周期0', '第4记分周期0', '第5记分周期-']
    unitScore = [item for item in unitScoreTable if "记分周期" in item]

    unitScore = [u[6:] for u in unitScore]
    # ['2019', '2020', '2021', '2022', '']
    unitInter = unitScoreTable[3::4]
    unitInter = [unitScoreTable[i + 1]
                 for i in range(len(unitScoreTable) - 1) if "记分周期" in unitScoreTable[i]]

    unitInter = [u[:4] for u in unitInter]

    # [2019:5,2020:5,...]
    unitScoreAndInter = [f"{args+1}:{i}:{s}" for args,
                         (s, i) in enumerate(zip(unitScore, unitInter))]

    unitInfo += unitScoreAndInter
    del unitInfo[1]

    return unitInfo


df_index = ['Name', 'id', 'locate', 'status', 'inter1',
            'inter2', 'inter3', 'inter4', 'inter5']
csv_path = "D:\ZZZMydocument\Codes\实验\output.csv"


def main():

    # print(getUnitInfo(getListUrl(4)[12]))
    if not os.path.exists(csv_path):
        df = pd.DataFrame(columns=df_index)
        df.to_csv(csv_path, index=False)

    for page in range(1, 336):

        time.sleep(random.betavariate(1, 3))

        listContent = getListUrl(page)

        print("===" * 20)
        print(f"当前正爬取第 {page} 页")
        print("===" * 20)

        for units in range(len(listContent)):
            info = getUnitInfo(listContent[units])

            # 判断是否存在
            unitID = info[1]
            existing_df = pd.read_csv(csv_path)
            if (existing_df.iloc[:, 1] == unitID).any():
                print(f"{info[0]} 已存在于CSV文件中，跳过。")
                continue

            # 如果id不存在于CSV文件中，将数据追加到CSV文件
            print(f"当前：第 {page} 页 {info[0]}")
            df = pd.DataFrame([info], columns=df_index)
            df.to_csv(csv_path, mode='a', header=False,
                      index=False)

            time.sleep(random.betavariate(1, 3))


if __name__ == '__main__':
    main()
