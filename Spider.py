from pyecharts import options
from pyecharts.charts import Map
from pyppeteer import launch
from lxml import etree
import pymysql
import asyncio
import json


class EpidemicSpider:
    total_detail = []

    def __init__(self):
        self.data = None

    async def get_data(self):
        """
        Pyppeteer获取数据
        :return:
        """
        browser = await launch(args=['--disable-infobars'])
        page = await browser.newPage()
        await page.setViewport({'width': 1280, 'height': 800})
        await page.goto('https://voice.baidu.com/act/newpneumonia/newpneumonia')
        await page.evaluate(
            '''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }'''
        )
        await page.evaluate('window.scrollTo(0, 6650)')
        await page.click('#foreignTable > .Common_1-1-284_3lDRV2')
        await page.evaluate('window.scrollTo(0, 15000)')
        self.data = await page.content()

    def domestic(self, html):
        """
        国内疫情数据解析
        :param html:
        :return:
        """
        china_lst = []
        china_lst.append('中国')
        other_info = html.xpath('//*[@id="ptab-0"]/div[1]/div[3]')
        for detail in other_info:
            increase = detail.xpath('./div[1]/div[3]/span/text()')[0]
            if increase[0] == '+':
                increase = increase[1:]
            else:
                increase = 0
            china_lst.append(increase)
            existing = html.xpath('//*[@id="ptab-0"]/div[1]/div[2]/div[1]/div[2]/text()')[0]
            china_lst.append(existing)
            altogether = detail.xpath('./div[1]/div[2]/text()')[0]
            china_lst.append(altogether)
            cure = detail.xpath('./div[3]/div[2]/text()')[0]
            china_lst.append(cure)
            death = detail.xpath('./div[4]/div[2]/text()')[0]
            china_lst.append(death)
        self.total_detail.append(china_lst)

    def abroad(self, html):
        """
        国外疫情数据解析
        :param html:
        :return:
        """
        country_lst = html.xpath('//*[@id="foreignTable"]/table//tr/td/table//tr')
        for country_tr in country_lst:
            detail_lst = []
            for index, detail in enumerate(country_tr):
                tostring = ''
                if index == 0:
                    country = detail.xpath('./a/div[1]/text()')
                    if not country:
                        country = detail.xpath('./div/text()')
                    tostring += str(country[0])
                    detail_lst.append(tostring)
                else:
                    other = detail.xpath('./text()')[0]
                    tostring += str(other)
                    detail_lst.append(int(tostring))
            self.total_detail.append(detail_lst)

    async def parse(self):
        """
        汇总，使代码逻辑清晰
        :return:
        """
        await self.get_data()
        html = etree.HTML(self.data)
        self.domestic(html)
        self.abroad(html)
        return self.total_detail


class AnalysisAndStorage:

    def __init__(self, task):
        """
        获取协程返回值，连接数据库持久化存储
        :param task:
        """
        self.result = task.result()
        self.conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='19981010',
            database='epidemic',
            charset='utf8'
        )
        self.cursor = self.conn.cursor()
        self.storage()
        self.analysis()

    def storage(self):
        """
        MySQL数据库操作
        :return:
        """
        for i in self.result:
            sql = "INSERT INTO global_epidemic(country,increase,existing,altogether,cure,death) VALUES(%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, (i[0], i[1], i[2], i[3], i[4], i[5]))
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def analysis(self):
        """
        数据分析可视化
        :return:
        """
        data_list = [(i[0], i[3]) for i in self.result]
        with open('CountryMap.json', 'r') as f:
            country_map = json.load(f)
        map = Map(options.InitOpts(width='1366px', height='768px'))
        map.add("确诊病例", data_list, 'world', name_map=country_map, is_map_symbol_show=False)
        map.set_series_opts(label_opts=options.LabelOpts(is_show=False))
        map.set_global_opts(
            title_opts=options.TitleOpts(title='全球疫情数据可视化展示'),
            visualmap_opts=options.VisualMapOpts(max_=100000)
        )
        map.render('World_Epidemic_Analysis.html')


if __name__ == '__main__':
    loop = asyncio.get_event_loop() # 创建事件循环
    epidemicSpider = EpidemicSpider()
    tasks = asyncio.ensure_future(epidemicSpider.parse())   # 放入任务对象
    tasks.add_done_callback(AnalysisAndStorage) # 绑定回调
    loop.run_until_complete(tasks)  # 运行事件循环
