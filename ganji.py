import requests
import parsel
import pymongo
import threading
from queue import Queue
import re
import time
client=pymongo.MongoClient('localhost',27017)
item_info=client['item_info']
item_infos=item_info['item_infos']
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
    }
class Get_url(threading.Thread):
    #获取所有链接
    def __init__(self,url_queue,info_queue, *args, **kwargs):
        super(Get_url,self).__init__(*kwargs, **kwargs)
        self.url_queue = url_queue
        self.info_queue = info_queue
    def run(self):
        while True:
            if self.url_queue.empty():
                print('采集线程结束')
                break
            url=self.url_queue.get()
            self.get_all_lines(url)
    def get_all_lines(self, url):
        # 将首页获取到的链接加上所获取的页数进行页面所有信息的链接拿到
        # info_url = '{}pn{}/'.format(url)
        response = requests.get(url=url,headers=headers)
        html = parsel.Selector(response.text)
        hourse_list = html.xpath("//div[@class='f-list-item ershoufang-list']/dl/dt/div")
        for item in hourse_list:
            url_line = item.xpath("./a/@href").extract_first()
            if 'http' in re.findall(r"^http",url_line):
                # 找到所有带有http头部的链接，并进行切割去掉？之后的字符
                url_line=url_line.split('?')[0]
                self.info_queue.put(url_line)
            else:
                #其他的为没有请求协议的链接,需要加上http:
                self.info_queue.put('http:'+url_line)

class Get_info(threading.Thread):
    # 获取所有信息类
    def __init__(self, url_queue, info_queue, *args, **kwargs):
        super(Get_info, self).__init__(*kwargs, **kwargs)
        self.url_queue = url_queue
        self.info_queue = info_queue
    def run(self):
        while True:
            if self.url_queue.empty() and self.info_queue.empty():
                print('全部采集完成')
                break
            url=self.info_queue.get()
            self.get_all_info(url)
    def get_all_info(self,url):
        # 获取所有房屋信息的详情
        response=requests.get(url=url,headers=headers)
        html=parsel.Selector(response.text)
        title=html.xpath("//p[@class='card-title']/i/text()").extract_first()
        price=html.xpath("//div[@class='price-wrap']/span[1]/text()").extract_first()
        pay_way=html.xpath("//div[@class='price-wrap']/span[2]/text()").extract_first()
        huxing=html.xpath("//ul[@class='er-list f-clear']/li[1]/span[2]/text()").extract_first()
        size=html.xpath("//ul[@class='er-list f-clear']/li[2]/span[2]/text()").extract_first()
        chaoxiang=html.xpath("//ul[@class='er-list f-clear']/li[3]/span[2]/text()").extract_first()
        floor=html.xpath("//ul[@class='er-list f-clear']/li[4]/span[2]/text()").extract_first()
        zhuagxiu=html.xpath("//ul[@class='er-list f-clear']/li[5]/span[2]/text()").extract_first()
        item_infos.insert_one({'title':title,'pay_way':pay_way,'price':price,'huxing':huxing,'size':size,'chaoxiang':chaoxiang,'floor':floor,'zhuagxiu':zhuagxiu})
        time.sleep(1)

def main():
    url_queue=Queue(100)
    info_queue=Queue(1000)
    for i in range(1,3):
        url='http://bj.ganji.com/zufang/pn{}'.format(i)
        url_queue.put(url)
    time.sleep(2)
    for i in range(3):
        t1=Get_url(url_queue,info_queue)
        t1.start()
        t2=Get_info(url_queue,info_queue)
        t2.start()
if __name__ == '__main__':
    main()





# def get_urlist(url_queue):
#     #获取赶集房屋类首页的所有链接并存到队列中，由于有几个页面的数据存储位置不同，需要分别写代码
#     url='http://bj.ganji.com/fang'
#     first_url='http://bj.ganji.com'
#     response=requests.get(url=url,headers=headers)
#     html=BeautifulSoup(response.text,'lxml')
#     all_line=html.select('li.nav-item ul li a')
#     for item in all_line:
#         url_list=item.get('href')
#         url_lines=first_url+url_list
#         url_queue.put(url_lines)
#     return url_queue
        # item_first_urls.insert_one({'url':url_lines})
        # print(url_lines)
