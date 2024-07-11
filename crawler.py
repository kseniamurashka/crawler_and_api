import requests
import json
import hashlib
from bs4 import BeautifulSoup
from flask import Flask
import psycopg2

import time

counter = 0

class Crawler:
    url = ""
    start = 0

    def __init__(self, URL, st):
        Crawler.url = URL
        Crawler.start = st
    
    def download_page(self, url_):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
        try:
            response = requests.get(url_, headers=headers)
            return response.text
        #check invalid url
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

    def parse_page(self):
        jobs = []
        if ("/search/" not in self.url and "/vacancies/" not in self.url):
            jobs.append(self.url)
            return jobs
        
        try:
            html_page = self.download_page(self.url)
        #check invalid url
        except SystemExit:
            return []
        
        soup_page = BeautifulSoup(html_page, 'html.parser')
        if (soup_page.find('div', class_ = "bloko-text bloko-text_secondary") != None and "Ошибка" in soup_page.find('div', class_ = "bloko-text bloko-text_secondary").text):
            print("Error")
            return []
        else: 
            for a in soup_page.findAll('a', class_='bloko-link', target = '_blank', href=True):
                if ('vacancy/' in a['href']):
                    jobs.append(a['href'])
            return jobs
        
    def make_insert_str(self, id, hash_vacancy, state, role, address, experience, employment, salary, id_empl, url):
        result_str = "insert into vacancies values ("+ id + ", " + hash_vacancy + ", '" + state + "', '" + role + "', " + address + ", '" + experience + "', '" + employment + "', " + salary + ", " + id_empl + ", '" + url + "');"
        return result_str
    
    def my_hash(self, data):
        sha256_hash = hashlib.new('sha256')
        sha256_hash.update(data.encode())
        return str(int(sha256_hash.hexdigest(), 16) % 99999999)
         
    def get_id_vac (self, cur):
        cur.execute("select max(id_vacancy) from vacancies")
        answer = str(cur.fetchall()[0])[1:-2]
        if (answer == "None"):
            id = 1
        else:
            id = int(answer) + 1
        return (id)

    def get_id_eml(self, cur, name):
        cur.execute("select max(id_employer) from employers;")
        answer = str(cur.fetchall()[0])[1:-2]
        #check if table is empty
        if (answer == "None"):
            id = 1
        #table not empty
        else:
            cur.execute("select id_employer from employers where name = '" + name + "';")
            s = cur.fetchall()
            #if this name doen't exist
            if (str(s) == "[]"):
                id = int(answer) + 1
            else:
                return int(answer)
        return id

    def parse_vacancy(self, ref, conn):
        cur_vacancies = conn.cursor()
        cur_empl = conn.cursor()

        try:
            html_job = self.download_page(ref)
        except SystemExit:
            return
        
        soup_job = BeautifulSoup(html_job, 'html.parser')
        if (soup_job.find('script', type='application/ld+json') is None):
            return
        
        json_job = json.loads(soup_job.find('script', type='application/ld+json').text)
        
        state = ""
        if (str(soup_job.select('html.desktop meta')).find('в архиве') != '-1'):
            state = "open"
        else:
            state = "close"

        city, building = "", ""
        metro_stations = []
        var1 = soup_job.find('span', attrs={"data-qa": "vacancy-view-raw-address"})
        var2 = soup_job.find('p', attrs = {"data-qa": "vacancy-view-location"})
        if (var2 != None):
            inf = var2
        elif (var1 != None):
            inf = var1
        else:
            inf = None
        if (inf != None):
            content = inf.contents
            if (len(content[0].split(",")) > 1):
                city = str(content[0].split(",")[0])
                building = str(content[0]).replace(city, "").replace(",", "",content[0].replace(city, "").count(","))
                building = building.replace("\"", "", building.count("\""))
            else:
                city = str(content[0]).replace("\"", "", city.count("\""))
            for a in content:
                if 'metro-station' in str(a):
                    if (str(a).split('</span>')[1] not in metro_stations):
                        metro_stations.append(str(a).split('</span>')[1])
            if (content[-1] != content[0]) and "метро" in str(content[-1]):
                if (content[-1] not in metro_stations): 
                    if (str(content[-1])[0] == ','):
                        metro_stations.append(str(content[-1])[2:])
                    else:
                        metro_stations.append(content[-1])
            if (content[-1] != content[0]) and "линия" not in str(content[-1]) and "метро" not in str(content[-1]) and 'metro-station' not in str(content[-1]):
                    building = str(content[-1])[2:]
        metro_stations_arr_str = "["
        for station in metro_stations:
            if (station == metro_stations[0]):
                metro_stations_arr_str += "\"" + station + "\""
            else:
                metro_stations_arr_str += ", \"" + station + "\""
        metro_stations_arr_str += "]"
        address_str = "'{\"city\": \""+ city + "\", \"building_address\": \"" + building + "\", \"metro_stations\": "+ metro_stations_arr_str + "}'"
        
        experience = str(soup_job.find('span', attrs= {"data-qa": "vacancy-experience"}))[35:-7]

        role = ""
        if ('title' in json_job):
            role = json_job['title'].lower()
        
        employmentType = ""
        if ('employmentType' in json_job):
            if (json_job['employmentType'] == 'FULL_TIME'):
                employmentType = "Полная занятость"
            elif (json_job['employmentType'] == 'PART_TIME'):
                employmentType = "Частичная занятость"
            elif (json_job['employmentType'] == 'INTERN'):
                employmentType = "Стажировка"
            elif (json_job['employmentType'] == 'TEMPORARY'):
                employmentType = "Проектная работа"
            elif (json_job['employmentType'] == 'VOLUNTEER'):
                employmentType = "Волонтерство"
        
        currency, from_, to = "", "", ""
        salary_str = "'{\"currency\": \"" + currency + "\", \"from\": \""+ from_ + " \", \"to\": \""+ to + "\"}'"
        if ('baseSalary' in json_job):
            salary = json_job['baseSalary']
            if ('currency' in salary): currency = salary['currency']
            if ('value' in salary):
                if ('minValue' in salary['value']): from_ = str(salary['value']['minValue'])
                if ('maxValue' in salary['value']): to = str(salary['value']['maxValue'])
            salary_str = "'{\"currency\": \"" + currency + "\", \"from\": \""+ from_ + " \", \"to\": \""+ to + "\"}'"

        employer_name = ""
        if ('hiringOrganization' in json_job and 'name' in json_job['hiringOrganization']):
            employer_name = json_job['hiringOrganization']['name']

        id_vacancy = self.get_id_vac(cur_vacancies)
        
        id_eml = self.get_id_eml(cur_empl, employer_name)
        
        hash_vac = self.my_hash(state + str(role) + str(address_str) + experience + employmentType + salary_str)
        
        result_str = self.make_insert_str(str(id_vacancy), hash_vac, state, str(role).replace("'", " ", str(role).count("'")), address_str, experience, employmentType, salary_str, str(id_eml), ref)
        
        cur_vacancies.execute("SELECT * FROM vacancies WHERE hash_vacancy =" + hash_vac)
        r1 = cur_vacancies.fetchall()
        if (str(r1) == "[]"): 
            cur_vacancies.execute(result_str)
            
            cur_empl.execute("SELECT * FROM employers WHERE name = '" + employer_name + "'")
            r2 = cur_empl.fetchall()
            if (str(r2) == "[]" and employer_name != ""):
                cur_empl.execute("insert into employers values (" + str(id_eml) + ", '" + employer_name + "')")
        conn.commit()
        return
    
    def check_time(self):
        return time.time() - self.start

    def save_data(self):
        data = self.parse_page()
        conn = psycopg2.connect(database="practice5", user="postgres", password="rere2004", host="localhost", port=5432)
        for ref in data:
            print (ref)
            self.parse_vacancy(ref, conn)
            #check time of working crawler
            if (self.check_time() > 5 * 60):
                return
        return
    
    def perform(self):
        print('start perform')
        self.save_data()
        print('complete perform') 

def start():
    start = time.time()
    number_of_page = 10
    url_ = input("Введите url: ")
    #for sorting by time
    if "order_by=publication_time" not in url_:
        if "&" in url_: url_ += "&order_by=publication_time"
        else:
            if url_[-1] != "?": url_ += "?order_by=publication_time"
            else: url_ += "order_by=publication_time"
    print(url_)
    print('\n')
    for i in range (number_of_page):
        url = url_
        if ("/search/" not in url):
            crawler = Crawler(url, start)
            crawler.perform()
            break
        elif ("?" in url):
            url += "&page="+ str(i)
        else:
            url += "?page=" + str(i)
        crawler = Crawler(url, start)
        crawler.perform()

def main():
    global counter
    counter += 1
    try:
        start()
    except KeyboardInterrupt: 
        print("work was stopped")

if __name__ == '__main__':
    if (counter == 0):
        main()