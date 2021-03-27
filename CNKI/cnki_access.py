from selenium import webdriver
import time

driver = webdriver.Chrome("../chromedriver")
url = "http://cnki.net"
driver.get(url=url)

search_box = driver.find_element_by_xpath(xpath="//*[@id=\"txt_SearchText\"]")
search_box.send_keys("乡村振兴")

search_button = driver.find_element_by_xpath(xpath="/html/body/div[1]/div[2]/div/div[1]/input[2]")
search_button.click()
time.sleep(5)

category_button = driver.find_element_by_xpath(xpath="/html/body/div[5]/div[1]/div/ul[1]/li[1]/a")
category_button.click()
driver.implicitly_wait(10)
time.sleep(1)

citation_desc_button = driver.find_element_by_xpath(xpath="/html/body/div[5]/div[2]/div[2]/div[2]/form/div/div[1]/div[2]/div[3]/ul/li[3]")
citation_desc_button.click()
driver.implicitly_wait(10)

time.sleep(1)

article_units = driver.find_elements_by_class_name("name")

for article_unit in article_units:
    print(article_unit.text)


driver.quit()