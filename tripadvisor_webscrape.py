# -*- coding: utf-8 -*-
"""
File that contains functions to webscrape data from tripadvisor restaurants page for Barcelona
"""

import pandas as pd
import numpy as np
import time, os
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from urllib.request import urlopen

def scrape_tripadvisor(
    savepath="dump/tripadvisor/tripadvisor.csv",
    pages_to_scrape=10,
    images_per_resto=5 , # number of times to click next image
    next_max_tries=10,  # mac number of tries to click the next button
    url="https://www.tripadvisor.com/Restaurants-g187497-Barcelona_Catalonia.html"
):

    # import the webdriver
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get(url)
    
    # give the DOM time to load
    time.sleep(10)
    
    # Click the "I agree" button.
    driver.find_element("xpath","//button[contains(text(), 'I Accept')]").click()
    
    # Create savepath directories if needed
    savedir='/'.join(savepath.split('/')[:-1])
    savedir_img=os.path.join(savedir,'images')
    
    for s in [savedir, savedir_img]:
        if not os.path.exists(s):
            print('Create directory:', s)
            os.makedirs(s)
    
    restos=[]
    
    for x in range(pages_to_scrape):
    
        time.sleep(5)
        
        # Get divs per resto listing
        container=driver.find_elements("xpath",
                             "//div[@class='YtrWs']//div[@data-test[substring(.,string-length(.) - string-length('list_item') + 1) = 'list_item']]")
        temp=[]
        
        # A loop defined by the number of restos  
        for i in range(len(container)):   
            names=container[i].find_element("xpath", ".//a[@class='Lwqic Cj b']")
            number=names.text.split('. ')[0].strip()
            name=names.text.split('. ')[-1].strip()
            number=np.nan if number==name else int(number)
            url=names.get_property('href')
            rating=float(container[i].find_element("xpath", ".//*[name()='svg' and @class='UctUV d H0']").get_attribute('aria-label').split(' of ')[0])
            reviewer=int(container[i].find_element("xpath", ".//span[@class='IiChw']").text.replace('(','').replace(')','').replace(',','').replace('reviews','').replace('review',''))
            cuisines=(container[i].find_elements("xpath", ".//div[@class='hBcUX XFrjQ mIBqD']//span[@class='tqpbe']//span[@class='SUszq']"))
            cuisine=[j.text for j in cuisines]
            if len(cuisine)==2:
                price=cuisine[-1]
                cuisine=cuisine[0]
            else:
                price=None
                cuisine=cuisine[0]
            
            # Get next button for pictures
            button=container[i].find_element("xpath",".//button[@class='BrOJk u j z _F wSSLS tIqAi IyzRb' and @aria-label='Next Photo']")
            
            # Extract images
            srcs=[]
            for j in range(images_per_resto):
                try:
                    if j>0:
                        webdriver.ActionChains(driver).move_to_element(button).click(button).perform()
                    xsrcs=container[i].find_elements('xpath',".//div[@class='rtRyy _R w _Z GA']")
                    src=[j.get_attribute('style').split('url("')[-1].split('"')[0] for j in xsrcs]
                    srcs.append(src)
                except:
                    pass
                
            srcs=list(set(sum(srcs, [])))
    
            # save image
            for y in range(len(srcs)):
                while True:
                    try:
                        print('Save image:',y)
                        resp = urlopen(srcs[y], timeout=10)
                        respHtml = resp.read()
                        binfile = open(os.path.join(savedir_img, f"{x}_{i}_{name.replace(' ','_')}_{y}.jpg"), "wb")
                        binfile.write(respHtml)
                        binfile.close()
                        break
                    except:
                        print('timeout, retry in 2 seconds.')
                        time.sleep(2)
            
            review_preview=[j.text.replace("“",'').replace("”",'') for j in container[i].find_elements("xpath", ".//*[contains(@class,'fnrKq')]//span")]
            temp.append([number, name, url, rating, reviewer, cuisine, price, review_preview, src])
            
            print('------------------------------',f'Restaurant {i+1} done','------------------------------', sep='\n')
        
        # Consolidate
        temp=pd.DataFrame(temp, columns=['rank','name','url','rating','n_reviewers','cuisine','price','review_preview', 'image'])
        restos.append(temp)
        
        # Click the "Next" button.
        result=None
        tries=1
        while result==None or tries>next_max_tries:
            try:
                driver.find_element("xpath","//a[contains(text(), 'Next') and @class='nav next rndBtn ui_button primary taLnk']").click()
                result=1
                tries+=1
            except:
                pass
        print('===================================',f"Page {x+1} done",'===================================', sep='\n')
    
    # Concatenate and save all restaurant data scraped    
    restos=pd.concat(restos, ignore_index=True)
    
    # Save file
    restos.to_csv(savepath, index=False)
    
    # When all pages have been processed, quit the driver
    driver.quit()

scrape_tripadvisor()