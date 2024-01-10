import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

# set up a controllable Firefox instance
# in headless mode
service = Service()
options = webdriver.FirefoxOptions()
options.add_argument("--headless=new")
driver = webdriver.Firefox(
    service=service,
    options=options
)

# define query, location and number of pages to search for
query = '("data scientist" OR "data engineer" OR "data analyst")'
location = ''
pages = 1

# dataframe in which the data will be stored
cols = ['role', 'company_name', 'company_location', 'company_rating', 'salary', 'job_type', 'description']
df = pd.DataFrame(columns=cols)

for page in range(0, pages):
    url = f'https://www.indeed.com/jobs?q={query}&l={location}&start={page*10}'
    driver.get(url)

    jobs = driver.find_elements(By.CSS_SELECTOR, ".cardOutline")

    for job in jobs:

        # click on job and wait until it loads its information on the right side panel
        job.click()
        try:
            title_element = WebDriverWait(driver, 5) \
                .until(EC.presence_of_element_located((By.CSS_SELECTOR, ".jobsearch-HeaderContainer")))
        except NoSuchElementException:
            continue
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        role = soup.find('h2', {'class': 'jobsearch-JobInfoHeader-title'}).find('span').text
        description = soup.find('div', {'id': 'jobDescriptionText'}).text
        company_info = soup.find('div', {'data-testid': 'jobsearch-CompanyInfoContainer'})
        company_name = company_info.find('div', {'data-testid': 'inlineHeader-companyName'}).find('a').text
        company_location = company_info.find('div', {'data-testid': 'inlineHeader-companyLocation'}).find('div').text
        try:
            company_rating = company_info.find('div', {'id': 'companyRatings'})['aria-label']
        except:
            company_rating = ''

        salary_and_type = soup.find('div', {'id': 'salaryInfoAndJobType'})
        if salary_and_type is not None:
            salary_and_type = salary_and_type.find_all('span')
            if len(salary_and_type) == 2:
                salary = salary_and_type[0].text
                job_type = salary_and_type[1].text
            else:
                if '$' in salary_and_type[0].text:
                    salary = salary_and_type[0].text
                    job_type = ''
                else:
                    salary = ''
                    job_type = salary_and_type[0].text      
        else:
            salary = ''
            job_type = ''
        
        df.loc[len(df)] = (role, company_name, company_location, company_rating, salary, job_type, description)

driver.quit()
df.to_csv('jobs.csv', index=False)
