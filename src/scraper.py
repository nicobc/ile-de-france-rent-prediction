import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from tqdm import tqdm

def scrap_laforet(data_folder='data', replace_strategy='abort'):
    """
    Web scrapping function for www.laforet.com meant to retrieve relevant info from property ads
    in Ile-de-France.

    Parameters
    ----------
    data_folder: str, defaut 'data'
        path of the folder where the data will be written, created when needed
    replace_strategy: str, any from ['abort', 'replace'], default 'abort'
        strategy to follow if a file with the same name as the data file already exists

    Returns
    -------
    None

    """
    def get_soup(URL):
        return BeautifulSoup(requests.get(URL).content)

    BASE_URL = 'https://www.laforet.com/louer/rechercher?'
    depts = [75, 77, 78, 91, 92, 93, 94, 95]

    # Create data folder if it doesn't exist
    if not os.path.isdir(data_folder):
        os.mkdir(data_folder)

    # Instanciate data container
    data = []

    for dept in depts:
        for filter_ in ('is_furnished', 'is_not_furnished'):
            print(f'Scraping {"furnished" if filter_ == "is_furnished" else "unfurnished"} properties in dept. {dept} ...')

            url = f'{BASE_URL}filter[cities]={dept}&filter[types]=house%2Capartment&filter[{filter_}]=true&next=5'

            # Get search soup
            soup = get_soup(url)

            # Find all property ad links
            elements = soup.select('a.property-card__link')
            links = set([el.attrs['href'] for el in elements])

            for link in tqdm(links):

                # Get ad soup
                temp_soup = get_soup('https://www.laforet.com' + link)

                # Scrap
                try:
                    ref = temp_soup.select_one('div.property__title span').text.strip()
                except AttributeError:
                    continue # in case a link is dead
                title = temp_soup.select_one('div.property__title h1').text
                price = (
                    temp_soup.select_one('div.property__price').text
                    .replace('\u202f', '').replace('\xa0', '').replace('\n', '').strip()
                )
                descr = ' '.join([text.strip()
                                  for text in temp_soup.select_one('div.property-content__description.mb-4').text.split('\n')
                                  if text.strip() != ''])
                try:
                    conso = temp_soup.select('div.mb-4.col')[0].select_one('span.indicator__value').text.replace('\n', '').strip()
                except AttributeError:
                    conso = ''
                try:
                    emiss = temp_soup.select('div.mb-4.col')[1].select_one('span.indicator__value').text.replace('\n', '').strip()
                except AttributeError:
                    emiss = ''
                feats = [feat.text.replace('\n', '').strip()
                         for feat in temp_soup.select('div.property-features__content span.property-feature')]
                feats = '#'.join(feats)

                # Append data
                data.append([ref, title, price, descr, conso, emiss, feats, dept, filter_])

            print('\n')

    # Store data in a DataFrame and write it on disk
    df = pd.DataFrame(data, columns=['ref', 'title', 'price', 'descr', 'conso', 'emiss', 'feats', 'dept', 'furnitures'])

    # Check if data file name already exists : if so follow replace_strategy, if not then create it
    filename = f'laforet_{dt.now().year}_{dt.now().month}_{dt.now().day}.csv'
    if not os.path.isfile(os.path.join(data_folder, filename)):
        df.to_csv(os.path.join(data_folder, filename), sep='|', index=False)
    else:
        if replace_strategy == 'abort':
            raise FileExistsError(f"File {os.path.join(data_folder, file_name)} already exists. Scraping aborted. To replace the existing file, change replace_strategy to 'replace'.")
        elif replace_strategy == 'replace':
            df.to_csv(os.path.join(data_folder, filename), sep='|', index=False)

def scrap_orpi(data_folder='data', replace_strategy='abort'):
    """
    Web scrapping function for www.orpi.com meant to retrieve relevant info from property ads
    in Ile-de-France.

    Parameters
    ----------
    data_folder: str, defaut 'data'
        path of the folder where the data will be written, created when needed
    replace_strategy: str, any from ['abort', 'replace'], default 'abort'
        strategy to follow if a file with the same name as the data file already exists

    Returns
    -------
    None

    """

    BASE_URL = 'https://www.orpi.com/recherche/rent?'
    depts = ['paris', 'seine-et-marne', 'yvelines', 'essonne', 'hauts-de-seine',
             'seine-saint-denis', 'val-de-marne', 'val-d-oise']

    links = {
        dept: []
        for dept in depts
    }

    print('Getting links to property ads for each département ...')

    for dept in tqdm(depts):
        url = f'{BASE_URL}transaction=rent&resultUrl=&realEstateTypes[0]=maison&realEstateTypes[1]=appartement&locations[0][value]={dept}&agency=&minSurface=&maxSurface=&newBuild=&oldBuild=&minPrice=&maxPrice=&sort=date-down&layoutType=mixte&nbBedrooms=&page=&minLotSurface=&maxLotSurface=&minStoryLocation=&maxStoryLocation='

        driver = webdriver.Firefox()

        driver.get(url)

        # accept cookies
        driver.find_element_by_css_selector('button.c-btn.c-btn--lg').click()

        # append property ads links
        soup = BeautifulSoup(driver.page_source)
        links[dept].extend([a.get('href') for a in soup.select('a.u-link-unstyled.c-overlay__link')])

        # repeat for every page
        next_page = driver.find_elements_by_css_selector('a.c-pagination__link')[-1] \
                          .find_element_by_css_selector('span') \
                          .text == 'Suivant'
        while next_page:
            driver.find_elements_by_css_selector('a.c-pagination__link')[-1].click()
            next_page = driver.find_elements_by_css_selector('a.c-pagination__link')[-1] \
                              .find_element_by_css_selector('span') \
                              .text == 'Suivant'
            soup = BeautifulSoup(driver.page_source)
            links[dept].extend([a.get('href') for a in soup.select('a.u-link-unstyled.c-overlay__link')])

        driver.close()

    print('\n')

    word2num = {
        'paris': 75,
        'seine-et-marne': 77,
        'yvelines': 78,
        'essonne': 91,
        'hauts-de-seine': 92,
        'seine-saint-denis': 93,
        'val-de-marne': 94,
        'val-d-oise': 95
    }

    data = []

    for dept in links.keys():
        print(f'Scraping {dept} ...')

        for link in tqdm(links[dept]):

            url = 'https://www.orpi.com' + link
            soup = BeautifulSoup(requests.get(url).content)

            try:
                ref = soup.select_one('span.u-text-xs').text
            except AttributeError:
                continue
            prop_type = soup.select_one('span.u-text-xl').text.replace('\n', '').strip()
            rooms, surface = soup.select_one('span.u-h3.u-color-primary').text.split(' • ')
            city = soup.select_one('span.u-text-lg').text
            price = soup.select_one('span.u-h1').text.replace('\xa0', '')
            descr = soup.select_one('div.c-section__inner div.o-container p').text.replace('\n', '').strip()
            feats = [span.text for span in soup.select('span.c-badge__text')]
            try:
                conso = soup.select_one('abbr.c-dpe__index.c-dpe__index--5').text
            except AttributeError:
                conso = ''

            try:
                emiss = soup.select_one('abbr.c-dpe__index.c-dpe__index--3').text
            except AttributeError:
                emiss = ''

            data.append([ref, prop_type, city, word2num[dept], rooms, surface, price, descr, conso, emiss, feats])

        print('\n')

    df = pd.DataFrame(data,
                      columns=['ref', 'prop_type', 'city', 'dept', 'rooms',
                               'surface', 'price', 'descr', 'conso', 'emiss', 'feats'])

    # Check if data file name already exists : if so follow replace_strategy, if not then create it
    filename = f'orpi_{dt.now().year}_{dt.now().month}_{dt.now().day}.csv'
    if not os.path.isfile(os.path.join(data_folder, filename)):
        df.to_csv(os.path.join(data_folder, filename), sep='|', index=False)
    else:
        if replace_strategy == 'abort':
            raise FileExistsError(f"File {os.path.join(data_folder, file_name)} already exists. Scraping aborted. To replace the existing file, change replace_strategy to 'replace'.")
        elif replace_strategy == 'replace':
            df.to_csv(os.path.join(data_folder, filename), sep='|', index=False)

def scrap_guy_hoquet(data_folder='data', replace_strategy='abort'):
    """
    Web scrapping function for www.guy-hoquet.com meant to retrieve relevant info from property ads
    in Ile-de-France.

    Parameters
    ----------
    data_folder: str, defaut 'data'
        path of the folder where the data will be written, created when needed
    replace_strategy: str, any from ['abort', 'replace'], default 'abort'
        strategy to follow if a file with the same name as the data file already exists

    Returns
    -------
    None

    """

    url = 'https://www.guy-hoquet.com/biens/result#1&p=1&f10=2&f20=75_c2,77_c2,78_c2,91_c2,92_c2,93_c2,94_c2,95_c2&f30=appartement,maison'

    links = []

    driver = webdriver.Firefox()
    driver.implicitly_wait(5) # seconds
    driver.get(url)

    driver.find_element_by_css_selector('div#accept-all-cookies').click()
    links.extend([a.get_attribute('href') for a in driver.find_elements_by_css_selector('a.property_link_block')])

    while True:
        try:
            driver.find_element_by_css_selector('li.page-item.next a').click()
        except NoSuchElementException:
            break
        links.extend([a.get_attribute('href') for a in driver.find_elements_by_css_selector('a.property_link_block')])

    driver.close()

    data = []

    for link in tqdm(links):

        soup = BeautifulSoup(requests.get(link).content)

        try:
            prop_type = soup.select_one('h1.name.property-name').text
            city = soup.select_one('div.add').text
            price = soup.select_one('div.price').text.replace('\n', '').strip()
            descr = soup.select_one('span.description-more').text.replace('\n', '').replace('Voir moins', '').strip()
            feats = [tag.text for tag in soup.select('div.ttl')]
            feats2 = [re.sub(r'\s+', ' ',re.sub(r'\n+', '', tag.text)).strip() for tag in soup.select('div.horaires-item')]
            neighborhood = re.sub(r'\s+', ' ',re.sub(r'\n+', '', soup.select_one('div.quartier-info.mt-4').text)).strip()
        except AttributeError:
            continue

        data.append([prop_type, city, price, descr, feats, feats2, neighborhood])

    df = pd.DataFrame(data, columns=['prop_type', 'city', 'price', 'descr', 'feats', 'feats2', 'neighborhood'])

    # Check if data file name already exists : if so follow replace_strategy, if not then create it
    filename = f'guy_hoquet_{dt.now().year}_{dt.now().month}_{dt.now().day}.csv'
    if not os.path.isfile(os.path.join(data_folder, filename)):
        df.to_csv(os.path.join(data_folder, filename), sep='|', index=False)
    else:
        if replace_strategy == 'abort':
            raise FileExistsError(f"File {os.path.join(data_folder, file_name)} already exists. Scraping aborted. To replace the existing file, change replace_strategy to 'replace'.")
        elif replace_strategy == 'replace':
            df.to_csv(os.path.join(data_folder, filename), sep='|', index=False)
